import * as http from 'node:http';
import * as https from 'node:https';
import { Database, type ValuesForInflux } from './Database';
import {
    isDeletedTable,
    parseInfluxTime,
    splitStatements,
    toLineProtocol,
    translateWriteError,
} from './influx3Helpers';
import type { MeasurementStatistics } from './statistics';

/** A row of an InfluxQL answer. `iox::measurement` names the measurement the row belongs to */
type RawRow = { 'iox::measurement'?: string; [column: string]: any };

/** How many measurements one statistics statement covers */
const STATISTICS_BATCH_SIZE = 100;

/** How many rejected lines of a partial write are named in the error. The rest only adds log noise */
const MAX_REPORTED_LINES = 3;

/**
 * Talks to InfluxDB 3.x (Core and Enterprise) over plain HTTP: line protocol to write, InfluxQL to read.
 *
 * InfluxDB 3 has no organizations and buckets. A "database" is created by the first write, access is
 * controlled by a token only, and the InfluxQL endpoint takes exactly one statement per request.
 * Reading with InfluxQL (not SQL) lets `getHistory` and the other read paths of 1.x be reused as they are.
 */
export default class DatabaseInfluxDB3x extends Database {
    /** Base path of a reverse proxy: empty, or starting with a slash and without a trailing one */
    private readonly basePath: string;
    private readonly token: string;
    private readonly useTags: boolean;
    private readonly validateSSL: boolean;
    private readonly syncWrites: boolean;
    private agent!: http.Agent;

    constructor(
        options: {
            log: ioBroker.Logger;
            host: string;
            port: number | string;
            protocol: 'http' | 'https';
            database: string;
            requestTimeout: number;
        },
        db3xOptions: {
            path: string;
            token: string;
            validateSSL: boolean;
            useTags?: boolean;
            syncWrites?: boolean;
        },
    ) {
        super(options);
        // trimmed without a regex: `/^\/+|\/+$/` backtracks polynomially on many slashes (CodeQL js/polynomial-redos)
        const rawPath = db3xOptions.path || '';
        let start = 0;
        let end = rawPath.length;
        while (start < end && rawPath[start] === '/') {
            start++;
        }
        while (end > start && rawPath[end - 1] === '/') {
            end--;
        }
        const path = rawPath.slice(start, end);
        this.basePath = path ? `/${path}` : '';
        this.token = db3xOptions.token;
        this.useTags = db3xOptions.useTags || false;
        this.validateSSL = db3xOptions.validateSSL;
        this.syncWrites = db3xOptions.syncWrites || false;

        this.connect();
    }

    connect(): void {
        this.log.debug(
            `Connect InfluxDB3: ${this.protocol}://${this.host}:${this.port}${this.basePath} [${this.database}]`,
        );
        this.agent =
            this.protocol === 'https'
                ? new https.Agent({ keepAlive: true, rejectUnauthorized: this.validateSSL })
                : new http.Agent({ keepAlive: true });
    }

    /**
     * Send one request and resolve with the body of the answer.
     *
     * Every failure is an `Error` that `isConnectionError()` can classify: socket errors keep their
     * `code`, a timeout gets `ETIMEDOUT`, and a rejected request carries its `statusCode`.
     *
     * @param method HTTP method
     * @param route path below the configured base path, including the query string
     * @param body request body
     * @param contentType content type of the body
     */
    private request(method: string, route: string, body?: string, contentType?: string): Promise<string> {
        return this.trackConnection(
            () =>
                new Promise<string>((resolve, reject) => {
                    const headers: http.OutgoingHttpHeaders = {};
                    if (this.token) {
                        headers.Authorization = `Bearer ${this.token}`;
                    }
                    if (body !== undefined) {
                        headers['Content-Type'] = contentType || 'text/plain; charset=utf-8';
                        headers['Content-Length'] = Buffer.byteLength(body);
                    }

                    const url = new URL(`${this.protocol}://${this.host}:${this.port}${this.basePath}${route}`);
                    const transport = this.protocol === 'https' ? https : http;
                    const req = transport.request(url, { method, headers, agent: this.agent }, res => {
                        const chunks: Buffer[] = [];
                        res.on('data', (chunk: Buffer) => chunks.push(chunk));
                        res.on('error', reject);
                        res.on('end', () => {
                            const text = Buffer.concat(chunks).toString('utf8');
                            if (res.statusCode && res.statusCode >= 400) {
                                reject(DatabaseInfluxDB3x.toHttpError(res.statusCode, text));
                            } else {
                                resolve(text);
                            }
                        });
                    });
                    if (this.requestTimeout) {
                        req.setTimeout(this.requestTimeout, () =>
                            req.destroy(
                                Object.assign(new Error(`Request timed out after ${this.requestTimeout} ms`), {
                                    code: 'ETIMEDOUT',
                                }),
                            ),
                        );
                    }
                    req.on('error', reject);
                    req.end(body);
                }),
        );
    }

    /**
     * Build the error for a rejected request from the body InfluxDB 3 sent.
     *
     * A partial write lists the rejected lines. The valid ones were stored anyway, so the message says
     * which lines failed - and a type conflict is worded the way the write path expects it.
     *
     * @param statusCode HTTP status of the answer
     * @param text body of the answer
     */
    private static toHttpError(statusCode: number, text: string): Error {
        let message = text.trim();
        try {
            const parsed = JSON.parse(text);
            if (Array.isArray(parsed?.data) && parsed.data.length) {
                const lines = (parsed.data as { error_message?: string; original_line?: string }[])
                    .slice(0, MAX_REPORTED_LINES)
                    .map(rejected => translateWriteError(rejected.error_message || '', rejected.original_line));
                const more = parsed.data.length - lines.length;
                message = `${parsed.error || 'write failed'}: ${lines.join('; ')}${more > 0 ? ` and ${more} more` : ''}`;
            } else if (typeof parsed?.error === 'string') {
                message = parsed.error;
            }
        } catch {
            // not JSON - the plain text is the best description there is
        }
        const error = new Error(message || `HTTP ${statusCode}`);
        error.name = 'HttpError';
        return Object.assign(error, { statusCode });
    }

    private db(): string {
        return encodeURIComponent(this.database);
    }

    async getDatabaseNames(): Promise<string[]> {
        try {
            const rows: { 'iox::database'?: string }[] = JSON.parse(
                (await this.request('GET', '/api/v3/configure/database?format=json')) || '[]',
            );
            return rows.map(row => row['iox::database']).filter((name): name is string => !!name);
        } catch (error) {
            // A token that may only write cannot list databases, but it can still write into its own.
            // 404/405 are not a permission problem but a wrong path or a server that is not InfluxDB 3
            if ((error as { statusCode?: number }).statusCode === 403) {
                this.log.debug(`Cannot list the databases (${error}), assuming "${this.database}" exists`);
                return [this.database];
            }
            throw error;
        }
    }

    async createDatabase(dbname: string): Promise<void> {
        this.log.info(`Creating database ${dbname}`);
        try {
            await this.request(
                'POST',
                '/api/v3/configure/database',
                JSON.stringify({ db: dbname }),
                'application/json',
            );
        } catch (error) {
            // 409: it exists already. 403: the first write creates it anyway
            if (![403, 409].includes((error as { statusCode?: number }).statusCode || 0)) {
                throw error;
            }
            this.log.debug(`Database ${dbname} not created explicitly: ${error}`);
        }
    }

    async dropDatabase(dbname: string): Promise<void> {
        this.log.info(`Dropping database ${dbname}`);
        await this.request('DELETE', `/api/v3/configure/database?db=${encodeURIComponent(dbname)}`);
    }

    async ping(): Promise<{ online: boolean }[]> {
        try {
            await this.request('GET', '/health');
        } catch (error) {
            // the server answered, it just wants a better token - that is not a dead host
            if ([401, 403].includes((error as { statusCode?: number }).statusCode || 0)) {
                return [{ online: true }];
            }
            throw error;
        }
        return [{ online: true }];
    }

    async getRetentionPolicyForDB(dbName: string): Promise<{ name: string | null; time: number | undefined } | null> {
        let rows: RawRow[];
        try {
            rows = await this.rawQuery('SHOW RETENTION POLICIES', dbName);
        } catch (error) {
            // a database that was not created yet has no retention
            if ((error as { statusCode?: number }).statusCode === 404) {
                return null;
            }
            throw error;
        }
        const row = rows[0] as { name?: string; duration?: string } | undefined;
        if (!row) {
            return null;
        }
        // looks like "720h0m0s", "0s" for an infinite retention
        const match = /^(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$/.exec(row.duration || '');
        const time = match
            ? (parseInt(match[1], 10) || 0) * 3600 + (parseInt(match[2], 10) || 0) * 60 + (parseInt(match[3], 10) || 0)
            : undefined;
        return { name: row.name || null, time };
    }

    async applyRetentionPolicyToDB(dbName: string, retention: string | number): Promise<void> {
        const seconds = parseInt(retention as string, 10) || 0;
        const old = await this.getRetentionPolicyForDB(dbName);
        this.log.debug(`old retention: ${JSON.stringify(old)} new retention: ${seconds}`);
        if (old && old.time === seconds) {
            this.log.debug(`Retention policy for ${dbName} remains unchanged.`);
            return;
        }
        this.log.info(
            `Applying retention policy for ${dbName} to ${seconds === 0 ? 'infinity' : `${seconds} seconds`}`,
        );
        // no retention_period (null) means infinite
        await this.request(
            'PUT',
            '/api/v3/configure/database',
            JSON.stringify({ db: dbName, retention_period: seconds === 0 ? null : `${seconds}s` }),
            'application/json',
        );
    }

    async getMetaDataStorageType(): Promise<'tags' | 'fields' | 'none'> {
        const isMetaData = (key: unknown): boolean => key === 'q' || key === 'ack' || key === 'from';
        try {
            const tagKeys = await this.rawQuery('SHOW TAG KEYS');
            if (tagKeys.some(row => isMetaData(row.tagKey))) {
                return 'tags';
            }
            const fieldKeys = await this.rawQuery('SHOW FIELD KEYS');
            return fieldKeys.some(row => isMetaData(row.fieldKey)) ? 'fields' : 'none';
        } catch (error) {
            const statusCode = (error as { statusCode?: number }).statusCode;
            // a database that was not written to yet does not exist
            if (statusCode === 404) {
                return 'none';
            }
            // a token that may only write cannot look at the stored keys - connecting must still succeed
            if (statusCode === 403) {
                this.log.warn(
                    `The token may not read from ${this.database}, so the storage of q, ack and from cannot be checked: ${error}`,
                );
                return 'none';
            }
            throw error;
        }
    }

    /**
     * Write lines of line protocol.
     *
     * The server stores every valid line of a batch and rejects the rest ("partial write"). The adapter
     * relies on that to find the offending point.
     *
     * @param lines lines of line protocol
     * @param noSync answer before the write reached the write-ahead log. By default the server answers after
     *               the next WAL flush, which takes about a second - far too long for the direct write of
     *               a single value, which the adapter awaits before it handles the next change of the
     *               same datapoint. Rejected lines are reported either way; the price is that the value
     *               becomes readable (and durable) up to a second later.
     */
    private async writeLines(lines: string[], noSync = false): Promise<void> {
        if (!lines.length) {
            return;
        }
        await this.request(
            'POST',
            `/api/v3/write_lp?db=${this.db()}&precision=millisecond&accept_partial=true${noSync ? '&no_sync=true' : ''}`,
            lines.join('\n'),
        );
        this.log.debug(`Points written to ${this.database}`);
    }

    async writeSeries(series: { [id: string]: ValuesForInflux[] }): Promise<void> {
        const lines: string[] = [];
        for (const [seriesId, values] of Object.entries(series)) {
            for (const value of values) {
                lines.push(toLineProtocol(seriesId, value, this.useTags));
            }
        }
        await this.writeLines(lines);
    }

    async writePoints(seriesId: string, pointsToSend: ValuesForInflux[]): Promise<void> {
        await this.writeLines(pointsToSend.map(value => toLineProtocol(seriesId, value, this.useTags)));
    }

    async writePoint(seriesId: string, value: ValuesForInflux): Promise<void> {
        await this.writeLines([toLineProtocol(seriesId, value, this.useTags)], !this.syncWrites);
    }

    deleteData(
        _start: Date | number,
        _stop: Date | number,
        _org: string,
        _dbName: string,
        _query: string,
    ): Promise<void> {
        return Promise.reject(
            new Error('InfluxDB 3.x cannot delete single values or a time range, only a whole datapoint'),
        );
    }

    async dropMeasurement(measurement: string): Promise<void> {
        try {
            await this.request(
                'DELETE',
                // `hard_delete_at=now`: without it the data is kept for a grace period and the table is only hidden
                `/api/v3/configure/table?db=${this.db()}&table=${encodeURIComponent(measurement)}&hard_delete_at=now`,
            );
        } catch (error) {
            // nothing was ever written for it - which is what the caller wants to reach
            if ((error as { statusCode?: number }).statusCode !== 404) {
                throw error;
            }
        }
    }

    /**
     * Run ONE InfluxQL statement and return the rows as the server sent them
     *
     * @param statement the statement
     */
    private async rawQuery(statement: string, database = this.database): Promise<RawRow[]> {
        this.log.debug(`Query to execute: ${statement}`);
        const text = await this.request(
            'POST',
            '/api/v3/query_influxql',
            JSON.stringify({ db: database, q: statement, format: 'json' }),
            'application/json',
        );
        return text.trim() ? JSON.parse(text) : [];
    }

    /**
     * Run InfluxQL.
     *
     * Several statements separated by `;` are run one after the other, as the server takes only one per
     * request. The answer has the shape of the `influx` package used for 1.x: the rows of a single
     * statement, or one array of rows per statement.
     *
     * @param query one or more InfluxQL statements
     */
    async query<T>(query: string): Promise<Array<T & { time: Date }>> {
        const statements = splitStatements(query);
        const results: Array<Array<T & { time: Date }>> = [];
        for (const statement of statements) {
            const rows = await this.rawQuery(statement);
            results.push(
                rows.map(row => {
                    const { 'iox::measurement': _measurement, ...columns } = row;
                    if (columns.time !== undefined && columns.time !== null) {
                        columns.time = parseInfluxTime(columns.time);
                    }
                    return columns as T & { time: Date };
                }),
            );
        }
        return (results.length === 1 ? results[0] : results) as Array<T & { time: Date }>;
    }

    /**
     * Run a SQL statement and return the rows.
     *
     * SQL is only used where InfluxQL cannot do it: `first()`/`last()` over several measurements fail on the
     * server as soon as a boolean one is among them, and `count()` reports nothing for a boolean measurement.
     *
     * @param sql the statement
     */
    private async sqlQuery(sql: string): Promise<RawRow[]> {
        this.log.debug(`SQL to execute: ${sql}`);
        const text = await this.request(
            'POST',
            '/api/v3/query_sql',
            JSON.stringify({ db: this.database, q: sql, format: 'json' }),
            'application/json',
        );
        return text.trim() ? JSON.parse(text) : [];
    }

    /**
     * Count the points of a measurement.
     *
     * InfluxQL `count(value)` reports nothing for a boolean measurement, SQL does not have that problem.
     *
     * @param measurement the measurement
     * @param start first millisecond to count, no lower limit if omitted
     * @param end last millisecond to count, no upper limit if omitted
     */
    async countRows(measurement: string, start?: number, end?: number): Promise<number> {
        const conditions: string[] = [];
        if (start !== undefined) {
            conditions.push(`time >= '${new Date(start).toISOString()}'`);
        }
        if (end !== undefined) {
            conditions.push(`time <= '${new Date(end).toISOString()}'`);
        }
        const where = conditions.length ? ` WHERE ${conditions.join(' AND ')}` : '';
        try {
            const rows = await this.sqlQuery(`SELECT count(*) AS c FROM "${measurement.replace(/"/g, '""')}"${where}`);
            return Number(rows[0]?.c) || 0;
        } catch (error) {
            // nothing was ever written for it
            if ((error as { statusCode?: number }).statusCode === 404) {
                return 0;
            }
            throw error;
        }
    }

    async listMeasurements(): Promise<string[]> {
        try {
            const tables = await this.sqlQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'iox'",
            );
            return tables.map(row => String(row.table_name)).filter(name => !!name && !isDeletedTable(name));
        } catch (error) {
            // a database nothing was written to yet does not exist
            if ((error as { statusCode?: number }).statusCode === 404) {
                return [];
            }
            throw error;
        }
    }

    async getStatistics(start: number, stop: number): Promise<MeasurementStatistics> {
        const statistics: MeasurementStatistics = {};
        const names = await this.listMeasurements();
        const quoteIdentifier = (name: string): string => `"${name.replace(/"/g, '""')}"`;
        const quoteText = (text: string): string => `'${text.replace(/'/g, "''")}'`;
        const where = `WHERE time >= ${quoteText(new Date(start).toISOString())} AND time <= ${quoteText(new Date(stop).toISOString())}`;

        // One request per batch of measurements: a request per measurement is too slow for the thousands of
        // IDs of a long-running installation, a single one would be a statement of several hundred kilobytes
        for (let i = 0; i < names.length; i += STATISTICS_BATCH_SIZE) {
            const selects = names
                .slice(i, i + STATISTICS_BATCH_SIZE)
                .map(
                    (name, index) =>
                        `SELECT ${quoteText(name)}${index ? '' : ' AS m'}, count(*)${index ? '' : ' AS c'}, ` +
                        `min(time)${index ? '' : ' AS f'}, max(time)${index ? '' : ' AS l'} ` +
                        `FROM ${quoteIdentifier(name)} ${where}`,
                );
            for (const row of await this.sqlQuery(selects.join(' UNION ALL '))) {
                statistics[String(row.m)] = {
                    count: Number(row.c) || 0,
                    firstTs: row.f ? parseInfluxTime(row.f).getTime() : null,
                    lastTs: row.l ? parseInfluxTime(row.l).getTime() : null,
                    // InfluxDB 3 has no `SHOW SERIES`, so the cardinality stays unknown
                    cardinality: null,
                };
            }
        }
        return statistics;
    }
}
