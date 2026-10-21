import { request as httpRequest } from 'node:http';
import { request as httpsRequest } from 'node:https';
import type { IPoint, IQueryOptions } from 'influx';
import { escapeInfluxQLIdentifier, type ValuesForInflux } from './Database';
import DatabaseInfluxDB1x from './DatabaseInfluxDB1x';
import type { MeasurementStatistics } from './statistics';

/**
 * A table deleted in InfluxDB 3 keeps being listed by `SHOW MEASUREMENTS`, renamed to
 * `<name>-<deletion time>`, until the server removed its files
 */
const DELETED_TABLE = /-\d{8}T\d{6}$/;

/** Number of measurements whose statistics are read with one request (4 statements each) */
const STATISTICS_CHUNK = 50;

/**
 * InfluxDB 3 (Core / Enterprise).
 *
 * InfluxDB 3 still speaks the 1.x API (`/write` and `/query` with InfluxQL), so writing and reading is done by
 * the 1.x client. The token is the password, the username is ignored. Only what 1.x does with InfluxQL
 * statements InfluxDB 3 does not know any more - create and drop databases, change the retention, drop
 * measurements - uses the v3 HTTP API. InfluxDB 3 can not delete single values or time ranges.
 */
export default class DatabaseInfluxDB3x extends DatabaseInfluxDB1x {
    private readonly token: string;

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
            token: string;
            validateSSL?: boolean;
        },
    ) {
        // the username is ignored, but the driver only authenticates when one is set
        super(options, { username: 'iobroker', password: db3xOptions.token, validateSSL: db3xOptions.validateSSL });
        this.token = db3xOptions.token;
    }

    /** InfluxDB 3 rejects the empty `epoch` parameter the driver sends by default */
    protected queryOptions(): IQueryOptions {
        return { precision: 'ms' };
    }

    /**
     * Send a request to the v3 API
     *
     * @param method HTTP method
     * @param path path with query string, e.g. `/api/v3/configure/database`
     * @param body JSON body
     * @returns status code and body of the answer
     */
    private api(
        method: string,
        path: string,
        body?: Record<string, unknown>,
    ): Promise<{ status: number; text: string }> {
        const data = body ? JSON.stringify(body) : undefined;
        return this.trackConnection(
            () =>
                new Promise((resolve, reject) => {
                    const req = (this.protocol === 'https' ? httpsRequest : httpRequest)(
                        {
                            method,
                            host: this.host,
                            port: parseInt(this.port as string, 10) || 8181,
                            path,
                            headers: {
                                Authorization: `Bearer ${this.token}`,
                                ...(data
                                    ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) }
                                    : {}),
                            },
                            timeout: this.requestTimeout || undefined,
                            rejectUnauthorized: this.validateSSL,
                        },
                        res => {
                            let text = '';
                            res.setEncoding('utf8');
                            res.on('data', chunk => (text += chunk));
                            res.on('end', () => resolve({ status: res.statusCode || 0, text }));
                        },
                    );
                    req.on('timeout', () =>
                        req.destroy(Object.assign(new Error('Request timeout'), { code: 'ETIMEDOUT' })),
                    );
                    req.on('error', reject);
                    req.end(data);
                }),
        );
    }

    /**
     * Send a request to the v3 API and throw on an error answer
     *
     * @param method HTTP method
     * @param path path with query string
     * @param body JSON body
     * @param ignore status codes that are no error for the caller
     */
    private async apiOk(
        method: string,
        path: string,
        body?: Record<string, unknown>,
        ignore: number[] = [],
    ): Promise<void> {
        const { status, text } = await this.api(method, path, body);
        if ((status < 200 || status >= 300) && !ignore.includes(status)) {
            throw Object.assign(new Error(`InfluxDB 3: ${status} ${text}`.trim()), { statusCode: status });
        }
    }

    async createDatabase(dbname: string): Promise<void> {
        // 409: exists already
        await this.apiOk('POST', '/api/v3/configure/database', { db: dbname }, [409]);
    }

    async dropDatabase(dbname: string): Promise<void> {
        // without hard_delete_at the database is only renamed and its name can not be used again at once
        await this.apiOk(
            'DELETE',
            `/api/v3/configure/database?db=${encodeURIComponent(dbname)}&hard_delete_at=now`,
            undefined,
            [404],
        );
    }

    async applyRetentionPolicyToDB(dbname: string, retention: string | number): Promise<void> {
        const retentionSeconds = parseInt(retention as string, 10) || 0;
        // SHOW RETENTION POLICIES still works and reports the retention of the database like 1.x
        const oldRetention = await this.getRetentionPolicyForDB(dbname);
        if ((oldRetention?.time || 0) === retentionSeconds) {
            this.log.debug(`Retention policy for ${dbname} remains unchanged.`);
            return;
        }
        this.log.info(
            `Applying retention policy for ${dbname} to ${retentionSeconds ? `${retentionSeconds} seconds` : 'infinity'}`,
        );
        // without retention_period the database keeps its data forever
        await this.apiOk(
            'PUT',
            '/api/v3/configure/database',
            retentionSeconds ? { db: dbname, retention_period: `${retentionSeconds}s` } : { db: dbname },
        );
    }

    deleteData(): Promise<void> {
        return Promise.reject(new Error('InfluxDB 3 can not delete single values or time ranges'));
    }

    async dropMeasurement(measurement: string): Promise<void> {
        await this.apiOk(
            'DELETE',
            `/api/v3/configure/table?db=${encodeURIComponent(this.database)}&table=${encodeURIComponent(measurement)}&hard_delete_at=now`,
        );
    }

    /**
     * Is this measurement a deleted table that InfluxDB 3 still lists?
     *
     * @param measurement name from `SHOW MEASUREMENTS`
     */
    static isDeletedTable(measurement: string): boolean {
        return DELETED_TABLE.test(measurement);
    }

    /**
     * Write `from` also when it is empty.
     *
     * InfluxDB 3 applies LIMIT and OFFSET to every field on its own: if a field is missing in some points,
     * `SELECT * ... ORDER BY time DESC LIMIT 1 OFFSET 1` answers with values of different points in one page.
     *
     * @param seriesId the measurement
     * @param pointToSend the value
     */
    protected toPoint(seriesId: string, pointToSend: ValuesForInflux): IPoint {
        const point = super.toPoint(seriesId, pointToSend);
        point.fields!.from = pointToSend.from || '';
        return point;
    }

    /**
     * Statistics measurement by measurement.
     *
     * Unlike 1.x, `first()` and `last()` over `FROM /.*\/` fail as soon as the measurements hold different
     * types ("expected 1 selector expression, got 0"), and `count()` answers null for the non-numeric ones.
     * The statements of many measurements are sent in one request instead.
     *
     * @param start beginning of the examined range (ms)
     * @param stop end of the examined range (ms)
     */
    async getStatistics(start: number, stop: number): Promise<MeasurementStatistics> {
        const where = ` WHERE time >= '${new Date(start).toISOString()}' AND time <= '${new Date(stop).toISOString()}'`;
        const statistics: MeasurementStatistics = {};

        const names = (await this.query<{ name: string }>('SHOW MEASUREMENTS'))
            .map(row => row.name)
            .filter(name => !!name && !DatabaseInfluxDB3x.isDeletedTable(name));

        for (let i = 0; i < names.length; i += STATISTICS_CHUNK) {
            const chunk = names.slice(i, i + STATISTICS_CHUNK);
            const statements = chunk.flatMap(name => {
                const from = `FROM "${escapeInfluxQLIdentifier(name)}"${where}`;
                // grouped by all tags, every row of the answer is one series
                return [
                    `SELECT count("value") ${from}`,
                    `SELECT first("value") ${from}`,
                    `SELECT last("value") ${from}`,
                    `SELECT count("value") ${from} GROUP BY *`,
                ];
            });
            // with more than one statement, the driver answers with one array of rows per statement
            const results = (await this.query(statements.join(';'))) as unknown as Array<
                Array<{ time: Date; count?: number }>
            >;
            chunk.forEach((name, n) => {
                const [counted, first, last, series] = results.slice(n * 4, n * 4 + 4);
                const count = Number(counted?.[0]?.count) || 0;
                if (!count) {
                    // no value in the range: the caller reports the measurement as empty
                    return;
                }
                statistics[name] = {
                    count,
                    firstTs: first?.[0]?.time ? new Date(first[0].time).getTime() : null,
                    lastTs: last?.[0]?.time ? new Date(last[0].time).getTime() : null,
                    // unlike 1.x only the series with values in the examined range are counted
                    cardinality: series?.length || null,
                };
            });
        }

        return statistics;
    }
}
