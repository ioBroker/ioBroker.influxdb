import { InfluxDB, type IPoint, type IResults, escape } from 'influx';
import { Database, type ValuesForInflux } from './Database';
import { cardinalityFromSeriesKeys, type MeasurementStatistics } from './statistics';

export default class DatabaseInfluxDB1x extends Database {
    private readonly username: string;
    private readonly password: string;
    private readonly validateSSL: boolean;
    private connection: InfluxDB | null = null;

    constructor(
        options: {
            log: ioBroker.Logger;
            host: string;
            port: number | string;
            protocol: 'http' | 'https';
            database: string;
            requestTimeout: number;
        },
        db1xOptions: {
            username: string;
            password: string;
            validateSSL?: boolean;
        },
    ) {
        super(options);
        this.username = db1xOptions.username;
        this.password = db1xOptions.password;
        this.validateSSL = db1xOptions.validateSSL !== undefined ? db1xOptions.validateSSL : true;

        this.connect();
    }

    connect(): void {
        this.log.debug(`Connect InfluxDB1: ${this.protocol}://${this.host}:${this.port} [${this.database}]`);

        this.connection = new InfluxDB({
            host: this.host,
            port: parseInt(this.port as string, 10) || undefined, // optional, default 8086
            protocol: this.protocol, // optional, default 'http'
            username: this.username,
            password: this.password,
            database: this.database,
            // Honor the configured request timeout (otherwise requests could hang indefinitely)
            pool: this.requestTimeout ? { requestTimeout: this.requestTimeout } : undefined,
            // Honor SSL validation setting for https connections
            options: this.protocol === 'https' ? { rejectUnauthorized: this.validateSSL } : undefined,
        });
    }

    deleteData(
        _start: Date | number,
        _stop: Date | number,
        _org: string,
        _dbName: string,
        _query: string,
    ): Promise<void> {
        throw new Error('Method not implemented.');
    }

    getMetaDataStorageType(): Promise<'tags' | 'fields'> {
        return Promise.resolve('fields');
    }

    async ping(): Promise<{ online: boolean }[]> {
        if (!this.connection) {
            return Promise.reject(new Error('No connection to InfluxDB'));
        }
        const hosts = await this.connection.ping(500);
        // ping() does not throw for a dead host, it reports it as offline
        if (hosts.some(host => host.online)) {
            this.markHostAvailable();
        } else {
            this.markHostUnavailable();
        }
        return hosts.map(host => ({ online: host.online }));
    }

    getDatabaseNames(): Promise<string[]> {
        if (!this.connection) {
            return Promise.reject(new Error('No connection to InfluxDB'));
        }
        return this.trackConnection(() => this.connection!.getDatabaseNames());
    }

    async getRetentionPolicyForDB(dbname: string): Promise<{ name: string | null; time: number | undefined } | null> {
        if (!this.connection) {
            return Promise.reject(new Error('No connection to InfluxDB'));
        }

        const rows = await this.connection.query<{
            name: string;
            // looks like "0h0m0s" for infinite retention, '3600s', ...
            duration: string;
            shardGroupDuration: string;
            replicaN: number;
            default: boolean;
        }>(`SHOW RETENTION POLICIES ON ${escape.quoted(dbname)}`);
        const regex = /(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?/;
        let retentionTime: number | undefined;
        let retentionName: string | null = null;
        rows.forEach(row => {
            if (row.default) {
                const regMatch = row.duration.match(regex);
                if (regMatch) {
                    const retHours = parseInt(regMatch[1]) || 0;
                    const retMinutes = parseInt(regMatch[2]) || 0;
                    const retSeconds = parseInt(regMatch[3]) || 0;
                    this.log.debug(
                        `Extracted retention time for ${dbname} - Hours: ${retHours} Minutes: ${retMinutes} Seconds: ${retSeconds}`,
                    );
                    retentionTime = retHours * 60 * 60 + retMinutes * 60 + retSeconds;
                    retentionName = row.name;
                }
            }
        });
        return { name: retentionName, time: retentionTime };
    }

    async applyRetentionPolicyToDB(dbname: string, retention: string | number): Promise<void> {
        let oldRetention = await this.getRetentionPolicyForDB(dbname);

        //Check, if it needs to be changed, otherwise skip
        this.log.debug(`old retention: ${JSON.stringify(oldRetention)} new retention: ${retention}`);

        if (oldRetention && oldRetention.time !== null && oldRetention.time === parseInt(retention as string, 10)) {
            this.log.debug(`Retention policy for ${dbname} remains unchanged.`);
            return;
        }

        const retentionSeconds = parseInt(retention as string, 10) || 0;
        const shardDuration = this.calculateShardGroupDuration(retentionSeconds);
        oldRetention ||= { name: null, time: undefined };

        // Get the name of currently active default policy first, to update only it.
        // Excodibur: As for new DBs Influx 1 creates "autogen" policy by default, is unlikely that there is a scenario
        //            where the policy needs to be CREATEd from scratch, since there is always one set. Just keep it in
        //            here for perhaps unknown config-scenarios.
        const command = oldRetention.name ? 'ALTER' : 'CREATE';
        this.log.debug(`Retention policy will be ${command}ed`);

        const retentionName = oldRetention.name ? oldRetention.name : 'global';

        this.log.info(
            `Applying retention policy (${retentionName}) for ${dbname} to ${retention === 0 ? 'infinity' : `${retention} seconds`}. Shard Duration: ${shardDuration} seconds`,
        );
        await this.connection!.query(
            `${command} RETENTION POLICY ${escape.quoted(retentionName)} ON ${escape.quoted(dbname)} DURATION ${retentionSeconds}s REPLICATION 1 SHARD DURATION ${shardDuration}s DEFAULT`,
        );
    }

    async createDatabase(dbname: string): Promise<void> {
        if (!this.connection) {
            return Promise.reject(new Error('No connection to InfluxDB'));
        }
        await this.connection.createDatabase(dbname);
    }

    async dropDatabase(dbname: string): Promise<void> {
        if (!this.connection) {
            return Promise.reject(new Error('No connection to InfluxDB'));
        }
        await this.connection.dropDatabase(dbname);
    }

    /** Convert a value into a point of the `influx` package: everything except time and tags is a field */
    private static toPoint(seriesId: string, pointToSend: ValuesForInflux): IPoint {
        const fields: {
            [name: string]: any;
        } = {};
        Object.keys(pointToSend).forEach(key => {
            if (key === 'time' || key === 'tags') {
                return;
            }
            if (key === 'from' && !pointToSend[key as keyof ValuesForInflux]) {
                return;
            }
            fields[key] = pointToSend[key as keyof ValuesForInflux];
        });

        const point: IPoint = {
            measurement: escape.measurement(seriesId),
            fields,
            timestamp: new Date(pointToSend.time),
        };
        if (pointToSend.tags && Object.keys(pointToSend.tags).length) {
            point.tags = pointToSend.tags;
        }
        return point;
    }

    async writeSeries(series: { [id: string]: ValuesForInflux[] }): Promise<void> {
        if (!this.connection) {
            return Promise.reject(new Error('No connection to InfluxDB'));
        }
        const points: IPoint[] = [];
        for (const seriesId in series) {
            if (Object.prototype.hasOwnProperty.call(series, seriesId)) {
                for (const pointToSend of series[seriesId]) {
                    points.push(DatabaseInfluxDB1x.toPoint(seriesId, pointToSend));
                }
            }
        }
        await this.trackConnection(() => this.connection!.writePoints(points));
    }

    async writePoints(seriesId: string, pointsToSend: ValuesForInflux[]): Promise<void> {
        if (!this.connection) {
            return Promise.reject(new Error('No connection to InfluxDB'));
        }
        const points = pointsToSend.map(pointToSend => DatabaseInfluxDB1x.toPoint(seriesId, pointToSend));

        await this.trackConnection(() => this.connection!.writePoints(points));
    }

    async writePoint(seriesId: string, pointToSend: ValuesForInflux): Promise<void> {
        if (!this.connection) {
            return Promise.reject(new Error('No connection to InfluxDB'));
        }
        await this.trackConnection(() =>
            this.connection!.writePoints([DatabaseInfluxDB1x.toPoint(seriesId, pointToSend)], { precision: 'ms' }),
        );
    }

    async query<T>(query: string): Promise<Array<T & { time: Date }>> {
        if (!this.connection) {
            return Promise.reject(new Error('No connection to InfluxDB'));
        }
        this.log.debug(`Query to execute: ${query}`);
        return await this.trackConnection(() => this.connection!.query<T>(query));
    }

    /**
     * Run a query and keep the measurement each row belongs to.
     *
     * `query()` flattens the series of an InfluxQL answer into one array, which loses the name of
     * the measurement. `SELECT ... FROM /.*\/` returns one series per measurement, so the grouped
     * form of the result is what makes a single query enough here.
     *
     * @param query the InfluxQL query to run
     */
    private async queryGrouped<T>(query: string): Promise<Array<{ name: string; rows: T[] }>> {
        if (!this.connection) {
            return Promise.reject(new Error('No connection to InfluxDB'));
        }
        this.log.debug(`Query to execute: ${query}`);
        const result = (await this.trackConnection(() => this.connection!.query<T>(query))) as unknown as IResults<T>;
        return result?.groups?.() || [];
    }

    async getStatistics(start: number, stop: number): Promise<MeasurementStatistics> {
        const where = ` WHERE time >= '${new Date(start).toISOString()}' AND time <= '${new Date(stop).toISOString()}'`;
        const statistics: MeasurementStatistics = {};

        const entryOf = (name: string): MeasurementStatistics[string] =>
            (statistics[name] ||= { count: 0, firstTs: null, lastTs: null, cardinality: null });

        // `count` over all measurements at once. The regex has to be inlined, InfluxQL has no
        // placeholders for it
        for (const group of await this.queryGrouped<{ count: number }>(`SELECT count("value") FROM /.*/${where}`)) {
            entryOf(group.name).count = Number(group.rows[0]?.count) || 0;
        }

        // `first()` and `last()` have to be asked for separately: in one SELECT InfluxQL reports
        // the beginning of the range as `time` instead of the timestamp of the point
        for (const group of await this.queryGrouped<{ time: Date }>(`SELECT first("value") FROM /.*/${where}`)) {
            const ts = group.rows[0]?.time;
            entryOf(group.name).firstTs = ts ? new Date(ts).getTime() : null;
        }
        for (const group of await this.queryGrouped<{ time: Date }>(`SELECT last("value") FROM /.*/${where}`)) {
            const ts = group.rows[0]?.time;
            entryOf(group.name).lastTs = ts ? new Date(ts).getTime() : null;
        }

        // `SHOW SERIES` reads the index, it does not scan any values - unlike the counts above.
        // A measurement that only exists in the index (all its values are outside the range) is
        // added here, so it is not silently missing from the statistics.
        // Note that the index knows no time: unlike the 2.x driver, which derives the cardinality
        // from the series it actually reads, this is the all-time count even for a limited range
        const seriesRows = await this.query<{ key: string }>('SHOW SERIES');
        const cardinality = cardinalityFromSeriesKeys((seriesRows || []).map(row => row.key).filter(key => !!key));
        for (const [name, count] of Object.entries(cardinality)) {
            entryOf(name).cardinality = count;
        }

        return statistics;
    }

    async dropMeasurement(measurement: string): Promise<void> {
        await this.query(`DROP MEASUREMENT "${measurement.replace(/"/g, '\\"')}"`);
    }
}
