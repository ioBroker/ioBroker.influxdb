import { InfluxDB, type IPoint, escape } from 'influx';
import { Database, type ValuesForInflux } from './Database';

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

    getHostsAvailable(): number {
        return 1;
    }

    async ping(): Promise<{ online: boolean }[]> {
        if (!this.connection) {
            return Promise.reject(new Error('No connection to InfluxDB'));
        }
        const hosts = await this.connection.ping(500);
        return hosts.map(host => ({ online: host.online }));
    }

    getDatabaseNames(): Promise<string[]> {
        if (!this.connection) {
            return Promise.reject(new Error('No connection to InfluxDB'));
        }
        return this.connection.getDatabaseNames();
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

    async writeSeries(series: { [id: string]: ValuesForInflux[] }): Promise<void> {
        if (!this.connection) {
            return Promise.reject(new Error('No connection to InfluxDB'));
        }
        const points: IPoint[] = [];
        for (const seriesId in series) {
            if (Object.prototype.hasOwnProperty.call(series, seriesId)) {
                const pointsToSend = series[seriesId];
                for (const pointToSend of pointsToSend) {
                    const fields: {
                        [name: string]: any;
                    } = {};
                    Object.keys(pointToSend).forEach(key => {
                        if (key === 'time') {
                            return;
                        }
                        if (key === 'from' && !pointToSend[key as keyof ValuesForInflux]) {
                            return;
                        }
                        fields[key] = pointToSend[key as keyof ValuesForInflux];
                    });

                    points.push({
                        measurement: escape.measurement(seriesId),
                        fields,
                        timestamp: new Date(pointToSend.time),
                    });
                }
            }
        }
        await this.connection.writePoints(points);
    }

    async writePoints(seriesId: string, pointsToSend: ValuesForInflux[]): Promise<void> {
        if (!this.connection) {
            return Promise.reject(new Error('No connection to InfluxDB'));
        }
        const points: IPoint[] = [];
        for (const pointToSend of pointsToSend) {
            const fields: {
                [name: string]: any;
            } = {};
            Object.keys(pointToSend).forEach(key => {
                if (key === 'time') {
                    return;
                }
                if (key === 'from' && !pointToSend[key as keyof ValuesForInflux]) {
                    return;
                }
                fields[key] = pointToSend[key as keyof ValuesForInflux];
            });
            points.push({
                measurement: escape.measurement(seriesId),
                fields,
                timestamp: new Date(pointToSend.time),
            });
        }

        await this.connection.writePoints(points);
    }

    async writePoint(seriesId: string, pointToSend: ValuesForInflux): Promise<void> {
        if (!this.connection) {
            return Promise.reject(new Error('No connection to InfluxDB'));
        }
        const fields: {
            [name: string]: any;
        } = {};
        Object.keys(pointToSend).forEach(key => {
            if (key === 'time') {
                return;
            }
            if (key === 'from' && !pointToSend[key as keyof ValuesForInflux]) {
                return;
            }
            fields[key] = pointToSend[key as keyof ValuesForInflux];
        });
        await this.connection.writePoints(
            [
                {
                    measurement: escape.measurement(seriesId),
                    fields,
                    timestamp: new Date(pointToSend.time),
                },
            ],
            { precision: 'ms' },
        );
    }

    async query<T>(query: string): Promise<Array<T & { time: Date }>> {
        if (!this.connection) {
            return Promise.reject(new Error('No connection to InfluxDB'));
        }
        this.log.debug(`Query to execute: ${query}`);
        return await this.connection.query<T>(query);
    }
}
