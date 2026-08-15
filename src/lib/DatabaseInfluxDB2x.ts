import { Database, type ValuesForInflux } from './Database';
import { InfluxDB, type QueryApi, type WriteApi, Point } from '@influxdata/influxdb-client';
import { BucketsAPI, OrgsAPI, HealthAPI, DeleteAPI } from '@influxdata/influxdb-client-apis';

// Influx 2.x auth requires token, not user/pw
export default class DatabaseInfluxDB2x extends Database {
    private connection!: InfluxDB;
    private readonly path: string;
    private readonly token: string;
    private readonly organization: string;
    private readonly useTags: boolean;
    private readonly validateSSL: boolean;
    private queryApi!: QueryApi;
    private writeApi!: WriteApi;
    private bucketsApi!: BucketsAPI;
    private orgsApi!: OrgsAPI;
    private healthApi!: HealthAPI;
    private deleteApi!: DeleteAPI;
    private organizationId: string = '';
    private bucketIds: { [buckedId: string]: string } = {};

    constructor(
        options: {
            log: ioBroker.Logger;
            host: string;
            port: number | string;
            protocol: 'http' | 'https';
            database: string;
            requestTimeout: number;
        },
        db2xOptions: {
            path: string;
            token: string;
            organization: string;
            validateSSL: boolean;
            useTags?: boolean;
        },
    ) {
        super(options);
        this.path = db2xOptions.path;
        this.token = db2xOptions.token;
        this.organization = db2xOptions.organization;
        this.useTags = db2xOptions.useTags || false;
        this.validateSSL = db2xOptions.validateSSL;

        this.connect();
    }

    connect(): void {
        const url = `${this.protocol}://${this.host}:${this.port}/${this.path || ''}`;

        this.log.debug(`Connect InfluxDB2: ${url} [${this.database}]`);

        this.connection = new InfluxDB({
            url,
            token: this.token,
            timeout: this.requestTimeout,
            transportOptions: { rejectUnauthorized: this.validateSSL },
        });

        this.queryApi = this.connection.getQueryApi(this.organization);
        this.writeApi = this.connection.getWriteApi(this.organization, this.database, 'ms');
        this.bucketsApi = new BucketsAPI(this.connection);
        this.orgsApi = new OrgsAPI(this.connection);
        this.healthApi = new HealthAPI(this.connection);
        this.deleteApi = new DeleteAPI(this.connection);
    }

    getHostsAvailable(): number {
        return 1; // always one host with InfluxDB 2.x
    }

    async deleteData(
        start: Date | number,
        stop: Date | number,
        org: string,
        dbName: string,
        query: string,
    ): Promise<void> {
        await this.deleteApi.postDelete({
            org,
            bucket: dbName,
            body: {
                start: new Date(start).toISOString(),
                stop: new Date(stop).toISOString(),
                predicate: query,
            },
        });
    }

    async getDatabaseNames(): Promise<string[]> {
        this.log.debug(`Organization being checked: ${this.organization}`);

        const organizations = await this.orgsApi.getOrgs({ org: this.organization });
        this.log.debug(`Organizations: ${JSON.stringify(organizations)}`);
        if (!organizations?.orgs?.length) {
            throw new Error(
                'No organizations exists or the token do not have proper permissions. Please check the token (see Readme for Tipps)!',
            );
        }
        this.organizationId = organizations.orgs[0].id || '';
        if (!this.organizationId) {
            throw new Error(`Could not find organization ID for organization "${this.organization}"`);
        }

        const buckets = await this.bucketsApi.getBuckets({ orgID: this.organizationId });
        this.log.debug(`Buckets: ${JSON.stringify(buckets)}`);

        const foundDatabases: string[] = [];

        buckets.buckets?.forEach(bucket => {
            if (bucket.name && bucket.id) {
                foundDatabases.push(bucket.name);
                this.bucketIds[bucket.name] = bucket.id;
            }
        });

        return foundDatabases;
    }

    async getRetentionPolicyForDB(dbName: string): Promise<{ name: string | null; time: number | undefined } | null> {
        this.log.debug(`Getting retention policy for ${dbName}`);
        try {
            const bucketData = await this.bucketsApi.getBucketsID({ bucketID: this.bucketIds[dbName] });
            this.log.debug(`Found retention policy: ${bucketData.retentionRules[0].everySeconds} seconds`);
            return { time: bucketData.retentionRules[0].everySeconds, name: dbName };
        } catch (error) {
            this.log.error(error);
            return null;
        }
    }

    async applyRetentionPolicyToDB(dbName: string, retention: number): Promise<void> {
        // Skip the PATCH request if the retention is already as desired (avoids an API call on every connect)
        const currentRetention = await this.getRetentionPolicyForDB(dbName);
        if (currentRetention && currentRetention.time === retention) {
            this.log.debug(`Retention policy for ${dbName} remains unchanged.`);
            return;
        }

        const shardGroupDuration = this.calculateShardGroupDuration(retention);
        this.log.info(
            `Applying retention policy for ${dbName} to ${!retention ? 'infinity' : `${retention} seconds`}. Shard Group Duration (calculated): ${shardGroupDuration} seconds`,
        );
        await this.bucketsApi.patchBucketsID({
            bucketID: this.bucketIds[dbName],
            body: {
                retentionRules: [
                    {
                        type: 'expire',
                        everySeconds: retention,
                        shardGroupDurationSeconds: shardGroupDuration,
                    },
                ],
            },
        });
    }

    async createDatabase(dbname: string): Promise<void> {
        this.log.info(`Creating database ${dbname} for orgId ${this.organizationId}`);
        const newBucket = await this.bucketsApi.postBuckets({
            body: {
                orgID: this.organizationId,
                name: dbname,
            },
        });

        this.bucketIds[dbname] = newBucket.id || '';
    }

    async dropDatabase(dbname: string): Promise<void> {
        this.log.info(`Dropping database ${dbname} for orgId "${this.organizationId}"`);
        await this.bucketsApi.deleteBucketsID({ bucketID: this.bucketIds[dbname] });
    }

    async writeSeries(series: { [id: string]: ValuesForInflux[] }): Promise<void> {
        this.log.debug(`Write series: ${JSON.stringify(series)}`);

        const points: Point[] = [];
        for (const [pointId, valueSets] of Object.entries(series)) {
            valueSets.forEach(value => {
                points.push(this.stateValueToPoint(pointId, value));
            });
        }

        this.writeApi.writePoints(points);
        await this.writeApi.flush();
        this.log.debug(`Points written to ${this.database}`);
    }

    async writePoints(seriesId: string, pointsToSend: ValuesForInflux[]): Promise<void> {
        this.log.debug(`Write Points: ${seriesId} pointsToSend:${JSON.stringify(pointsToSend)}`);

        const points: Point[] = [];
        pointsToSend.forEach(value => {
            points.push(this.stateValueToPoint(seriesId, value));
        });

        this.writeApi.writePoints(points);
        await this.writeApi.flush();
        this.log.debug(`Points written to ${this.database}`);
    }

    async writePoint(seriesId: string, value: ValuesForInflux): Promise<void> {
        this.log.debug(`Write Point: ${seriesId} values:${JSON.stringify(value)}`);
        this.writeApi.writePoint(this.stateValueToPoint(seriesId, value));
        await this.writeApi.flush();
        this.log.debug(`Point written to ${this.database}`);
    }

    stateValueToPoint(pointName: string, stateValue: ValuesForInflux): Point {
        let point = null;

        if (this.useTags) {
            point = new Point(pointName)
                .timestamp(stateValue.time)
                .tag('q', String(stateValue.q))
                .tag('ack', String(stateValue.ack))
                .tag('from', stateValue.from);
        } else {
            point = new Point(pointName)
                .timestamp(stateValue.time)
                .floatField('q', stateValue.q)
                .booleanField('ack', stateValue.ack)
                .stringField('from', stateValue.from);
        }

        switch (typeof stateValue.value) {
            case 'boolean':
                point.booleanField('value', stateValue.value);
                break;
            case 'number':
                point.floatField('value', parseFloat(stateValue.value as unknown as string));
                break;
            case 'string':
            default:
                point.stringField('value', stateValue.value);
                break;
        }
        return point;
    }

    query<T>(query: string): Promise<Array<T & { time: Date }>> {
        this.log.debug(`Query to execute: ${query}`);

        return new Promise((resolve, reject) => {
            const rows: Array<T & { time: Date }> = [];
            this.queryApi.queryRows(query, {
                next(row, tableMeta) {
                    const fields = tableMeta.toObject(row);

                    // Columns "_time" and "_value" are mapped to "time" and "value" for backwards compatibility
                    if ((fields as any)._time !== null) {
                        fields.time = (fields as any)._time;
                    }

                    rows.push(fields as T & { time: Date });
                },
                error(error) {
                    // Ignore errors that are related to an empty range. The handling of this currently seems inconsistent for flux.
                    // See also https://github.com/influxdata/flux/issues/3543
                    if (error.message.match('.*cannot query an empty range.*')) {
                        resolve(rows);
                    } else {
                        reject(error);
                    }
                },
                complete() {
                    resolve(rows);
                },
            });
        });
    }

    async getMetaDataStorageType(): Promise<'tags' | 'fields' | 'none'> {
        const queries = [
            `import "influxdata/influxdb/schema" schema.tagKeys(bucket: "${this.database}")`,
            `import "influxdata/influxdb/schema" schema.fieldKeys(bucket: "${this.database}")`,
        ];

        const result = await this.queries<{ _value: string }>(queries);
        if (!result) {
            throw new Error('Could not determine metadata storage type');
        }
        let storageType: 'tags' | 'fields' | 'none' = 'none';
        this.log.debug(`Result of metadata storage type check: ${JSON.stringify(result)}`);
        for (let i = 0; i <= 1; i++) {
            result[i].forEach(row => {
                switch (row._value) {
                    case 'q':
                    case 'ack':
                    case 'from':
                        storageType = !i ? 'tags' : 'fields';
                        return;
                }
            });
        }

        return storageType;
    }

    async ping(): Promise<{ online: boolean }[]> {
        // can't do much with interval, so ignoring it for compatibility reasons
        const result = await this.healthApi.getHealth();
        return result.status === 'pass' ? [{ online: true }] : [{ online: false }];
    }
}
