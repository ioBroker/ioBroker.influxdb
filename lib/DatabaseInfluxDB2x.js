'use strict';

const Database = require('./Database.js');
const InfluxClient = require('@influxdata/influxdb-client');
const InfluxClientApis = require('@influxdata/influxdb-client-apis');

/* Old node-influx lib had some connection-pool handling that is not present in new influx lib,
   so to not break old code we use a fictional pool here. */
class FakeConnectionPool {
    constructor() {
        this.connections = [];
    }

    activatePool () {
        this.connections.push('fakehost');
    }

    getHostsAvailable () {
        return this.connections;
    }
}

//Influx 2.x auth requires token, not user/pw
class DatabaseInfluxDB2x extends Database {
    constructor(log, host, port, protocol, path, token, organization, database, requestTimeout, validateSSL, useTags, timePrecision) {
        super();

        this.log = log;

        this.host = host;
        this.port = port;
        this.protocol = protocol;
        this.path = path;
        if (this.path.startsWith('/')) {
            this.path = this.path.substring(1);
        }
        this.token = token;
        this.organization = organization;
        this.database = database;
        this.timePrecision = timePrecision; // ms
        this.requestTimeout = requestTimeout; // 30000
        this.useTags = useTags || false;
        this.validateSSL = validateSSL;

        this.request = new FakeConnectionPool();

        this.connect();
    }

    connect() {
        const url = `${this.protocol}://${this.host}:${this.port}/${this.path || ''}`;

        this.log.debug(`Connect InfluxDB2: ${url} [${this.database}]`);

        this.connection = new InfluxClient.InfluxDB({
            url,
            token: this.token,
            timeout: this.requestTimeout,
            transportOptions: {rejectUnauthorized: this.validateSSL},
        });

        this.queryApi = this.connection.getQueryApi(this.organization);
        this.writeApi = this.connection.getWriteApi(this.organization, this.database, this.timePrecision);

        this.bucketsApi = new InfluxClientApis.BucketsAPI(this.connection);
        this.orgsApi = new InfluxClientApis.OrgsAPI(this.connection);
        this.healthApi = new InfluxClientApis.HealthAPI(this.connection);
        this.deleteApi = new InfluxClientApis.DeleteAPI(this.connection);

        this.bucketIds = [];

        this.request.activatePool();
    }

    async getDatabaseNames(callback) {
        this.log.debug(`Organization being checked: ${this.organization}`);

        try {
            const organizations = await this.orgsApi.getOrgs({ org: this.organization });
            this.log.debug(`Organizations: ${JSON.stringify(organizations)}`);
            if (organizations && organizations.orgs && !organizations.orgs.length) {
                throw new Error('No organizations exists or the token do not have proper permissions. Please check the token!')
            }
            this.organizationId = organizations.orgs[0].id;

            const buckets = await this.bucketsApi.getBuckets({ orgID: this.organizationId });
            this.log.debug(`Buckets: ${JSON.stringify(buckets)}`);

            const foundDatabases = [];

            buckets.buckets.forEach((bucket) => {
                foundDatabases.push(bucket.name);
                this.bucketIds[bucket.name] = bucket.id;
            });

            callback(null, foundDatabases)
        } catch (error) {
            callback(error, null);
        }
    }

    async getRetentionPolicyForDB(dbname, callback) {
        this.log.debug(`Getting retention policy for ${dbname}`);
        try {
            const bucketData = await this.bucketsApi.getBucketsID({ bucketID: this.bucketIds[dbname] });
            this.log.debug(`Found retention policy: ${bucketData.retentionRules[0].everySeconds} seconds`);
            callback({time: bucketData.retentionRules[0].everySeconds });
        } catch (error) {
            this.log.error(error);
            callback(null);
        }
    }

    async applyRetentionPolicyToDB(dbname, retention, callback_error) {
        const shardGroupDuration = this.calculateShardGroupDuration(retention);
        this.log.info(`Applying retention policy for ${dbname} to ${!retention ? "infinity" : retention + " seconds"}. Shard Group Duration (calculated): ${shardGroupDuration} seconds`);
        try {
            await this.bucketsApi.patchBucketsID({
                bucketID: this.bucketIds[dbname],
                body: {
                    retentionRules: [{
                        type: 'expire',
                        everySeconds: parseInt(retention),
                        shardGroupDurationSeconds: shardGroupDuration
                    }]
                }
            });
            callback_error(null);
        } catch (error) {
            this.log.error(error);
            callback_error(error);
        }
    }

    async createDatabase(dbname, callback_error) {
        try {
            this.log.info(`Creating database ${dbname} for orgId ${this.organizationId}`);
            const newBucket = await this.bucketsApi.postBuckets({
                body: {
                    orgID: this.organizationId,
                    name: dbname
                }
            });

            this.bucketIds[dbname] = newBucket.id;
            callback_error(false);
        } catch (error) {
            this.log.error(error);
            callback_error(true);
        }
    }

    async dropDatabase(dbname, callback_error) {
        try {
            this.log.info(`Dropping database ${dbname} for orgId ${this.organizationId}`);
            await this.bucketsApi.deleteBucketsID({ bucketID: this.bucketIds[dbname] });

            callback_error(false);
        } catch (error) {
            this.log.error(error);
            callback_error(true);
        }
    }

    async writeSeries(series, callback_error) {
        this.log.debug(`Write series: ${JSON.stringify(series)}`);

        const points = [];
        for (const [pointId, valueSets] of Object.entries(series)) {
            valueSets.forEach((values) => {
                values.forEach((value) => {
                    points.push(this.stateValueToPoint(pointId, value));
                });
            });
        }

        try {
            this.writeApi.writePoints(points);
            await this.writeApi.flush();
            this.log.debug(`Points written to ${this.database}`);
            callback_error();
        } catch (error) {
            callback_error(error);
        }
    }

    async writePoints(pointId, pointsToSend, callback_error) {
        this.log.debug(`Write Points: ${pointId} pointstoSend:${JSON.stringify(pointsToSend)}`);

        const points = [];
        pointsToSend.forEach((values) => {
            values.forEach((value) => {
                points.push(this.stateValueToPoint(pointId, value));
            });
        });

        try {
            this.writeApi.writePoints(points);
            await this.writeApi.flush();
            this.log.debug(`Points written to ${this.database}`);
            callback_error();
        } catch (error) {
            callback_error(error);
        }
    }

    async writePoint(pointId, values, options, callback_error) {
        this.log.debug(`Write Point: ${pointId} values:${JSON.stringify(values)} options: ${JSON.stringify(options)}`);

        try {
            this.writeApi.writePoint(this.stateValueToPoint(pointId, values));
            await this.writeApi.flush();
            this.log.debug(`Point written to ${this.database}`);
            callback_error();
        } catch (error) {
            this.log.warn(`Point could not be written to database: ${this.database}`);
            callback_error(error);
        }

    }

    stateValueToPoint(pointName, stateValue) {
        let point = null;

        if (this.useTags) {
            point = new InfluxClient.Point(pointName)
                .timestamp(stateValue.time)
                .tag('q', String(stateValue.q))
                .tag('ack', String(stateValue.ack))
                .tag('from', stateValue.from);
        } else {
            point = new InfluxClient.Point(pointName)
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
                point.floatField('value', parseFloat(stateValue.value));
                break;
            case 'string':
            default:
                point.stringField('value', stateValue.value);
                break;
        }
        return point;
    }

    query(query, callback) {
        this.log.debug(`Query to execute: ${query}`);
        let rows = [];

        this.queryApi.queryRows(query, {
            next (row, tableMeta) {
                const fields = tableMeta.toObject(row);

                //Columns "_time" and "_value" are mapped to "time" and "value" for backwards compatibility
                if (fields['_time'] !== null)
                    fields['time'] = fields['_time'];

                //if (fields["_value"] !== null)
                //    fields["value"] = fields["_value"];

                rows.push(fields);

                // console.log(JSON.stringify(o, null, 2))
            },
            error (error) {
                //Ignore errors that are related to an empty range. The handling of this currently seems inconsistent for flux.
                //See also https://github.com/influxdata/flux/issues/3543
                if (error.message.match('.*cannot query an empty range.*'))
                    callback(null, rows);
                else
                    callback(error, null);
            },
            complete () {
                callback(null, rows);

            }
        });
    }

    getMetaDataStorageType (callback) {
        const queries = [
            `import "influxdata/influxdb/schema" schema.tagKeys(bucket: "${this.database}")`,
            `import "influxdata/influxdb/schema" schema.fieldKeys(bucket: "${this.database}")`,
        ];

        this.queries(queries, (error, result) => {
            let storageType = "none";
            if (error) {
                callback(error, null);
            } else {
                this.log.debug(`Result of metadata storage type check: ${JSON.stringify(result)}`);
                for (let i = 0; i <= 1; i++) {
                    result[i].forEach(row => {
                        switch(row._value) {
                            case 'q':
                            case 'ack':
                            case 'from':
                                storageType = i == 0 ? 'tags' : 'fields';
                                return;
                        }
                    });
                }
                callback(null, storageType);
            }

        });
    }

    async queries (queries, callback) {
        const collectedRows = [];
        let success = false;
        let errors = [];
        for (const query of queries) {
            await new Promise((resolve, reject) => {
                this.query(query, (error, rows) => {
                    if (error) {
                        this.log.warn(`Error in query "${query}": ${error}`);
                        errors.push(error);
                        collectedRows.push([]);
                    } else {
                        success = true;
                        collectedRows.push(rows);
                    }
                    resolve();
                });
            });
        }

        let retError = null;
        if (errors.length) {
            retError = new Error(`${errors.length} Error happened while processing ${queries.length} queries`);
            retError.errors = errors;
        }
        callback(retError, success ? collectedRows : null);
    }

    ping(interval) {
        // can't do much with interval, so ignoring it for compatibility reasons
        return this.healthApi.getHealth()
            .then(result => result.status === 'pass' ? [{ online: true }] : [{ online: false }]);
    }
}

module.exports = {
    DatabaseInfluxDB2x: DatabaseInfluxDB2x
}
