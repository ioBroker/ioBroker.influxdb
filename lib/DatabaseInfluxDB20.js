'use strict';

const Database = require('./Database.js');
const InfluxDB = require('@influxdata/influxdb-client').InfluxDB;
const BucketsAPI = require('@influxdata/influxdb-client-apis').BucketsAPI;
const OrgsAPI = require('@influxdata/influxdb-client-apis').OrgsAPI;

//import InfluxDB from '@influxdata/influxdb-client'


//Influx 2.0 auth requires token, not user/pw
class DatabaseInfluxDB20 extends Database{
    constructor (log, host, port, protocol, token, organization, database, timePrecision, requestTimeout) {
        super();

        this.log = log;

        this.host = host;
        this.port = port;
        this.protocol = protocol;
        this.token = token;
        this.organization = organization;
        this.database = database;
        this.timePrecision = timePrecision; // ms
        this.requestTimeout = requestTimeout; // 30000

        //disable ping
        this.ping = false;

        this.connect();
    }

    connect () {
        const url = this.protocol + '://' + this.host + ':' + this.port + '/';

        this.connection = new InfluxDB({
            url:     url,
            token:   this.token,
            //timeout: this.requestTimeout, //?? is there something like request timeout? Here it is about socket-timeout
/*            database: this.database,
            timePrecision: this.timePrecision,
            requestTimeout: this.requestTimeout*/
        });

        this.queryApi = this.connection.getQueryApi(this.organization);
        this.bucketsApi = new BucketsAPI(this.connection);
        this.orgsApi = new OrgsAPI(this.connection);
/*
       */
       
        //this.request = this.connection.request;
    }

    getDatabaseNames (callback) {
        /*const fluxQuery = "buckets()";
        let databases = [];
        this.queryApi.queryRows(fluxQuery, {
            next(row , tableMeta) {
                const o = tableMeta.toObject(row);
                databases.push(o.name);
                // console.log(JSON.stringify(o, null, 2))
            },
            error(error) {
                console.log('\nFinished ERROR');
                callback(error, null);
            },
            complete() {
                console.log('\nFinished SUCCESS');
                callback(null, databases);

            }
        });*/
        this.orgsApi.getOrgs({org: this.organization})
            .then((organizations) => {
                this.organizationId = organizations.orgs[0].id;

                this.bucketsApi.getBuckets({orgId: this.organizationId, name: this.database})
                    .then((buckets) => {
                        this.log.info("Buckets: " + buckets.buckets);
                        callback(null, buckets.buckets)
                    })
                    .catch((error) => {
                        this.log.info("error with getting buckets:" + error);
                    });
            })
            .catch((error) => {
                this.log.info("error with getting orgId:" + error);
            });
    }

    createRetentionPolicyForDB(dbname, retention, callback) {
        //Not needed, done on bucket creation
        //this.connection.query('CREATE RETENTION POLICY "global" ON ' + adapter.config.dbname + ' DURATION ' + adapter.config.retention + 's REPLICATION 1 DEFAULT', callback);
    }

    createDatabase(dbname, callback_error) {
        //this.connection.createDatabase(dbname, callback_error);
        try {
            this.bucketsApi.postBuckets({ body: {
                orgId: this.organizationId,
                name: dbname,
                retentionRules: [{type: "expire", everySeconds: adapter.config.retention}]
            }});
        } catch (e) {
            callback_error(e);
        }
    }

    dropDatabase(dbname, callback_error) {
        this.connection.dropDatabase(dbname, callback_error);
    }

    // request
    // request -> getHostsAvailable() {}
    writeSeries(series, callback_error) {
        this.connection.writeSeries(series, callback_error);
    }

    writePoints(seriesId, pointsToSend, callback_error) {
        this.connection.writePoints(seriesId, pointsToSend, callback_error);
    }

    writePoint(seriesName, values, options, callback_error) {
        this.connection.writePoint(seriesName, values, options, callback_error);
    }
    
    query(query, callback) {
        this.connection.query(query, callback);
        //callback(error, rows);
    }

    /*ping(interval) {
        this.connection.ping && this.connection.ping(interval);
    }*/
}

module.exports = {
    DatabaseInfluxDB20 : DatabaseInfluxDB20, //export this class
    Database: Database // and export parent class too!
  }