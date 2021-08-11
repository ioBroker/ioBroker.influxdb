'use strict';

const Database = require('./Database.js');
const influx = require('influx');

class DatabaseInfluxDB1x extends Database{
    constructor (log, host, port, protocol, username, password, database, timePrecision, requestTimeout) {
        super();

        this.log = log;

        this.host = host;
        this.port = port;
        this.protocol = protocol;
        this.username = username;
        this.password = password;
        this.database = database;
        this.timePrecision = timePrecision; // ms
        this.requestTimeout = requestTimeout; // 30000

        //disable ping
        this.ping = false;

        this.connect();
    }

    connect () {
        this.connection = influx({
            host:     this.host,
            port:     this.port, // optional, default 8086
            protocol: this.protocol, // optional, default 'http'
            username: this.username,
            password: this.password,
            database: this.database,
            timePrecision: this.timePrecision,
            requestTimeout: this.requestTimeout
        });

        this.request = this.connection.request;
    }

    getDatabaseNames (callback) {
        this.connection.getDatabaseNames(callback);
    }

    getRetentionPolicyForDB(dbname, callback) {
        this.connection.query('SHOW RETENTION POLICIES ON ' + dbname, (err, rows) => {
            if(err){
                this.log.error("Error retrieving retention policy: " + err);
                callback(null);
            } else {
                const regex = /(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?/;
                let retentionTime, retentionName = null;
                rows[0].forEach(row => {
                    if (row.default) {
                        const regMatch = row.duration.match(regex);
                        const retHours = parseInt(regMatch[1]);
                        const retMinutes = parseInt(regMatch[2]);
                        const retSeconds = parseInt(regMatch[3]);
                        retentionTime = retHours * 60 * 60 + retMinutes * 60 + retSeconds;
                        retentionName = row.name;
                    }
                });
                callback({"name": retentionName, "time": retentionTime});
            }
        });
    }

    async applyRetentionPolicyToDB(dbname, retention, callback) {
        this.getRetentionPolicyForDB(dbname, oldRetention => {
            //Check, if it needs to be changed, otherwise skip
            this.log.debug("old retention: " + oldRetention + " new retention: " + retention);
            if ((oldRetention.time !== null) && (oldRetention.time === retention)) {
                this.log.debug("Retention policy for " + dbname + " remains unchanged.");
                callback(false);
                return;
            }

            const shardDuration = this.calculateShardGroupDuration(retention);

            //Get name of currently active default policy first, to update only it.

            const command = (oldRetention.time != null) ? "ALTER" : "CREATE";
            const retentionName = (oldRetention.name != null) ? oldRetention.name : "global";

            this.log.info("Applying retention policy (" + retentionName + ") for " + dbname + " to " + ((retention === 0) ? "infinity" :  retention + " seconds") + ". Shard Duration: " + shardDuration + " seconds");
            this.connection.query(command + ' RETENTION POLICY "' + retentionName + '" ON ' + dbname + ' DURATION ' + retention + 's REPLICATION 1 SHARD DURATION ' + shardDuration + 's DEFAULT', callback);
        });
    }

    createDatabase(dbname, callback_error) {
        this.connection.createDatabase(dbname, callback_error);
    }

    dropDatabase(dbname, callback_error) {
        this.connection.dropDatabase(dbname, callback_error);
    }

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
        this.log.debug("Query to execute: " + query);
        this.connection.query(query, callback);
    }

    /*ping(interval) {
        // Ping is only used internally. As _pool is private it won't work like below
        this.connection._pool.ping(interval);
    }*/
}

module.exports = {
    DatabaseInfluxDB1x : DatabaseInfluxDB1x
  }
