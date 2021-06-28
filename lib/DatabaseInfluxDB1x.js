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

    createRetentionPolicyForDB(dbname, retention, callback) {
        this.connection.query('CREATE RETENTION POLICY "global" ON ' + dbname + ' DURATION ' + retention + 's REPLICATION 1 DEFAULT', callback);
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
        this.log.info("Query to execute: " + query);
        this.connection.query(query, callback);
    }

    /*ping(interval) {
        this.connection.ping && this.connection.ping(interval);
    }*/
}

module.exports = {
    DatabaseInfluxDB1x : DatabaseInfluxDB1x
  }