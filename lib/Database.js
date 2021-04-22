'use strict';

class Database {
    connect () {}
    getDatabaseNames (callback) {

        //callback (error, dbnames);
    }
    createDatabase(dbname, callback_error) {}
    createRetentionPolicyForDB(dbname, retention, callback_error) {}
    dropDatabase(dbname, callback_error) {}

    // request
    // request -> getHostsAvailable() {}
    writeSeries(series, callback_error) {}
    writePoints(seriesId, pointsToSend, callback_error) {}
    writePoint(seriesName, values, options, callback_error) {}
    
    query(query, callback) {
        //callback(error, rows);
    }

    ping(interval) {}
}

module.exports = Database;
