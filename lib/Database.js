'use strict';

class Database {
    connect () {}
    getDatabaseNames (callback) {}
    createDatabase(dbname, callback_error) {}
    createRetentionPolicyForDB(dbname, retention, callback_error) {}
    dropDatabase(dbname, callback_error) {}

    writeSeries(series, callback_error) {}
    writePoints(seriesId, pointsToSend, callback_error) {}
    writePoint(seriesName, values, options, callback_error) {}
    
    query(query, callback) {}

    //ping(interval) {}
}

module.exports = Database;
