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

    calculateShardGroupDuration(retentionTime) { // in seconds
        // Shard Group Duration according to official Influx recommendations
        if (!retentionTime) { // infinite
            return 604800; // 7 days
        } else if (retentionTime < 172800) {// < 2 days
            return 3600; // 1 hour
        } else if (retentionTime >= 172800 && retentionTime <= 15811200) {// >= 2 days, <= 6 months (~182 days)
            return 86400; // 1 day
        } else {// > 6 months
            return 604800; // 7 days
        }
    }
}

module.exports = Database;
