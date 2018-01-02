/* jshint -W097 */// jshint strict:false
/*jslint node: true */
'use strict';

//noinspection JSUnresolvedFunction
var utils     = require(__dirname + '/lib/utils'); // Get common adapter utils
var influx    = require('influx');
var fs        = require('fs');
var path      = require('path');
var Aggregate = require(__dirname + '/lib/aggregate.js');
var dataDir   = path.normalize(utils.controllerDir + '/' + require(utils.controllerDir + '/lib/tools').getDefaultDataDir());
var cacheFile = dataDir + 'influxdata.json';

var subscribeAll        = false;
var influxDPs           = {};
var client;
var seriesBufferChecker = null;
var seriesBufferCounter = 0;
var seriesBufferFlushPlanned = false;
var seriesBuffer        = {};
var conflictingPoints   = {};
var errorPoints         = {};
var tasksStart          = [];
var connected           = null;
var finished            = false;

var adapter = utils.Adapter('influxdb');

adapter.on('objectChange', function (id, obj) {
    if (obj && obj.common &&
        (
            // todo remove history sometime (2016.08) - Do not forget object selector in io-package.json
            (obj.common.history && obj.common.history[adapter.namespace] && obj.common.history[adapter.namespace].enabled) ||
            (obj.common.custom  && obj.common.custom[adapter.namespace]  && obj.common.custom[adapter.namespace].enabled)
        )
    ) {

        if (!influxDPs[id] && !subscribeAll) {
            // unsubscribe
            for (var _id in influxDPs) {
                adapter.unsubscribeForeignStates(_id);
            }
            subscribeAll = true;
            adapter.subscribeForeignStates('*');
        }
        if (influxDPs[id] && influxDPs[id].relogTimeout) clearTimeout(influxDPs[id].relogTimeout);

        // todo remove history sometime (2016.08)
        influxDPs[id] = obj.common.custom || obj.common.history;
        if (influxDPs[id][adapter.namespace].retention !== undefined && influxDPs[id][adapter.namespace].retention !== null && influxDPs[id][adapter.namespace].retention !== '') {
            influxDPs[id][adapter.namespace].retention = parseInt(influxDPs[id][adapter.namespace].retention || adapter.config.retention, 10) || 0;
        } else {
            influxDPs[id][adapter.namespace].retention = adapter.config.retention;
        }
        if (influxDPs[id][adapter.namespace].debounce !== undefined && influxDPs[id][adapter.namespace].debounce !== null && influxDPs[id][adapter.namespace].debounce !== '') {
            influxDPs[id][adapter.namespace].debounce = parseInt(influxDPs[id][adapter.namespace].debounce, 10) || 0;
        } else {
            influxDPs[id][adapter.namespace].debounce = adapter.config.debounce;
        }
        influxDPs[id][adapter.namespace].changesOnly = influxDPs[id][adapter.namespace].changesOnly === 'true' || influxDPs[id][adapter.namespace].changesOnly === true;
        if (influxDPs[id][adapter.namespace].changesRelogInterval !== undefined && influxDPs[id][adapter.namespace].changesRelogInterval !== null && influxDPs[id][adapter.namespace].changesRelogInterval !== '') {
            influxDPs[id][adapter.namespace].changesRelogInterval = parseInt(influxDPs[id][adapter.namespace].changesRelogInterval, 10) || 0;
        } else {
            influxDPs[id][adapter.namespace].changesRelogInterval = adapter.config.changesRelogInterval;
        }
        if (influxDPs[id][adapter.namespace].changesMinDelta !== undefined && influxDPs[id][adapter.namespace].changesMinDelta !== null && influxDPs[id][adapter.namespace].changesMinDelta !== '') {
            influxDPs[id][adapter.namespace].changesMinDelta = parseFloat(influxDPs[id][adapter.namespace].changesMinDelta.toString().replace(/,/g, '.')) || 0;
        } else {
            influxDPs[id][adapter.namespace].changesMinDelta = adapter.config.changesMinDelta;
        }
        if (!influxDPs[id][adapter.namespace].storageType) influxDPs[id][adapter.namespace].storageType = false;

        // add one day if retention is too small
        if (influxDPs[id][adapter.namespace].retention && influxDPs[id][adapter.namespace].retention <= 604800) {
            influxDPs[id][adapter.namespace].retention += 86400;
        }

        writeInitialValue(id);

        adapter.log.info('enabled logging of ' + id + ', ' + Object.keys(influxDPs).length + ' points now activated');
    } else {
        if (influxDPs[id]) {
            if (influxDPs[id].relogTimeout) clearTimeout(influxDPs[id].relogTimeout);
            if (influxDPs[id].timeout) clearTimeout(influxDPs[id].timeout);
            delete influxDPs[id];
            adapter.log.info('disabled logging of ' + id + ', ' + Object.keys(influxDPs).length + ' points now activated');
        }
    }
});

adapter.on('stateChange', function (id, state) {
    pushHistory(id, state);
});

adapter.on('ready', function () {
    main();
});

adapter.on('unload', function (callback) {
    finish(callback);
});

adapter.on('message', function (msg) {
    processMessage(msg);
});

process.on('SIGINT', function () {
    if (adapter && adapter.setState) {
        finish();
    }
});
process.on('SIGTERM', function () {
    if (adapter && adapter.setState) {
        finish();
    }
});

process.on('uncaughtException', function (err) {
    adapter.log.warn('Exception: ' + err);
    if (adapter && adapter.setState) {
        finish();
    }
});

function setConnected(isConnected) {
    if (connected !== isConnected) {
        connected = isConnected;
        adapter.setState('info.connection', connected, true, function (err) {
            // analyse if the state could be set (because of permissions)
            if (err) adapter.log.error('Can not update connected state: ' + err);
              else adapter.log.debug('connected set to ' + connected);
        });
    }
}

function connect() {
    adapter.log.info('Connecting ' + adapter.config.protocol + '://' + adapter.config.host + ':' + adapter.config.port + ' ...');

    adapter.config.dbname = adapter.config.dbname || utils.appName;

    adapter.config.seriesBufferMax = parseInt(adapter.config.seriesBufferMax, 10) || 0;

    client = influx({
        host:     adapter.config.host,
        port:     adapter.config.port, // optional, default 8086
        protocol: adapter.config.protocol, // optional, default 'http'
        username: adapter.config.user,
        password: adapter.config.password,
        database: adapter.config.dbname,
        timePrecision: 'ms'
    });

    client.getDatabaseNames(function (err, dbNames) {
        if (err) {
            adapter.log.error(err);
            setConnected(false);
        } else {
            setConnected(true);
            if (dbNames.indexOf(adapter.config.dbname) === -1) {
                client.createDatabase(adapter.config.dbname, function (err) {
                    if (err) {
                        adapter.log.error(err);
                        setConnected(false);
                    } else {
                        if (!err && adapter.config.retention) {
                            client.query('CREATE RETENTION POLICY "global" ON ' + adapter.config.dbname + ' DURATION ' + adapter.config.retention + 's REPLICATION 1 DEFAULT', function (err) {
                                if (err && err.toString().indexOf('already exists') === -1) {
                                    adapter.log.error(err);
                                }
                            });
                        }
                        processStartValues();
                        adapter.log.info('Connected!');
                    }
                });
            }
            else {
                processStartValues();
                adapter.log.info('Connected!');
            }
        }
    });
}

function processStartValues() {
    if (tasksStart && tasksStart.length) {
        var taskId = tasksStart.shift();
        if (influxDPs[taskId][adapter.namespace].changesOnly) {
            pushHistory(taskId, influxDPs[taskId].state, true);
            setTimeout(processStartValues, 0);
        }
    }
}

function testConnection(msg) {
    msg.message.config.port = parseInt(msg.message.config.port, 10) || 0;

    var timeout;
    try {
        timeout = setTimeout(function () {
            timeout = null;
            adapter.sendTo(msg.from, msg.command, {error: 'connect timeout'}, msg.callback);
        }, 5000);

        var lClient = influx({
            host:     msg.message.config.host,
            port:     msg.message.config.port,
            protocol: msg.message.config.protocol,  // optional, default 'http'
            username: msg.message.config.user,
            password: msg.message.config.password,
            database: msg.message.config.dbname || utils.appName
        });

        lClient.getDatabaseNames(function (err /* , arrayDatabaseNames*/ ) {
            if (timeout) {
                clearTimeout(timeout);
                timeout = null;
                return adapter.sendTo(msg.from, msg.command, {error: err ? err.toString() : null}, msg.callback);
            }
        });
    } catch (ex) {
        if (timeout) {
            clearTimeout(timeout);
            timeout = null;
        }
        if (ex.toString() === 'TypeError: undefined is not a function') {
            return adapter.sendTo(msg.from, msg.command, {error: 'Node.js DB driver could not be installed.'}, msg.callback);
        } else {
            return adapter.sendTo(msg.from, msg.command, {error: ex.toString()}, msg.callback);
        }
    }
}

function destroyDB(msg) {
    if (!client) {
        return adapter.sendTo(msg.from, msg.command, {error: 'Not connected'}, msg.callback);
    }
    try {
        client.dropDatabase(adapter.config.dbname, function (err) {
            if (err) {
                adapter.log.error(err);
                adapter.sendTo(msg.from, msg.command, {error: err.toString()}, msg.callback);
            } else {
                adapter.sendTo(msg.from, msg.command, {error: null}, msg.callback);
                // restart adapter
                setTimeout(function () {
                    adapter.getForeignObject('system.adapter.' + adapter.namespace, function (err, obj) {
                        if (!err) {
                            adapter.setForeignObject(obj._id, obj);
                        } else {
                            adapter.log.error('Cannot read object "system.adapter.' + adapter.namespace + '": ' + err);
                            adapter.stop();
                        }
                    });
                }, 2000);
            }
        });
    } catch (ex) {
        return adapter.sendTo(msg.from, msg.command, {error: ex.toString()}, msg.callback);
    }
}

function processMessage(msg) {
    if (msg.command === 'getHistory') {
        getHistory(msg);
    }
    else if (msg.command === 'test') {
        testConnection(msg);
    }
    else if (msg.command === 'destroy') {
        destroyDB(msg);
    }
    /* else  if (msg.command === 'generateDemo') {
        generateDemo(msg);
    } */
    else if (msg.command === 'query') {
        query(msg);
    }
    else if (msg.command === 'getConflictingPoints') {
        getConflictingPoints(msg);
    }
    else if (msg.command === 'resetConflictingPoints') {
        resetConflictingPoints(msg);
    }
    else if (msg.command === 'storeState') {
        storeState(msg);
    }
    else if (msg.command === 'enableHistory') {
        enableHistory(msg);
    }
    else if (msg.command === 'disableHistory') {
        disableHistory(msg);
    }
    else if (msg.command === 'getEnabledDPs') {
        getEnabledDPs(msg);
    }
    else if (msg.command === 'stopInstance') {
        finish(function () {
            if (msg.callback) {
                adapter.sendTo(msg.from, msg.command, 'stopped', msg.callback);
                setTimeout(function () {
                    process.exit(0);
                }, 200);
            }
        });
    }
}

function getConflictingPoints(msg) {
    return adapter.sendTo(msg.from, msg.command, {conflictingPoints: conflictingPoints}, msg.callback);
}

function resetConflictingPoints(msg) {
    var resultMsg = {reset: true, conflictingPoints: conflictingPoints};
    conflictingPoints = {};
    return adapter.sendTo(msg.from, msg.command, resultMsg, msg.callback);
}

function fixSelector(callback) {
    // fix _design/custom object
    adapter.getForeignObject('_design/custom', function (err, obj) {
        if (!obj || obj.views.state.map.indexOf('common.history') === -1 || obj.views.state.map.indexOf('common.custom') === -1) {
            obj = {
                _id: '_design/custom',
                language: 'javascript',
                views: {
                    state: {
                        map: 'function(doc) { if (doc.type===\'state\' && (doc.common.custom || doc.common.history)) emit(doc._id, doc.common.custom || doc.common.history) }'
                    }
                }
            };
            adapter.setForeignObject('_design/custom', obj, function (err) {
                if (callback) callback(err);
            });
        } else {
            if (callback) callback(err);
        }
    });
}

function main() {
    adapter.config.port = parseInt(adapter.config.port, 10) || 0;

    setConnected(false);

    if (adapter.config.round !== null && adapter.config.round !== undefined) {
        adapter.config.round = Math.pow(10, parseInt(adapter.config.round, 10));
    } else {
        adapter.config.round = null;
    }
    if (adapter.config.changesRelogInterval !== null && adapter.config.changesRelogInterval !== undefined) {
        adapter.config.changesRelogInterval = parseInt(adapter.config.changesRelogInterval, 10);
    } else {
        adapter.config.changesRelogInterval = 0;
    }

    adapter.config.seriesBufferFlushInterval = parseInt(adapter.config.seriesBufferFlushInterval, 10) || 600;

    if (adapter.config.changesMinDelta !== null && adapter.config.changesMinDelta !== undefined) {
        adapter.config.changesMinDelta = parseFloat(adapter.config.changesMinDelta.toString().replace(/,/g, '.'));
    } else {
        adapter.config.changesMinDelta = 0;
    }

    // analyse if by the last stop the values were cached into file
    try {
        if (fs.statSync(cacheFile).isFile()) {
            var fileContent = fs.readFileSync(cacheFile);
            var tempData = JSON.parse(fileContent, function (key, value) {
                if (key === 'time') {
                    return new Date(value);
                }
                return value;
            });
            if (tempData.seriesBufferCounter) seriesBufferCounter = tempData.seriesBufferCounter;
            if (tempData.seriesBuffer)        seriesBuffer        = tempData.seriesBuffer;
            if (tempData.conflictingPoints)   conflictingPoints   = tempData.conflictingPoints;
            adapter.log.info('Buffer initialized with data for ' + seriesBufferCounter + ' points and ' + Object.keys(conflictingPoints).length + ' conflicts from last exit');
            fs.unlinkSync(cacheFile);
        }
    }
    catch (err) {
        adapter.log.info('No stored data from last exit found');
    }

    fixSelector(function () {
        // read all custom settings
        adapter.objects.getObjectView('custom', 'state', {}, function (err, doc) {
            if (err) adapter.log.error('main/getObjectView: ' + err);
            var count = 0;
            if (doc && doc.rows) {
                for (var i = 0, l = doc.rows.length; i < l; i++) {
                    if (doc.rows[i].value) {
                        var id = doc.rows[i].id;
                        influxDPs[id] = doc.rows[i].value;

                        if (!influxDPs[id][adapter.namespace]) {
                            delete influxDPs[id];
                        } else {
                            count++;
                            adapter.log.info('enabled logging of ' + id + ', ' + Object.keys(influxDPs).length + ' points now activated');
                            if (influxDPs[id][adapter.namespace].retention !== undefined && influxDPs[id][adapter.namespace].retention !== null && influxDPs[id][adapter.namespace].retention !== '') {
                                influxDPs[id][adapter.namespace].retention = parseInt(influxDPs[id][adapter.namespace].retention || adapter.config.retention, 10) || 0;
                            } else {
                                influxDPs[id][adapter.namespace].retention = adapter.config.retention;
                            }

                            if (influxDPs[id][adapter.namespace].debounce !== undefined && influxDPs[id][adapter.namespace].debounce !== null && influxDPs[id][adapter.namespace].debounce !== '') {
                                influxDPs[id][adapter.namespace].debounce = parseInt(influxDPs[id][adapter.namespace].debounce, 10) || 0;
                            } else {
                                influxDPs[id][adapter.namespace].debounce = adapter.config.debounce;
                            }

                            influxDPs[id][adapter.namespace].changesOnly   = influxDPs[id][adapter.namespace].changesOnly   === 'true' || influxDPs[id][adapter.namespace].changesOnly   === true;

                            if (influxDPs[id][adapter.namespace].changesRelogInterval !== undefined && influxDPs[id][adapter.namespace].changesRelogInterval !== null && influxDPs[id][adapter.namespace].changesRelogInterval !== '') {
                                influxDPs[id][adapter.namespace].changesRelogInterval = parseInt(influxDPs[id][adapter.namespace].changesRelogInterval, 10) || 0;
                            } else {
                                influxDPs[id][adapter.namespace].changesRelogInterval = adapter.config.changesRelogInterval;
                            }
                            if (influxDPs[id][adapter.namespace].changesMinDelta !== undefined && influxDPs[id][adapter.namespace].changesMinDelta !== null && influxDPs[id][adapter.namespace].changesMinDelta !== '') {
                                influxDPs[id][adapter.namespace].changesMinDelta = parseFloat(influxDPs[id][adapter.namespace].changesMinDelta) || 0;
                            } else {
                                influxDPs[id][adapter.namespace].changesMinDelta = adapter.config.changesMinDelta;
                            }
                            if (!influxDPs[id][adapter.namespace].storageType) influxDPs[id][adapter.namespace].storageType = false;

                            // add one day if retention is too small
                            if (influxDPs[id][adapter.namespace].retention && influxDPs[id][adapter.namespace].retention <= 604800) {
                                influxDPs[id][adapter.namespace].retention += 86400;
                            }
                            writeInitialValue(id);
                        }
                    }
                }
            }

            if (count < 20) {
                for (var _id in influxDPs) {
                    adapter.subscribeForeignStates(_id);
                }
            } else {
                subscribeAll = true;
                adapter.subscribeForeignStates('*');
            }
        });
    });

    adapter.subscribeForeignObjects('*');

    // store all buffered data every x seconds to not lost the data
    seriesBufferChecker = setInterval(function () {
        seriesBufferFlushPlanned = true;
        storeBufferedSeries();
    }, adapter.config.seriesBufferFlushInterval * 1000);

    connect();
}

function writeInitialValue(id) {
    adapter.getForeignState(id, function (err, state) {
        if (state) {
            state.from = 'system.adapter.' + adapter.namespace;
            influxDPs[id].state = state;
            tasksStart.push(id);
            if (tasksStart.length === 1 && connected) {
                processStartValues();
            }
        }
    });
}

function pushHistory(id, state, timerRelog) {
    if (timerRelog === undefined) timerRelog = false;
    // Push into InfluxDB
    if (influxDPs[id]) {
        var settings = influxDPs[id][adapter.namespace];

        if (!settings || !state) return;

        if (typeof state.val === 'string' && settings.storageType !== 'String') {
            var f = parseFloat(state.val);
            if (f == state.val) {
                state.val = f;
            }
        }
        if (influxDPs[id].state && settings.changesOnly && !timerRelog) {
            if (settings.changesRelogInterval === 0) {
                if (state.ts !== state.lc) {
                    influxDPs[id].skipped = state; // remember new timestamp
                    adapter.log.debug('value not changed ' + id + ', last-value=' + influxDPs[id].state.val + ', new-value=' + state.val + ', ts=' + state.ts);
                    return;
                }
            } else if (influxDPs[id].lastLogTime) {
                if ((state.ts !== state.lc) && (Math.abs(influxDPs[id].lastLogTime - state.ts) < settings.changesRelogInterval * 1000)) {
                    adapter.log.debug('value not changed ' + id + ', last-value=' + influxDPs[id].state.val + ', new-value=' + state.val + ', ts=' + state.ts);
                    influxDPs[id].skipped = state; // remember new timestamp
                    return;
                }
                if (state.ts !== state.lc) {
                    adapter.log.debug('value-changed-relog ' + id + ', value=' + state.val + ', lastLogTime=' + influxDPs[id].lastLogTime + ', ts=' + state.ts);
                }
            }
            if ((settings.changesMinDelta !== 0) && (typeof state.val === 'number') && (Math.abs(influxDPs[id].state.val - state.val) < settings.changesMinDelta)) {
                adapter.log.debug('Min-Delta not reached ' + id + ', last-value=' + influxDPs[id].state.val + ', new-value=' + state.val + ', ts=' + state.ts);
                influxDPs[id].skipped = state; // remember new timestamp
                return;
            }
            else if (typeof state.val === 'number') {
                adapter.log.debug('Min-Delta reached ' + id + ', last-value=' + influxDPs[id].state.val + ', new-value=' + state.val + ', ts=' + state.ts);
            }
            else {
                adapter.log.debug('Min-Delta ignored because no number ' + id + ', last-value=' + influxDPs[id].state.val + ', new-value=' + state.val + ', ts=' + state.ts);
            }
        }

        if (influxDPs[id].relogTimeout) {
            clearTimeout(influxDPs[id].relogTimeout);
            influxDPs[id].relogTimeout = null;
        }
        if (settings.changesRelogInterval > 0) {
            influxDPs[id].relogTimeout = setTimeout(reLogHelper, settings.changesRelogInterval * 1000, id);
        }

        var ignoreDebonce = false;
        if (timerRelog) {
            state.ts = new Date().getTime();
            adapter.log.debug('timed-relog ' + id + ', value=' + state.val + ', lastLogTime=' + influxDPs[id].lastLogTime + ', ts=' + state.ts);
            ignoreDebonce = true;
        } else {
            if (settings.changesOnly && influxDPs[id].skipped) {
                influxDPs[id].state = influxDPs[id].skipped;
                pushHelper(id);
            }
            if (influxDPs[id].state && ((influxDPs[id].state.val === null && state.val !== null) || (influxDPs[id].state.val !== null && state.val === null))) {
                ignoreDebonce = true;
            } else if (!influxDPs[id].state && state.val === null) {
                ignoreDebonce = true;
            }

            // only store state if really changed
            influxDPs[id].state = state;
        }
        influxDPs[id].lastLogTime = state.ts;
        influxDPs[id].skipped = null;

        if (settings.debounce && !ignoreDebonce) {
            // Discard changes in de-bounce time to store last stable value
            if (influxDPs[id].timeout) clearTimeout(influxDPs[id].timeout);
            influxDPs[id].timeout = setTimeout(pushHelper, settings.debounce, id);
        } else {
            pushHelper(id);
        }
    }
}

function reLogHelper(_id) {
    if (!influxDPs[_id]) {
        adapter.log.info('non-existing id ' + _id);
        return;
    }
    influxDPs[_id].relogTimeout = null;
    if (influxDPs[_id].skipped) {
        influxDPs[_id].state = influxDPs[_id].skipped;
        influxDPs[_id].state.from = 'system.adapter.' + adapter.namespace;
        influxDPs[_id].skipped = null;
        pushHistory(_id, influxDPs[_id].state, true);
    }
    else {
        adapter.getForeignState(_id, function (err, state) {
            if (err) {
                adapter.log.info('init timed Relog: can not get State for ' + _id + ' : ' + err);
            }
            else if (!state) {
                adapter.log.info('init timed Relog: disable relog because state not set so far for ' + _id + ': ' + JSON.stringify(state));
            }
            else {
                adapter.log.debug('init timed Relog: getState ' + _id + ':  Value=' + state.val + ', ack=' + state.ack + ', ts=' + state.ts  + ', lc=' + state.lc);
                influxDPs[_id].state = state;
                pushHistory(_id, influxDPs[_id].state, true);
            }
        });
    }
}

function pushHelper(_id, cb) {
    if (!influxDPs[_id] || !influxDPs[_id].state || !influxDPs[_id][adapter.namespace]) {
        if (cb) cb('ID ' + _id + ' not activated for logging');
        return;
    }
    var _settings = influxDPs[_id][adapter.namespace];
    // if it was not deleted in this time
    influxDPs[_id].timeout = null;

    if (influxDPs[_id].state.val === null) return; // InfluxDB can not handle null values

    if (typeof influxDPs[_id].state.val === 'object') influxDPs[_id].state.val = JSON.stringify(influxDPs[_id].state.val);

    adapter.log.debug('Datatype ' + _id + ': Currently: ' + typeof influxDPs[_id].state.val + ', StorageType: ' + _settings.storageType);
    if (typeof influxDPs[_id].state.val === 'string' && _settings.storageType !== 'String') {
        adapter.log.debug('Do Automatic Datatype conversion for ' + _id);
        var f = parseFloat(influxDPs[_id].state.val);
        if (f == influxDPs[_id].state.val) {
            influxDPs[_id].state.val = f;
        } else if (influxDPs[_id].state.val === 'true') {
            influxDPs[_id].state.val = true;
        } else if (influxDPs[_id].state.val === 'false') {
            influxDPs[_id].state.val = false;
        }
    }
    if (_settings.storageType === 'String' && typeof influxDPs[_id].state.val !== 'string') {
        influxDPs[_id].state.val = influxDPs[_id].state.val.toString();
    }
    else if (_settings.storageType === 'Number' && typeof influxDPs[_id].state.val !== 'number') {
        if (typeof influxDPs[_id].state.val === 'boolean') {
            influxDPs[_id].state.val = influxDPs[_id].state.val?1:0;
        }
        else {
            adapter.log.info('Do not store value "' + influxDPs[_id].state.val + '" for ' + _id + ' because no number');
            return;
        }
    }
    else if (_settings.storageType === 'Boolean' && typeof influxDPs[_id].state.val !== 'boolean') {
        influxDPs[_id].state.val = !!influxDPs[_id].state.val;
    }
    pushValueIntoDB(_id, influxDPs[_id].state, cb);
}

function pushValueIntoDB(id, state, cb) {
    if (!client) {
        adapter.log.warn('No connection to DB');
        if (cb) cb('No connection to DB');
        return;
    }

    if ((state.val === null) || (state.val === undefined)) {
        if (cb) cb('InfluxDB can not handle null/non-existing values');
        return;
    } // InfluxDB can not handle null/non-existing values
    if ((typeof state.val === 'number') && (isNaN(state.val))) {
        if (cb) cb('InfluxDB can not handle null/non-existing values');
        return;
    }

    state.ts = parseInt(state.ts, 10);

    // if less 2000.01.01 00:00:00
    if (state.ts < 946681200000) state.ts *= 1000;

    if (typeof state.val === 'object') {
        state.val = JSON.stringify(state.val);
    }

    /*
    if (state.val === 'true') {
        state.val = true;
    } else if (state.val === 'false') {
        state.val = false;
    } else {
        // try to convert to float
        var f = parseFloat(state.val);
        if (f == state.val) state.val = f;
    }*/

    //adapter.log.debug('write value ' + state.val + ' for ' + id);
    var influxFields = {
        value: state.val,
        time:  new Date(state.ts),
        from:  state.from,
        q:     state.q,
        ack:   !!state.ack
    };

    if ((conflictingPoints[id] || (adapter.config.seriesBufferMax === 0)) && (client.request) && (client.request.getHostsAvailable().length > 0)) {
        if (adapter.config.seriesBufferMax !== 0) {
            adapter.log.debug('Direct writePoint("' + id + ' - ' + influxFields.value + ' / ' + influxFields.time + ')');
        }
        writeOnePointForID(id, influxFields, true, cb);
    } else {
        addPointToSeriesBuffer(id, influxFields, cb);
    }
}

function addPointToSeriesBuffer(id, stateObj, cb) {
    if (!seriesBuffer[id]) {
        seriesBuffer[id] = [];
    }
    seriesBuffer[id].push([stateObj]);
    seriesBufferCounter++;
    if ((seriesBufferCounter > adapter.config.seriesBufferMax) && (client.request) && (client.request.getHostsAvailable().length > 0) && (!seriesBufferFlushPlanned)) {
        // flush out
        seriesBufferFlushPlanned = true;
        setTimeout(storeBufferedSeries, 0, cb);
    } else {
        if (cb) cb();
    }
}

function storeBufferedSeries(cb) {
    if (Object.keys(seriesBuffer).length === 0) {
        if (cb) cb();
        return;
    }

    if (client.request.getHostsAvailable().length === 0) {
        setConnected(false);
        adapter.log.info('No hosts available currently, try later');
        seriesBufferFlushPlanned = false;
        if (cb) cb('No hosts available currently, try later');
        return;
    }
    if (seriesBufferChecker) {
        clearInterval(seriesBufferChecker);
    }

    adapter.log.info('Store ' + seriesBufferCounter + ' buffered influxDB history points');

    if (seriesBufferCounter > 15000) {
        // if we have too many datapoints in buffer; we better writer them per id
        adapter.log.info('Too many datapoints (' + seriesBufferCounter + ') to write at once; write per ID');
        writeAllSeriesPerID(seriesBuffer);
        if (cb) cb();
    } else {
        writeAllSeriesAtOnce(seriesBuffer, cb);
    }
    seriesBuffer = {};
    seriesBufferCounter = 0;
    seriesBufferFlushPlanned = false;
    seriesBufferChecker = setInterval(storeBufferedSeries, adapter.config.seriesBufferFlushInterval * 1000);
}

function writeAllSeriesAtOnce(series, cb) {
    client.writeSeries(series, function (err /* , result */) {
        if (err) {
            adapter.log.warn('Error on writeSeries: ' + err);
            if (client.request.getHostsAvailable().length === 0) {
                setConnected(false);
                adapter.log.info('Host not available, move all points back in the Buffer');
                // error caused InfluxDB client to remove the host from available for now
                for (var id in series) {
                    if (!series.hasOwnProperty(id)) continue;
                    for (var i = 0; i < series[id].length; i++) {
                        if (!seriesBuffer[id]) seriesBuffer[id] = [];
                        seriesBuffer[id].push(series[id][i]);
                        seriesBufferCounter++;
                    }
                }
            }
            else if (err.message && (typeof err.message === 'string') && (err.message.indexOf('partial write') !== -1)) {
                adapter.log.warn('All possible datapoints were written, others can not really be corrected');
            }
            else {
                adapter.log.info('Try to write ' + Object.keys(series).length + ' Points separate to find the conflicting id');
                // fallback and send data per id to find out problematic id!
                writeAllSeriesPerID(series);
            }
        } else {
            setConnected(true);
        }
        if (cb) cb();
    });
}

function writeAllSeriesPerID(series) {
    for (var id in series) {
        if (series.hasOwnProperty(id)) {
            writeSeriesPerID(id, series[id]);
        }
    }
}

function writeSeriesPerID(seriesId, points) {
    adapter.log.debug('writePoints ' + points.length + ' for ' + seriesId + ' at once');

    if (points.length > 15000) {
        adapter.log.info('Too many datapoints (' + points.length + ') for "' + seriesId + '" to write at once; write each single one');
        writeSeriesPerID(seriesId, points.slice(0, 15000));
        writeSeriesPerID(seriesId, points.slice(15000));
    } else {
        client.writePoints(seriesId, points, function(err) {
            if (err) {
                adapter.log.warn('Error on writePoints for ' + seriesId + ': ' + err);
                if ((client.request.getHostsAvailable().length === 0) || (err.message && err.message === 'timeout')) {
                    setConnected(false);
                    adapter.log.info('Host not available, move all points back in the Buffer');
                    // error caused InfluxDB client to remove the host from available for now
                    if (!seriesBuffer[seriesId]) seriesBuffer[seriesId] = [];
                    for (var i = 0; i < points.length; i++) {
                        seriesBuffer[seriesId].push(points[i]);
                        seriesBufferCounter++;
                    }
                } else {
                    adapter.log.warn('Try to write ' + points.length + ' Points separate to find the conflicting one');
                    // we found the conflicting id
                    writePointsForID(seriesId, points);
                }
            }
        });
    }
}

function writePointsForID(seriesId, points) {
    adapter.log.debug('writePoint ' + points.length + ' for ' + seriesId + ' separate');

    for (var i = 0; i < points.length; i++) {
        writeOnePointForID(seriesId, points[i][0]);
    }
}

function writeOnePointForID(pointId, point, directWrite, cb) {
    if (directWrite === undefined) {
        directWrite = false;
    }
    client.writePoint(pointId, point, null, function (err /* , result */) {
        if (err) {
            adapter.log.warn('Error on writePoint("' + JSON.stringify(point) + '): ' + err + ' / ' + JSON.stringify(err.message));
            if ((client.request.getHostsAvailable().length === 0) || (err.message && err.message === 'timeout')) {
                setConnected(false);
                addPointToSeriesBuffer(pointId, point);
            } else if (err.message && (typeof err.message === 'string') && (err.message.indexOf('field type conflict') !== -1)) {
                // retry write after type correction for some easy cases
                var retry = false;
                if (!influxDPs[pointId][adapter.namespace].storageType) {
                    var convertDirection = '';
                    if (err.message.indexOf('is type bool, already exists as type float') !== -1 ||
                        err.message.indexOf('is type boolean, already exists as type float') !== -1) {
                        convertDirection = 'bool -> float';
                        if (point.value === true) {
                            point.value = 1;
                            retry = true;
                        }
                        else if (point.value === false) {
                            point.value = 0;
                            retry = true;
                        }
                        influxDPs[pointId][adapter.namespace].storageType = 'Number';
                    }
                    else if ((err.message.indexOf('is type float, already exists as type bool') !== -1) || (err.message.indexOf('is type float64, already exists as type bool') !== -1)) {
                        convertDirection = 'float -> bool';
                        if (point.value === 1) {
                            point.value = true;
                            retry = true;
                        }
                        else if (point.value === 0) {
                            point.value = false;
                            retry = true;
                        }
                        influxDPs[pointId][adapter.namespace].storageType = 'Boolean';
                    }
                    else if (err.message.indexOf(', already exists as type string') !== -1) {
                        point.value = point.value.toString();
                        retry = true;
                        influxDPs[pointId][adapter.namespace].storageType = 'String';
                    }
                    if (retry) {
                        adapter.log.info('Try to convert ' + convertDirection + ' and re-write for ' + pointId + ' and set storageType to ' + influxDPs[pointId][adapter.namespace].storageType);
                        writeOnePointForID(pointId, point, true, cb);
                        var obj = {};
                        obj.common = {};
                        obj.common.custom = {};
                        obj.common.custom[adapter.namespace] = {};
                        obj.common.custom[adapter.namespace].storageType = influxDPs[pointId][adapter.namespace].storageType;
                        adapter.extendForeignObject(pointId, obj, function (err) {
                            if (err) {
                                adapter.log.error('error updating history config for ' + pointId + ' to pin datatype: ' + err);
                            } else {
                                adapter.log.info('changed history configuration to pin detected datatype for ' + pointId);
                            }
                        });
                    }
                }
                if (!directWrite || !retry) {
                    // remember this as a pot. conflicting point and write synchronous
                    conflictingPoints[pointId]=1;
                    adapter.log.warn('Add ' + pointId + ' to conflicting Points (' + Object.keys(conflictingPoints).length + ' now)');
                }
            } else {
                if (! errorPoints[pointId]) {
                    errorPoints[pointId] = 1;
                } else {
                    errorPoints[pointId]++;
                }
                if (errorPoints[pointId] < 10) {
                    // re-add that point to buffer to try write again
                    adapter.log.info('Add point that had error for ' + pointId + ' to buffer again, error-count=' + errorPoints[pointId]);
                    addPointToSeriesBuffer(pointId, point);
                } else {
                    errorPoints[pointId] = 0;
                }
            }
        } else {
            setConnected(true);
        }
        if (cb) cb();
    });
}

function finish(callback) {
    if (finished) {
        if (callback) callback();
        return;
    }
    finished = true;
    if (seriesBufferChecker) {
        clearInterval(seriesBufferChecker);
        seriesBufferChecker = null;
    }
    var count = 0;
    var now = new Date().getTime();
    for (var id in influxDPs) {
        if (!influxDPs.hasOwnProperty(id)) continue;

        if (influxDPs[id].relogTimeout) {
            clearTimeout(influxDPs[id].relogTimeout);
            influxDPs[id].relogTimeout = null;
        }
        if (influxDPs[id].timeout) {
            clearTimeout(influxDPs[id].timeout);
            influxDPs[id].timeout = null;
        }

        var tmpState;
        if (Object.assign) {
            tmpState = Object.assign({}, influxDPs[id].state);
        }
        else {
            tmpState = JSON.parse(JSON.stringify(influxDPs[id].state));
        }
        var state = influxDPs[id].state ? tmpState : null;

        if (influxDPs[id].skipped) {
            count++;
            influxDPs[id].state = influxDPs[id].skipped;
            pushHelper(id, function () {
                if (!--count) {
                    if (callback) {
                        setTimeout(callback, 500);
                    } else {
                        setTimeout(function () {process.exit();}, 500);
                    }
                }
            });
            influxDPs[id].skipped = null;
        }
    }

    // write buffered values into cache file to process it by next start
    var fileData = {};
    if (seriesBufferCounter) {
        fileData.seriesBufferCounter = seriesBufferCounter;
        fileData.seriesBuffer        = seriesBuffer;
        fileData.conflictingPoints   = conflictingPoints;
        fs.writeFileSync(cacheFile, JSON.stringify(fileData));
        adapter.log.warn('Store data for ' + fileData.seriesBufferCounter + ' points and ' + Object.keys(fileData.conflictingPoints).length + ' conflicts');
    }
    seriesBufferCounter = null;

    if (!count) {
        if (callback) {
            callback();
        } else {
            process.exit();
        }
    }
}

function getHistory(msg) {

    var options = {
        id:         msg.message.id === '*' ? null : msg.message.id,
        start:      msg.message.options.start,
        end:        msg.message.options.end || ((new Date()).getTime() + 5000000),
        step:       parseInt(msg.message.options.step,  10) || null,
        count:      parseInt(msg.message.options.count, 10) || 500,
        aggregate:  msg.message.options.aggregate || 'average', // One of: max, min, average, total
        limit:      parseInt(msg.message.options.limit || adapter.config.limit || 2000),
        addId:      msg.message.options.addId || false,
        ignoreNull: true,
        sessionId:  msg.message.options.sessionId
    };
    var query = 'SELECT';
    if (options.step) {
        switch (options.aggregate) {
            case 'average':
                query += ' mean(value) as val';
                break;

            case 'max':
                query += ' max(value) as val';
                break;

            case 'min':
                query += ' min(value) as val';
                break;

            case 'total':
                query += ' sum(value) as val';
                break;

            case 'count':
                query += ' count(value) as val';
                break;

            case 'none':
            case 'onchange':
            case 'minmax':
                query += ' value';
                break;

            default:
                query += ' mean(value) as val';
                break;
        }

    } else {
        query += ' *';
    }

    query += ' from "' + msg.message.id + '"';

    if (!influxDPs[options.id]) {
        adapter.sendTo(msg.from, msg.command, {
            result: [],
            step:   0,
            error:  'No connection'
        }, msg.callback);
        return;
    }

    if (options.start > options.end) {
        var _end = options.end;
        options.end   = options.start;
        options.start = _end;
    }
    // if less 2000.01.01 00:00:00
    if (options.end   && options.end   < 946681200000) options.end   *= 1000;
    if (options.start && options.start < 946681200000) options.start *= 1000;

    if (!options.start && !options.count) {
        options.start = options.end - 86400000; // - 1 day
    }

    // query one timegroup-value more then requested originally at start and end
    // to make sure to have no 0 values because of the way InfluxDB doies group by time
    if (options.aggregate !== 'onchange' && options.aggregate !== 'none' && options.aggregate !== 'minmax') {
        if (!options.step) {
            // calculate "step" based on difference between start and end using count
            options.step = parseInt((options.end - options.start) / options.count, 10);
        }
        if (options.start) options.start -= options.step;
        options.end += options.step;
        options.limit += 2;
    }

    query += " WHERE ";
    if (options.start) query += " time > '" + new Date(options.start).toISOString() + "' AND ";
    query += " time < '" + new Date(options.end).toISOString() + "'";

    if (!options.start && (options.count || options.limit)) {
        query += " ORDER BY time DESC";
    }

    if (options.aggregate !== 'onchange' && options.aggregate !== 'none' && options.aggregate !== 'minmax') {
        query += ' GROUP BY time(' + options.step + 'ms) fill(previous) LIMIT ' + options.limit;
    } else if (options.aggregate !== 'minmax') {
        query += ' LIMIT ' + options.count;
    }

    // select one datapoint more then wanted
    if (options.aggregate === 'minmax' || options.aggregate === 'onchange' || options.aggregate === 'none') {
        var add_query = "";
        if (options.start) {
            add_query = 'SELECT value from "' + msg.message.id + '"' + " WHERE time <= '" + new Date(options.start).toISOString() + "' ORDER BY time DESC LIMIT 1;";
            query = add_query + query;
        }
        add_query = ';SELECT value from "' + msg.message.id + '"' + " WHERE time >= '" + new Date(options.end).toISOString() + "' LIMIT 1";
        query = query + add_query;
    }

    adapter.log.debug(query);

    // if specific id requested
    client.query(query, function (err, rows) {
        if (err) {
            if (client.request.getHostsAvailable().length === 0) {
                setConnected(false);
            }
            adapter.log.error('getHistory: ' + err);
        } else {
            setConnected(true);
        }

        var result = [];
        if (rows && rows.length) {
            for (var qr = 0; qr < rows.length; qr++) {
                for (var rr = 0; rr < rows[qr].length; rr++) {
                    if ((rows[qr][rr].val === undefined) && (rows[qr][rr].value !== undefined)) {
                        rows[qr][rr].val = rows[qr][rr].value;
                        delete rows[qr][rr].value;
                    }
                    rows[qr][rr].ts  = new Date(rows[qr][rr].time).getTime();
                    delete rows[qr][rr].time;
                    if (rows[qr][rr].val !== null) {
                        var f = parseFloat(rows[qr][rr].val);
                        if (f == rows[qr][rr].val) {
                            rows[qr][rr].val = f;
                            if (adapter.config.round) {
                                rows[qr][rr].val = Math.round(rows[qr][rr].val * adapter.config.round) / adapter.config.round;
                            }
                        }
                    }
                    if (options.addId) rows[qr][rr].id = msg.message.id;
                    result.push(rows[qr][rr]);
                }
            }
        }

        if ((result.length > 0) && (options.aggregate === 'minmax')) {
            Aggregate.initAggregate(options);
            Aggregate.aggregation(options, result);
            Aggregate.finishAggregation(options);
            result = options.result;
        }

        adapter.sendTo(msg.from, msg.command, {
            result:     result,
            error:      err,
            sessionId:  options.sessionId
        }, msg.callback);
    });
}

/*
function generateDemo(msg) {

    var id    = adapter.name +'.' + adapter.instance + '.Demo.' + (msg.message.id || 'Demo_Data');
    var start = new Date(msg.message.start).getTime();
    var end   = new Date(msg.message.end).getTime();
    var value = 1;
    var sin   = 0.1;
    var up    = true;
    var curve = msg.message.curve;
    var step  = (msg.message.step || 60) * 1000;

    if (end < start) {
        var tmp = end;
        end     = start;
        start   = tmp;
    }

    end = new Date(end).setHours(24);

    function generate() {

        if (curve === 'sin') {
            if (sin === 6.2) {
                sin = 0;
            } else {
                sin = Math.round((sin + 0.1) * 10) / 10;
            }
            value = Math.round(Math.sin(sin) * 10000) / 100;
        } else if (curve === 'dec') {
            value++;
        } else if (curve === 'inc') {
            value--;
        } else {
            if (up === true) {
                value++;
            } else {
                value--;
            }
        }
        start += step;

        pushValueIntoDB(id, {
            ts:     new Date(start).getTime(),
            val:    value,
            q:      0,
            ack:    true
        });


        if (start <= end) {
            setTimeout(function () {
                generate();
            }, 15);
        } else {
            adapter.sendTo(msg.from, msg.command, 'finished', msg.callback);
        }
    }

    var obj = {
        type: 'state',
        common: {
            name:    msg.message.id,
            type:    'state',
            enabled: false,
            custom:  {}
        }
    };

    obj.common.custom[adapter.namespace] = {
        enabled:        true,
        changesOnly:    false,
        debounce:       1000,
        retention:      31536000
    };

    adapter.setObject('demo.' + msg.message.id, obj);

    influxDPs[id] = {};
    influxDPs[id][adapter.namespace] = obj.common.custom[adapter.namespace];

    generate();
}
*/
function query(msg) {
    if (client) {
        var query = msg.message.query || msg.message;

        if (!query || typeof query !== 'string') {
          adapter.log.error('query missing: ' + query);
          adapter.sendTo(msg.from, msg.command, {
              result: [],
              error:  'Query missing'
          }, msg.callback);
          return;
        }

        adapter.log.debug('query: ' + query);

        client.query(query, function (err, rows) {
            if (err) {
                if (client.request.getHostsAvailable().length === 0) {
                    setConnected(false);
                }
                adapter.log.error('query: ' + err);
                adapter.sendTo(msg.from, msg.command, {
                        result: [],
                        error:  'Invalid call'
                    }, msg.callback);
                return;
            } else {
                setConnected(true);
            }

            adapter.log.debug('result: ' + JSON.stringify(rows));

            for (var r = 0, l = rows.length; r < l; r++) {
                for (var rr = 0, ll = rows[r].length; rr < ll; rr++) {
                    if (rows[r][rr].time) {
                        rows[r][rr].ts = new Date(rows[r][rr].time).getTime();
                        delete rows[r][rr].time;
                    }
                }
            }

            adapter.sendTo(msg.from, msg.command, {
                result: rows,
                ts:     new Date().getTime(),
                error:  err
            }, msg.callback);
        });
    } else {
        adapter.sendTo(msg.from, msg.command, {
            result: [],
            error:  'No connection'
        }, msg.callback);
    }
}

function storeState(msg) {
    if (!msg.message || !msg.message.id || !msg.message.state) {
        adapter.log.error('storeState called with invalid data');
        adapter.sendTo(msg.from, msg.command, {
            error:  'Invalid call'
        }, msg.callback);
        return;
    }

    if (Array.isArray(msg.message)) {
        adapter.log.debug('storeState: store ' + msg.message.length + ' states for multiple ids');
        for (var i = 0; i < msg.message.length; i++) {
            pushValueIntoDB(msg.message[i].id, msg.message[i].state);
        }
    } else if (Array.isArray(msg.message.state)) {
        adapter.log.debug('storeState: store ' + msg.message.state.length + ' states for ' + msg.message.id);
        for (var j = 0; j < msg.message.state.length; j++) {
            pushValueIntoDB(msg.message.id, msg.message.state[j]);
        }
    } else {
        adapter.log.debug('storeState: store 1 state for ' + msg.message.id);
        pushValueIntoDB(msg.message.id, msg.message.state);
    }

    adapter.sendTo(msg.from, msg.command, {
        success:                  true,
        connected:                connected,
        seriesBufferCounter:      seriesBufferCounter,
        seriesBufferFlushPlanned: seriesBufferFlushPlanned
    }, msg.callback);
}

function enableHistory(msg) {
    if (!msg.message || !msg.message.id) {
        adapter.log.error('enableHistory called with invalid data');
        adapter.sendTo(msg.from, msg.command, {
            error:  'Invalid call'
        }, msg.callback);
        return;
    }
    var obj = {};
    obj.common = {};
    obj.common.custom = {};
    if (msg.message.options) {
        obj.common.custom[adapter.namespace] = msg.message.options;
    }
    else {
        obj.common.custom[adapter.namespace] = {};
    }
    obj.common.custom[adapter.namespace].enabled = true;
    adapter.extendForeignObject(msg.message.id, obj, function (err) {
        if (err) {
            adapter.log.error('enableHistory: ' + err);
            adapter.sendTo(msg.from, msg.command, {
                error:  err
            }, msg.callback);
        } else {
            adapter.log.info(JSON.stringify(obj));
            adapter.sendTo(msg.from, msg.command, {
                success:                  true
            }, msg.callback);
        }
    });
}

function disableHistory(msg) {
    if (!msg.message || !msg.message.id) {
        adapter.log.error('disableHistory called with invalid data');
        adapter.sendTo(msg.from, msg.command, {
            error:  'Invalid call'
        }, msg.callback);
        return;
    }
    var obj = {};
    obj.common = {};
    obj.common.custom = {};
    obj.common.custom[adapter.namespace] = {};
    obj.common.custom[adapter.namespace].enabled = false;
    adapter.extendForeignObject(msg.message.id, obj, function (err) {
        if (err) {
            adapter.log.error('disableHistory: ' + err);
            adapter.sendTo(msg.from, msg.command, {
                error:  err
            }, msg.callback);
        } else {
            adapter.log.info(JSON.stringify(obj));
            adapter.sendTo(msg.from, msg.command, {
                success:                  true
            }, msg.callback);
        }
    });
}

function getEnabledDPs(msg) {
    var data = {};
    for (var id in influxDPs) {
        if (!influxDPs.hasOwnProperty(id)) continue;
        data[id] = influxDPs[id][adapter.namespace];
    }

    adapter.sendTo(msg.from, msg.command, data, msg.callback);
}
