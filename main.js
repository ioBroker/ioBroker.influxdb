/* jshint -W097 */// jshint strict:false
/*jslint node: true */
"use strict";

var utils  = require(__dirname + '/lib/utils'); // Get common adapter utils
var influx = require('influx');

var subscribeAll = false;
var influxDPs    = {};
var client;

var adapter = utils.adapter('influxdb');

adapter.on('objectChange', function (id, obj) {
    if (obj && obj.common && obj.common.history && obj.common.history[adapter.namespace]) {

        if (!influxDPs[id] && !subscribeAll) {
            // unsubscribe
            for (var id in influxDPs) {
                adapter.unsubscribeForeignStates(id);
            }
            subscribeAll = true;
            adapter.subscribeForeignStates('*');
        }
        influxDPs[id] = obj.common.history;
        adapter.log.info('enabled logging of ' + id);
    } else {
        if (influxDPs[id]) {
            adapter.log.info('disabled logging of ' + id);
            delete influxDPs[id];
        }
    }
});

adapter.on('stateChange', function (id, state) {
    pushHistory(id, state);
});

adapter.on('ready', function () {
    main();
});

adapter.on('message', function (msg) {
    processMessage(msg);
});

function connect() {

    adapter.log.info('Connecting ' + adapter.config.protocol + '://' + adapter.config.host + ':' + adapter.config.port + ' ...');

    client = influx({
        host:     adapter.config.host,
        port:     adapter.config.port, // optional, default 8086
        protocol: adapter.config.protocol, // optional, default 'http'
        username: adapter.config.user,
        password: adapter.config.password,
        database: 'iobroker'
    });

    client.createDatabase('iobroker', function (err) {
        if (err && err.message != 'database iobroker exists') {
            console.log(err);
        } else {
            adapter.log.info('Connected!');
        }
    });
}

function testConnection(msg) {
    msg.message.config.port = parseInt(msg.message.config.port, 10) || 0;

    var timeout;
    try {
        timeout = setTimeout(function () {
            timeout = null;
            adapter.sendTo(msg.from, msg.command, {error: 'connect timeout'}, msg.callback);
        }, 5000);

        var lclient = influx({
            host:     msg.message.config.host,
            port:     msg.message.config.port,
            protocol: msg.message.config.protocol,  // optional, default 'http'
            username: msg.message.config.user,
            password: msg.message.config.password,
            database: 'iobroker'
        });

        lclient.getDatabaseNames(function (err, arrayDatabaseNames) {
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
        if (ex.toString() == 'TypeError: undefined is not a function') {
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
        client.deleteDatabase('iobroker', function (err) {
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
    if (msg.command == 'getHistory') {
        getHistory(msg);
    } else if (msg.command == 'test') {
        testConnection(msg);
    } else if (msg.command == 'destroy') {
        destroyDB(msg);
    }else if (msg.command == 'generateDemo') {
        generateDemo(msg)
    }
}

function main() {
    adapter.config.port = parseInt(adapter.config.port, 10) || 0;

    if (adapter.config.round !== null && adapter.config.round !== undefined) {
        adapter.config.round = Math.pow(10, parseInt(adapter.config.round, 10));
    } else {
        adapter.config.round = null;
    }

    // read all history settings
    adapter.objects.getObjectView('history', 'state', {}, function (err, doc) {
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
                        adapter.log.info('enabled logging of ' + id);
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

                        // add one day if retention is too small
                        if (influxDPs[id][adapter.namespace].retention <= 604800) {
                            influxDPs[id][adapter.namespace].retention += 86400;
                        }
                    }
                }
            }
        }
        if (count < 20) {
            for (var id in influxDPs) {
                adapter.subscribeForeignStates(id);
            }
        } else {
            subscribeAll = true;
            adapter.subscribeForeignStates('*');
        }
    });

    adapter.subscribeForeignObjects('*');

    connect();
}

function pushHistory(id, state) {
    // Push into redis
    if (influxDPs[id]) {
        var settings = influxDPs[id][adapter.namespace];

        if (!settings || !state) return;
        
        if (influxDPs[id].state && settings.changesOnly && (state.ts !== state.lc)) return;

        influxDPs[id].state = state;

        // Do not store values ofter than 1 second
        if (!influxDPs[id].timeout && settings.debounce) {
            influxDPs[id].timeout = setTimeout(pushHelper, settings.debounce, id);
        } else {
            pushHelper(id);
        }
    }
}

function pushHelper(_id) {
    if (!influxDPs[_id] || !influxDPs[_id].state) return;
    var _settings = influxDPs[_id][adapter.namespace];
    // if it was not deleted in this time
    if (_settings) {
        influxDPs[_id].timeout = null;

        if (typeof influxDPs[_id].state.val === 'string') {
            var f = parseFloat(influxDPs[_id].state.val);
            if (f.toString() == influxDPs[_id].state.val) {
                influxDPs[_id].state.val = f;
            } else if (influxDPs[_id].state.val === 'true') {
                influxDPs[_id].state.val = true;
            } else if (influxDPs[_id].state.val === 'false') {
                influxDPs[_id].state.val = false;
            }
        }
        pushValueIntoDB(_id, influxDPs[_id].state);
    }
}

function checkRetention(id) {
    if (influxDPs[id][adapter.namespace].retention) {
        var d = new Date();
        var dt = d.getTime();
        // check every 6 hours
        if (!influxDPs[id].lastCheck || dt - influxDPs[id].lastCheck >= 21600000/* 6 hours */) {
            influxDPs[id].lastCheck = dt;
            d.setSeconds(-influxDPs[id][adapter.namespace].retention);
            var time = d.toISOString();
            time = time.replace('T', ' ').replace(/\.\d\d\dZ/, '');
            client.query("DELETE FROM " + id + " WHERE time < '" + time + "'", function (err) {
                if (err) {
                    adapter.log.warn('Cannot delete from influxdb: ' + err);
                }
            });
        }
    }
}

function pushValueIntoDB(id, state) {
    if (!client) {
        adapter.log.warn('No connection to DB');
        return;
    }

    // todo change it after ms are added
    state.ts = parseInt(state.ts, 10) * 1000 + (parseInt(state.ms, 10) || 0);

    client.writePoint(id, {
        value: state.val,
        time: new Date(state.ts),
        from: state.from,
        q:    state.q
    }, {}, function (err, result) {
        if (err) adapter.log.warn(err);
    });

    checkRetention(id);
}

function sortByTs(a, b) {
    var aTs = a.ts;
    var bTs = b.ts;
    return ((aTs < bTs) ? -1 : ((aTs > bTs) ? 1 : 0));
}

function getDataFromDB(db, options, callback) {
    if (options.start) options.start *= 1000;
    if (options.end)   options.end   *= 1000;
    if (options.step)  options.step  *= 1000;

    var query = SQLFuncs.getHistory(db, options);
    adapter.log.debug(query);

    clientPool.borrow(function (err, client) {
        if (err) {
            if (callback) callback(err);
            return;
        }
        client.execute(query, function (err, rows, fields) {
            if (rows && rows.rows) rows = rows.rows;
            // because descending
            if (!err && rows && !options.start && options.count) {
                rows.sort(sortByTs);
            }

            if (rows) {
                for (var c = 0; c < rows.length; c++) {
                    // todo change it after ms are added
                    if (options.ms) rows[c].ms = rows[c].ts % 1000;
                    if (adapter.common.loglevel == 'debug') rows[c].date = new Date(parseInt(rows[c].ts, 10));
                    rows[c].ts = Math.round(rows[c].ts / 1000);

                    if (options.ack) rows[c].ack = !!rows[c].ack;
                    if (adapter.config.round) rows[c].val = Math.round(rows[c].val * adapter.config.round) / adapter.config.round;
                    if (influxDPs[options.index].type === 2) rows[c].val = !!rows[c].val;
                }
            }

            // todo change it after ms are added
            if (options.start) options.start /= 1000;
            if (options.end)   options.end   /= 1000;
            if (options.step)  options.step  /= 1000;

            clientPool.return(client);
            if (callback) callback(err, rows);
        });
    });
}

function getHistory(msg) {
    var startTime = new Date().getTime();
    var options = {
        id:         msg.message.id == '*' ? null : msg.message.id,
        start:      msg.message.options.start,
        end:        msg.message.options.end || Math.round((new Date()).getTime() / 1000) + 5000,
        step:       parseInt(msg.message.options.step) || null,
        count:      parseInt(msg.message.options.count) || 500,
        ignoreNull: msg.message.options.ignoreNull,
        aggregate:  msg.message.options.aggregate || 'average', // One of: max, min, average, total
        limit:      msg.message.options.limit || adapter.config.limit || 2000,
        from:       msg.message.options.from  || false,
        q:          msg.message.options.q     || false,
        ack:        msg.message.options.ack   || false,
        ms:         msg.message.options.ms    || false
    };

    if (!influxDPs[options.id]) {
        commons.sendResponse(adapter, msg, options, [], startTime);
        return;
    }

    if (options.start > options.end) {
        var _end = options.end;
        options.end   = options.start;
        options.start =_end;
    }

    if (!options.start && !options.count) {
        options.start = Math.round((new Date()).getTime() / 1000) - 5030; // - 1 year
    }

    if (options.id && influxDPs[options.id].index === undefined) {
        // read or create in DB
        return getId(options.id, null, function (err) {
            if (err) {
                adapter.log.warn('Cannot get index of "' + options.id + '": ' + err);
                commons.sendResponse(adapter, msg, options, [], startTime);
            } else {
                getHistory(msg);
            }
        });
    }
    var type = influxDPs[options.id].type;
    if (options.id) {
        options.index = options.id;
        options.id = influxDPs[options.id].index;
    }

    // if specific id requested
    if (options.id || options.id === 0) {
        getDataFromDB(dbNames[type], options, function (err, data) {
            commons.sendResponse(adapter, msg, options, (err ? err.toString() : null) || data, startTime);
        });
    } else {
        // if all IDs requested
        var rows = [];
        var count = 0;
        for (var db = 0; db < dbNames.length; db++) {
            count++;
            getDataFromDB(dbNames[db], options, function (err, data) {
                if (data) rows = rows.concat(data);
                if (!--count) {
                    rows.sort(sortByTs);
                    commons.sendResponse(adapter, msg, options, rows, startTime);
                }
            });
        }
    }
}

function generateDemo(msg) {

    var id = adapter.name +'.' + adapter.instance + '.Demo.' + (msg.message.id || 'Demo_Data');
    var start = new Date(msg.message.start).getTime();
    var end = new Date(msg.message.end).getTime();
    var value = 1;
    var sin = 0.1;
    var up = true;
    var curve = msg.message.curve;
    var step = (msg.message.step || 60) * 1000;


    if (end < start) {
        var tmp = end;
        end = start;
        start = tmp;
    }

    end = new Date(end).setHours(24);

    function generate() {

        if (curve == 'sin') {
            if (sin == 6.2) {
                sin = 0
            } else {
                sin = Math.round((sin + 0.1) * 10) / 10;
            }
            value = Math.round(Math.sin(sin) * 10000) / 100;
        } else if (curve == 'dec') {
            value++
        } else if (curve == 'inc') {
            value--
        } else {
            if (up == true) {
                value++;
            } else {
                value--;
            }
        }
        start += step;

        pushValueIntoDB(id, {
            ts:     new Date(start).getTime() / 1000,
            val:    value,
            q:      0,
            ack:    true
        });


        if (start <= end) {
            setTimeout(function () {
                generate()
            }, 15)
        } else {
            adapter.sendTo(msg.from, msg.command, 'finished', msg.callback);
        }
    }

    var obj = {
        type: 'state',
        common: {
            name: msg.message.id,
            type: 'state',
            enabled: false,
            history: {}
        }
    };

    obj.common.history[adapter.namespace] = {
        enabled:        true,
        changesOnly:    false,
        debounce:       1000,
        retention:      31536000
    };

    adapter.setObject('demo.' + msg.message.id, obj);

    influxDPs[id] = {};
    influxDPs[id][adapter.namespace] = obj.common.history[adapter.namespace];

    generate();
}

process.on('uncaughtException', function(err) {
    adapter.log.warn('Exception: ' + err);
});