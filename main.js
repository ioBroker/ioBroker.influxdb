/* jshint -W097 */// jshint strict:false
/*jslint node: true */
"use strict";

//noinspection JSUnresolvedFunction
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
            for (var _id in influxDPs) {
                adapter.unsubscribeForeignStates(_id);
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

    adapter.config.dbname = adapter.config.dbname || utils.appName;

    client = influx({
        host:     adapter.config.host,
        port:     adapter.config.port, // optional, default 8086
        protocol: adapter.config.protocol, // optional, default 'http'
        username: adapter.config.user,
        password: adapter.config.password,
        database: adapter.config.dbname,
        timePrecision: 'ms'
    });

    client.createDatabase(adapter.config.dbname, function (err) {
        if (err && (!err.message || (err.message !== 'database ' + adapter.config.dbname + ' exists' && err.message.indexOf('database exits') !== -1))) {
            console.log('createDatabase: ' + JSON.stringify(err));
        } else {
            if (!err && adapter.config.retention) {
                client.query('CREATE RETENTION POLICY "global" ON ' + adapter.config.dbname + ' DURATION ' + adapter.config.retention + 's REPLICATION 1 DEFAULT', function (err) {
                    if (err && err.toString().indexOf('already exists') === -1) {
                        if (err) adapter.log.error(err);
                    }
                });
            }

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

        var lClient = influx({
            host:     msg.message.config.host,
            port:     msg.message.config.port,
            protocol: msg.message.config.protocol,  // optional, default 'http'
            username: msg.message.config.user,
            password: msg.message.config.password,
            database: msg.message.config.dbname || utils.appName
        });

        lClient.getDatabaseNames(function (err, arrayDatabaseNames) {
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
        client.deleteDatabase(adapter.config.dbname, function (err) {
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
    } else if (msg.command == 'generateDemo') {
        generateDemo(msg);
    } else if (msg.command == 'query') {
        query(msg);
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
            for (var _id in influxDPs) {
                adapter.subscribeForeignStates(_id);
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
        } else if (!settings.debounce) {
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

function pushValueIntoDB(id, state) {
    if (!client) {
        adapter.log.warn('No connection to DB');
        return;
    }

    state.ts = parseInt(state.ts, 10);

    // if less 2000.01.01 00:00:00
    if (state.ts < 946681200000) state.ts *= 1000;

    if (typeof state.val === 'object') state.val = JSON.stringify(state.val);

    if (state.val === 'true') {
        state.val = true;
    } else if (state.val === 'false') {
        state.val = false;
    } else {
        // try to convert to float
        var f = parseFloat(state.val);
        if (f == state.val) state.val = f;
    }

    client.writePoint(id, {
        value: state.val,
        time:  new Date(state.ts),
        from:  state.from,
        q:     state.q,
        ack:   state.ack
    }, null, function (err, result) {
        if (err) adapter.log.warn('writePoint: ' + err);
    });
}

function getHistory(msg) {

    var options = {
        id:         msg.message.id == '*' ? null : msg.message.id,
        start:      msg.message.options.start,
        end:        msg.message.options.end || ((new Date()).getTime() + 5000000),
        step:       parseInt(msg.message.options.step) || null,
        count:      parseInt(msg.message.options.count) || 500,
        ignoreNull: msg.message.options.ignoreNull,
        aggregate:  msg.message.options.aggregate || 'average', // One of: max, min, average, total
        limit:      msg.message.options.limit || adapter.config.limit || 2000,
        from:       msg.message.options.from  || false,
        q:          msg.message.options.q     || false,
        ack:        msg.message.options.ack   || false,
        ms:         msg.message.options.ms    || false,
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

            case 'onchange':
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
    if (options.end  && options.end    < 946681200000) options.end   *= 1000;
    if (options.start && options.start < 946681200000) options.start *= 1000;

    if (!options.start && !options.count) {
        options.start = options.end - 86400000; // - 1 day
    }

    query += " WHERE ";
    if (options.start) query += " time > '" + new Date(options.start).toISOString() + "' AND ";
    query += " time < '" + new Date(options.end).toISOString() + "'";

    if (options.step && options.aggregate !== 'onchange') {
        query += ' GROUP BY time(' + options.step + 's)';
        if (options.limit) query += ' LIMIT ' + options.limit;
    } else {
        query += ' LIMIT ' + options.count;
    }

    adapter.log.debug(query);

    // if specific id requested
    client.query(query, function (err, rows) {
        if (err) adapter.log.error('getHistory: ' + err);
        
        var result = [];
        if (rows && rows.length) {
            for (var rr = rows[0].length - 1; rr >= 0; rr--) {
                if (rows[0][rr].val === undefined) {
                    rows[0][rr].val = rows[0][rr].value;
                    delete rows[0][rr].value;
                }
                rows[0][rr].ts  = new Date(rows[0][rr].time).getTime();
                rows[0][rr].val = adapter.config.round ? Math.round(rows[0][rr].val * adapter.config.round) / adapter.config.round : rows[0][rr].val;
                delete rows[0][rr].time;
            }
            result = rows[0];
        }

        adapter.sendTo(msg.from, msg.command, {
            result:     result,
            error:      err,
            sessionId:  options.sessionId
        }, msg.callback);
    });
}

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
            ts:     new Date(start).getTime(),
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

function query(msg) {
    if (client) {
        client.query(msg.message.query, function (err, rows) {
            if (err) adapter.log.error('query: ' + err);

            for (var r = 0, l = rows.length; r < l; r++) {
                if (rows[r].time) {
                    rows[r].ts = rows[r].time;
                    delete rows[r].time;
                }
            }

            adapter.sendTo(msg.from, msg.command, {
                result: rows,
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

process.on('uncaughtException', function (err) {
    adapter.log.warn('Exception: ' + err);
});