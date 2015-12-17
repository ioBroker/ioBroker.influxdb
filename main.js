/* jshint -W097 */// jshint strict:false
/*jslint node: true */
"use strict";

var utils  = require(__dirname + '/lib/utils'); // Get common adapter utils
var influx;

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
        database: 'iobroker',
        timePrecision: 'ms'
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
    influx = (adapter.config.version == '0.8') ? require(__dirname + '/lib/node-influx-3.5.0') : require(__dirname + '/lib/node-influx');

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
        q:    state.q,
        ack:  state.ack
    }, {}, function (err, result) {
        if (err) adapter.log.warn(err);
    });

    checkRetention(id);
}

function toDateString(date) {
    if (typeof date !== 'object') {
        date = new Date(date);
    }
    return date.getFullYear() + '-' + ('0' + (date.getMonth() + 1)).slice(-2) + '-' + ('0' + date.getDate()).slice(-2) + ' ' +
        ('0' + date.getHours()).slice(-2) + ':' + ('0' + date.getMinutes()).slice(-2) + ':' + ('0' + date.getSeconds()).slice(-2);

}

function getHistory(msg) {

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

    if (!options.start && !options.count) {
        options.start = Math.round((new Date()).getTime() / 1000) - 86400; // - 1 day
    }
    query += " WHERE ";
    if (options.start) " time > '" + toDateString(options.start * 1000) + "' AND ";
    query += " time < '" + toDateString(options.end * 1000) + "'";

    if (options.step) {
        query += ' GROUP BY time(' + options.step + 's)';
        if (options.limit) query += ' LIMIT ' + options.limit;
    } else {
        query += ' LIMIT ' + options.count;
    }

    adapter.log.debug(query);

    // if specific id requested
    client.query(query, function (err, rows) {
        if (err) {
            adapter.log.error(err);
        }
        var result = [];
        if (rows && rows.length) {
            // InfluxDB 0.8
            if (rows[0].points) {
                for (var r = rows[0].points.length - 1; r >= 0; r--) {
                    var obj = {};
                    for (var c = 0; c < rows[0].columns.length; c++) {
                        if (rows[0].columns[c] === 'time') {
                            obj.ts = Math.round(rows[0].points[r][c] / 1000);
                            if (options.ms) obj.ms = rows[0].points[r][c] % 1000;
                        } else if (rows[0].columns[c] === 'from') {
                            if (options.from) obj.from = rows[0].points[r][c];
                        } else if (rows[0].columns[c] === 'q') {
                            if (options.from) obj.q = rows[0].points[r][c];
                        } else if (rows[0].columns[c] === 'ack') {
                            if (options.from) obj.ack = rows[0].points[r][c];
                        } else if (rows[0].columns[c] === 'value') {
                            obj.val = adapter.config.round ? Math.round(rows[0].points[r][c] * adapter.config.round) / adapter.config.round : rows[0].points[r][c];
                        } else {
                            obj[rows[0].columns[c]] = adapter.config.round ? Math.round(rows[0].points[r][c] * adapter.config.round) / adapter.config.round : rows[0].points[r][c];
                        }
                    }
                    if (options.ack && obj.ack === undefined) obj.ack = null;
                    result.push(obj);
                }
            } else {
                for (var r = rows[0].length - 1; r >= 0; r--) {
                    var t = new Date(rows[0][r].time).getTime();
                    rows[0][r].ts = Math.round(t / 1000);
                    if (options.ms) rows[0][r].ms = t % 1000;
                    rows[0][r].val = adapter.config.round ? Math.round(rows[0][r].val * adapter.config.round) / adapter.config.round : rows[0][r].val;
                    delete rows[0][r].time;
                }
                result = rows[0];
            }
        }

        adapter.sendTo(msg.from, msg.command, {
            result: result,
            error:  err
        }, msg.callback);
    });
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

function query(msg) {
    if (client) {
        client.query(msg.message.query, function (err, rows) {
            if (err) {
                adapter.log.error(err);
            }

            for (var r = 0, l = rows.length; r < l; r++) {
                if (rows[r].time) {
                    rows[r].ts = rows[r].time / 1000;
                    rows[r].ms = rows[r].time % 1000;
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

process.on('uncaughtException', function(err) {
    adapter.log.warn('Exception: ' + err);
});