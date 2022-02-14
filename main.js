/* jshint -W097 */
/* jshint strict: false */
/* jslint node: true */
'use strict';

//noinspection JSUnresolvedFunction
const utils       = require('@iobroker/adapter-core'); // Get common adapter utils
const DatabaseInfluxDB1x = require ('./lib/DatabaseInfluxDB1x.js').DatabaseInfluxDB1x; // TODO if else
const DatabaseInfluxDB2x = require ('./lib/DatabaseInfluxDB2x.js').DatabaseInfluxDB2x;
const fs          = require('fs');
const path        = require('path');
const [appName, adapterName] = require('./package.json').name.split('.');
const Aggregate   = require('./lib/aggregate.js');
const dataDir     = path.normalize(utils.controllerDir + '/' + require(utils.controllerDir + '/lib/tools').getDefaultDataDir());
const cacheFile   = dataDir + 'influxdata.json';

let adapter;

function isEqual(a, b) {
    //console.log('Compare ' + JSON.stringify(a) + ' with ' +  JSON.stringify(b));
    // Create arrays of property names
    if (a === null || a === undefined || b === null || b === undefined) {
        return a === b;
    }

    const aProps = Object.getOwnPropertyNames(a);
    const bProps = Object.getOwnPropertyNames(b);

    // If number of properties is different,
    // objects are not equivalent
    if (aProps.length !== bProps.length) {
        //console.log('num props different: ' + JSON.stringify(aProps) + ' / ' + JSON.stringify(bProps));
        return false;
    }

    for (let i = 0; i < aProps.length; i++) {
        const propName = aProps[i];

        if (typeof a[propName] !== typeof b[propName]) {
            //console.log('type props ' + propName + ' different');
            return false;
        }
        if (typeof a[propName] === 'object') {
            if (!isEqual(a[propName], b[propName])) {
                return false;
            }
        }
        else {
            // If values of same property are not equal,
            // objects are not equivalent
            if (a[propName] !== b[propName]) {
                //console.log('props ' + propName + ' different');
                return false;
            }
        }
    }

    // If we made it this far, objects
    // are considered equivalent
    return true;
}

function startAdapter(options) {
    options = options || {};
    Object.assign(options, {name: adapterName});

    adapter = new utils.Adapter(options);

    adapter.on('objectChange', (id, obj) => {
        const formerAliasId = adapter._aliasMap[id] ? adapter._aliasMap[id] : id;
        if (obj && obj.common &&
            (obj.common.custom && obj.common.custom[adapter.namespace] && typeof obj.common.custom[adapter.namespace] === 'object' && obj.common.custom[adapter.namespace].enabled)
        ) {
            const realId = id;
            let checkForRemove = true;
            if (obj.common.custom && obj.common.custom[adapter.namespace] && obj.common.custom[adapter.namespace].aliasId) {
                if (obj.common.custom[adapter.namespace].aliasId !== id) {
                    adapter._aliasMap[id] = obj.common.custom[adapter.namespace].aliasId;
                    adapter.log.debug(`Registered Alias: ${id} --> ${adapter._aliasMap[id]}`);
                    id = adapter._aliasMap[id];
                    checkForRemove = false;
                } else {
                    adapter.log.warn('Ignoring Alias-ID because identical to ID for ' + id);
                    obj.common.custom[adapter.namespace].aliasId = '';
                }
            }
            if (checkForRemove && adapter._aliasMap[id]) {
                adapter.log.debug(`Removed Alias: ${id} !-> ${adapter._aliasMap[id]}`);
                delete adapter._aliasMap[id];
            }

            if (!adapter._influxDPs[formerAliasId] && !adapter._subscribeAll) {
                // unsubscribe
                for (const _id in adapter._influxDPs) {
                    adapter.unsubscribeForeignStates(adapter._influxDPs[_id].realId);
                }
                adapter._subscribeAll = true;
                adapter.subscribeForeignStates('*');
            }

            if (obj.common.custom[adapter.namespace].debounce !== undefined && obj.common.custom[adapter.namespace].debounce !== null && obj.common.custom[adapter.namespace].debounce !== '') {
                obj.common.custom[adapter.namespace].debounce = parseInt(obj.common.custom[adapter.namespace].debounce, 10) || 0;
            } else {
                obj.common.custom[adapter.namespace].debounce = adapter.config.debounce;
            }

            obj.common.custom[adapter.namespace].changesOnly = obj.common.custom[adapter.namespace].changesOnly === 'true' || obj.common.custom[adapter.namespace].changesOnly === true;
            obj.common.custom[adapter.namespace].ignoreZero  = obj.common.custom[adapter.namespace].ignoreZero === 'true' || obj.common.custom[adapter.namespace].ignoreZero === true;
            obj.common.custom[adapter.namespace].ignoreBelowZero = obj.common.custom[adapter.namespace].ignoreBelowZero === 'true' || obj.common.custom[adapter.namespace].ignoreBelowZero === true;

            if (obj.common.custom[adapter.namespace].changesRelogInterval !== undefined && obj.common.custom[adapter.namespace].changesRelogInterval !== null && obj.common.custom[adapter.namespace].changesRelogInterval !== '') {
                obj.common.custom[adapter.namespace].changesRelogInterval = parseInt(obj.common.custom[adapter.namespace].changesRelogInterval, 10) || 0;
            } else {
                obj.common.custom[adapter.namespace].changesRelogInterval = adapter.config.changesRelogInterval;
            }

            if (obj.common.custom[adapter.namespace].changesMinDelta !== undefined && obj.common.custom[adapter.namespace].changesMinDelta !== null && obj.common.custom[adapter.namespace].changesMinDelta !== '') {
                obj.common.custom[adapter.namespace].changesMinDelta = parseFloat(obj.common.custom[adapter.namespace].changesMinDelta.toString().replace(/,/g, '.')) || 0;
            } else {
                obj.common.custom[adapter.namespace].changesMinDelta = adapter.config.changesMinDelta;
            }

            if (!obj.common.custom[adapter.namespace].storageType) obj.common.custom[adapter.namespace].storageType = false;

            if (adapter._influxDPs[formerAliasId] && !adapter._influxDPs[formerAliasId].storageTypeAdjustedInternally && adapter._influxDPs[formerAliasId][adapter.namespace] && isEqual(obj.common.custom[adapter.namespace], adapter._influxDPs[formerAliasId][adapter.namespace])) {
                adapter.log.debug(`Object ${id} unchanged. Ignore`);
                return;
            }

            const state = adapter._influxDPs[formerAliasId] ? adapter._influxDPs[formerAliasId].state : null;
            const skipped = adapter._influxDPs[formerAliasId] ? adapter._influxDPs[formerAliasId].skipped : null;

            adapter._influxDPs[id] = obj.common.custom;
            adapter._influxDPs[id].realId = realId;
            adapter._influxDPs[id].state = state;
            adapter._influxDPs[id].skipped = skipped;

            adapter._influxDPs[formerAliasId] && adapter._influxDPs[formerAliasId].relogTimeout && clearTimeout(adapter._influxDPs[formerAliasId].relogTimeout);

            writeInitialValue(adapter, realId, id);

            adapter.log.info(`enabled logging of ${id}, Alias=${id !== realId}`);
        } else {
            if (adapter._aliasMap[id]) {
                adapter.log.debug(`Removed Alias: ${id} !-> ${adapter._aliasMap[id]}`);
                delete adapter._aliasMap[id];
            }

            id = formerAliasId;

            if (adapter._influxDPs[id]) {
                adapter._influxDPs[id].relogTimeout && clearTimeout(adapter._influxDPs[id].relogTimeout);
                adapter._influxDPs[id].timeout && clearTimeout(adapter._influxDPs[id].timeout);

                delete adapter._influxDPs[id];
                adapter.log.info('disabled logging of ' + id);
            }
        }
    });

    adapter.on('stateChange', (id, state) => {
        id = adapter._aliasMap[id] ? adapter._aliasMap[id] : id;
        pushHistory(adapter, id, state);
    });

    adapter.on('ready', () => main(adapter));

    adapter.on('unload', callback => finish(adapter, callback));

    adapter.on('message', msg => processMessage(adapter, msg));

    adapter._subscribeAll          = false;
    adapter._influxDPs             = {};
    adapter._client                = null;
    adapter._seriesBufferChecker   = null;
    adapter._seriesBufferCounter   = 0;
    adapter._seriesBufferFlushPlanned = false;
    adapter._seriesBuffer          = {};
    adapter._conflictingPoints     = {};
    adapter._errorPoints           = {};

    adapter._tasksStart            = [];
    adapter._connected             = null;
    adapter._finished              = false;
    adapter._aliasMap              = {};

    return adapter;
}

process.on('SIGINT', () => adapter && adapter.setState && finish(adapter));
process.on('SIGTERM', () => adapter && adapter.setState && finish(adapter));

process.on('uncaughtException', err => {
    adapter.log.warn('Exception: ' + err);
    if (adapter && adapter.setState) {
        finish(adapter);
    }
});

function setConnected(adapter, isConnected) {
    if (adapter._connected !== isConnected) {
        adapter._connected = isConnected;
        adapter.setState('info.connection', adapter._connected, true, err =>
            // analyse if the state could be set (because of permissions)
            err ? adapter.log.error('Can not update adapter._connected state: ' + err) :
                adapter.log.debug('connected set to ' + adapter._connected));
    }
}

function reconnect(adapter) {
    setConnected(adapter, false);
    stopPing(adapter);
    if (!adapter._reconnectTimeout) {
        adapter._reconnectTimeout = setTimeout(() => {
            adapter._reconnectTimeout = null;
            connect(adapter);
        }, adapter.config.reconnectInterval);
    }
}

function startPing(adapter) {
    adapter._pingInterval = adapter._pingInterval || setInterval(() => ping(adapter), adapter.config.pingInterval);
}

function stopPing(adapter) {
    adapter._pingInterval && clearInterval(adapter._pingInterval);
    adapter._pingInterval = null;
}

function ping(adapter) {
    adapter._client.ping && adapter.config.pingserver !== false && adapter._client.ping(adapter.config.pingInterval - 1000 < 0 ? 1000 : adapter.config.pingInterval - 1000)
        .then(
            hosts => {
                if (!hosts.some(host => host.online)) {
                    reconnect(adapter);
                } else {
                    adapter.log.debug('PING OK');
                }
            },
            error => {
                adapter.log.error(`Error during ping: ${error}. Attempting reconnect.`);
                reconnect(adapter);
            });
}

function connect(adapter) {
    adapter.log.info(`Connecting ${adapter.config.protocol}://${adapter.config.host}:${adapter.config.port} ...`);

    adapter.config.dbname = adapter.config.dbname || appName;

    adapter.config.seriesBufferMax = parseInt(adapter.config.seriesBufferMax, 10) || 0;

    adapter.log.info('Influx DB Version used: ' + adapter.config.dbversion);

    switch (adapter.config.dbversion) {
        case '2.x':
            if (/[\x00-\x08\x0E-\x1F\x80-\xFF]/.test(adapter.config.token)) {
                adapter.log.error('Token error: Please re-enter the token in Admin. Stopping');
                return;
            }
            adapter._client = new DatabaseInfluxDB2x(
                adapter.log,
                adapter.config.host,
                adapter.config.port, // optional, default 8086
                adapter.config.protocol, // optional, default 'http'
                adapter.config.token,
                adapter.config.organization,
                adapter.config.dbname,
                adapter.config.usetags
            )
            break;
        case '1.x':
        default:
            if (/[\x00-\x08\x0E-\x1F\x80-\xFF]/.test(adapter.config.password)) {
                adapter.log.error('Password error: Please re-enter the password in Admin. Stopping');
                return;
            }
            adapter._client = new DatabaseInfluxDB1x(
                adapter.log,
                adapter.config.host,
                adapter.config.port, // optional, default 8086
                adapter.config.protocol, // optional, default 'http'
                adapter.config.user,
                adapter.config.password,
                adapter.config.dbname,
                'ms',
                30000
            );
            break;
    }

    if (adapter.config.pingserver === false) {
        adapter.log.info('Deactivated DB health checks (ping) via configuration');
    }

    adapter._client.getDatabaseNames((err, dbNames) => {
        if (err) {
            adapter.log.error(err);
            reconnect(adapter);
        } else {
            setConnected(adapter, true); // ??? to early, move down?
            if (!dbNames.includes(adapter.config.dbname)) {
                adapter._client.createDatabase(adapter.config.dbname, err => {
                    if (err) {
                        adapter.log.error(err);
                        reconnect(adapter);
                    }

                    //Check and potentially update retention policy
                    adapter._client.applyRetentionPolicyToDB(adapter.config.dbname, adapter.config.retention, err => {
                        if (err) {
                            //Ignore issues with creating/altering retention policy, as it might be due to insufficient permissions
                            adapter.log.warn(err);
                        }
                    });

                    if (adapter.config.dbversion === '2.x') {
                        checkMetaDataStorageType(adapter);
                    }
                });
            } else {
                //Check and potentially update retention policy
                adapter._client.applyRetentionPolicyToDB(adapter.config.dbname, adapter.config.retention, err => {
                    if (err) {
                        //Ignore issues with creating/altering retention policy, as it might be due to insufficient permissions
                        adapter.log.warn(err);
                    }
                });

                if (adapter.config.dbversion === '2.x') {
                    checkMetaDataStorageType(adapter);
                }
            }
        }
    });
}

function checkMetaDataStorageType(adapter) {
    adapter._client.getMetaDataStorageType((error,storageType) => {
        if (error)
            adapter.log.error(`Error checking for metadata storage type: ${error}`);
        else {
            adapter.log.debug(`Storage type for metadata found in DB: ${storageType}`);
            if ((storageType === 'tags' && !adapter.config.usetags) || (storageType === 'fields' && adapter.config.usetags)) {
                adapter.log.error(`Cannot use ${adapter.config.usetags ? 'tags' : 'fields'} for metadata (q, ack, from) since ` +
                    `the selected DB already uses ${storageType} instead. Please change your adapter configuration, or choose a DB ` +
                    `that already uses ${adapter.config.usetags ? 'tags' : 'fields'}, or is empty.`);
                setConnected(adapter, false);
                finish(adapter, null);
            } else {
                setConnected(adapter, true);
                processStartValues(adapter);
                adapter.log.info('Connected!');
                startPing(adapter);
            }
        }
    });
}

function processStartValues(adapter) {
    if (adapter._tasksStart && adapter._tasksStart.length) {
        const taskId = adapter._tasksStart.shift();
        if (adapter._influxDPs[taskId] && adapter._influxDPs[taskId][adapter.namespace].changesOnly) {
            pushHistory(adapter, taskId, adapter._influxDPs[taskId].state, true);
        }
        setImmediate(() => processStartValues(adapter));
    }
}

function getRetention(adapter, msg) {
    adapter.log.debug('getRetention invoked, checking DB');
    try {
        adapter._client.getRetentionPolicyForDB(adapter.config.dbname, result => {
            adapter.sendTo(msg.from, msg.command, {
                result:     result,
                error:      null
            }, msg.callback);
        });
    } catch (ex) {
        adapter.sendTo(msg.from, msg.command, {error: ex.toString()}, msg.callback);
    }
}

function testConnection(adapter, msg) {
    adapter.log.debug('testConnection msg-object: ' + JSON.stringify(msg));
    msg.message.config.port = parseInt(msg.message.config.port, 10) || 0;

    let timeout;
    try {
        timeout = setTimeout(() => {
            timeout = null;
            adapter.sendTo(msg.from, msg.command, {error: 'connect timeout'}, msg.callback);
        }, 5000);

        let lClient;
        adapter.log.debug('TEST DB Version: ' + msg.message.config.dbversion);
        switch (msg.message.config.dbversion) {
            case '2.x':
                adapter.log.info('Connecting to InfluxDB 2');
                lClient = new DatabaseInfluxDB2x(
                    adapter.log,
                    msg.message.config.host,
                    msg.message.config.port,
                    msg.message.config.protocol,  // optional, default 'http'
                    msg.message.config.token,
                    msg.message.config.organization,
                    msg.message.config.dbname || appName
                )
                break;
            default:
            case '1.x':
                lClient = new DatabaseInfluxDB1x(
                    adapter.log,
                    msg.message.config.host,
                    msg.message.config.port,
                    msg.message.config.protocol,  // optional, default 'http'
                    msg.message.config.user,
                    msg.message.config.password,
                    msg.message.config.dbname || appName
                );
                break;
        }

        lClient.getDatabaseNames((err /* , arrayDatabaseNames*/ ) => {
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

function destroyDB(adapter, msg) {
    if (!adapter._client) {
        return adapter.sendTo(msg.from, msg.command, {error: 'Not connected'}, msg.callback);
    }
    try {
        adapter._client.dropDatabase(adapter.config.dbname, err => {
            if (err) {
                adapter.log.error(err);
                adapter.sendTo(msg.from, msg.command, {error: err.toString()}, msg.callback);
            } else {
                adapter.sendTo(msg.from, msg.command, {error: null}, msg.callback);
                // restart adapter
                setTimeout(() => {
                    adapter.getForeignObject('system.adapter.' + adapter.namespace, (err, obj) => {
                        if (!err) {
                            adapter.setForeignObject(obj._id, obj);
                        } else {
                            adapter.log.error(`Cannot read object "system.adapter.${adapter.namespace}": ${err}`);
                            adapter.stop();
                        }
                    });
                }, 2000);
            }
        });
    } catch (err) {
        return adapter.sendTo(msg.from, msg.command, {error: err.toString()}, msg.callback);
    }
}

function processMessage(adapter, msg) {
    adapter.log.debug(`Incoming message ${msg.command} from ${msg.from}`);
    if (msg.command === 'features') {
        adapter.sendTo(msg.from, msg.command, {supportedFeatures: []}, msg.callback);
    } else
    if (msg.command === 'getHistory') {
        adapter.config.dbversion === '1.x' ? getHistory(adapter, msg) : getHistoryIflx2(adapter, msg);
    }
    else if (msg.command === 'test') {
        testConnection(adapter, msg);
    }
    else if (msg.command === 'destroy') {
        destroyDB(adapter, msg);
    }
    else if (msg.command === 'query') {
        switch (adapter.config.dbversion) {
            case '2.x':
                // Influx 2.x uses Flux instead of InfluxQL, so for multiple statements there is no delimiter by default, so we introduce ;
                multiQuery(adapter, msg);
                break;
            case '1.x':
            default:
                query(adapter, msg);
                break;
        }
    }
    else if (msg.command === 'getConflictingPoints') {
        getConflictingPoints(adapter, msg);
    }
    else if (msg.command === 'resetConflictingPoints') {
        resetConflictingPoints(adapter, msg);
    }
    else if (msg.command === 'storeState') {
        storeState(adapter, msg);
    }
    else if (msg.command === 'enableHistory') {
        enableHistory(adapter, msg);
    }
    else if (msg.command === 'disableHistory') {
        disableHistory(adapter, msg);
    }
    else if (msg.command === 'getEnabledDPs') {
        getEnabledDPs(adapter, msg);
    }
    else if (msg.command === 'stopInstance') {
        finish(adapter, () => {
            if (msg.callback) {
                adapter.sendTo(msg.from, msg.command, 'stopped', msg.callback);
                setTimeout(() => adapter.terminate ? adapter.terminate() : process.exit(), 200);
            }
        });
    }
    else if (msg.command === 'getRetention') {
        getRetention(adapter, msg);
    }
}

function getConflictingPoints(adapter, msg) {
    return adapter.sendTo(msg.from, msg.command, {conflictingPoints: adapter._conflictingPoints}, msg.callback);
}

function resetConflictingPoints(adapter, msg) {
    const resultMsg = {reset: true, conflictingPoints: adapter._conflictingPoints};
    adapter._conflictingPoints = {};
    return adapter.sendTo(msg.from, msg.command, resultMsg, msg.callback);
}

function main(adapter) {
    adapter.config.port = parseInt(adapter.config.port, 10) || 0;

    // set default history if not yet set
    adapter.getForeignObject('system.config', (err, obj) => {
        if (obj && obj.common && !obj.common.defaultHistory) {
            obj.common.defaultHistory = adapter.namespace;
            adapter.setForeignObject('system.config', obj, err => {
                if (err) {
                    adapter.log.error(`Cannot set default history instance: ${err}`);
                } else {
                    adapter.log.info(`Set default history instance to "${adapter.namespace}"`);
                }
            });
        }
    });

    setConnected(adapter, false);

    adapter.config.reconnectInterval = parseInt(adapter.config.reconnectInterval, 10) || 10000;
    adapter.config.pingInterval      = parseInt(adapter.config.pingInterval, 10) || 15000;

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
            const fileContent = fs.readFileSync(cacheFile, 'utf-8');
            const tempData = JSON.parse(fileContent, (key, value) =>
                key === 'time' ? new Date(value) : value);

            if (tempData.seriesBufferCounter) adapter._seriesBufferCounter = tempData.seriesBufferCounter;
            if (tempData.seriesBuffer)        adapter._seriesBuffer        = tempData.seriesBuffer;
            if (tempData.conflictingPoints)   adapter._conflictingPoints   = tempData.conflictingPoints;
            adapter.log.info(`Buffer initialized with data for ${adapter._seriesBufferCounter} points and ${Object.keys(adapter._conflictingPoints).length} conflicts from last exit`);
            fs.unlinkSync(cacheFile);
        }
    } catch (err) {
        adapter.log.info('No stored data from last exit found');
    }

    // read all custom settings
    adapter.getObjectView('system', 'custom', {}, (err, doc) => {
        if (err) adapter.log.error('main/getObjectView: ' + err);
        let count = 0;
        if (doc && doc.rows) {
            for (let i = 0, l = doc.rows.length; i < l; i++) {
                if (doc.rows[i].value) {
                    let id = doc.rows[i].id;
                    const realId = id;
                    if (doc.rows[i].value[adapter.namespace] && doc.rows[i].value[adapter.namespace].aliasId) {
                        adapter._aliasMap[id] = doc.rows[i].value[adapter.namespace].aliasId;
                        adapter.log.debug(`Found Alias: ${id} --> ${adapter._aliasMap[id]}`);
                        id = adapter._aliasMap[id];
                    }
                    adapter._influxDPs[id] = doc.rows[i].value;

                    if (!adapter._influxDPs[id][adapter.namespace] || typeof adapter._influxDPs[id][adapter.namespace] !== 'object' || adapter._influxDPs[id][adapter.namespace].enabled === false) {
                        delete adapter._influxDPs[id];
                    } else {
                        count++;
                        adapter.log.info(`enabled logging of ${id}, Alias=${id !== realId}, ${count} points now activated`);

                        if (adapter._influxDPs[id][adapter.namespace].debounce !== undefined && adapter._influxDPs[id][adapter.namespace].debounce !== null && adapter._influxDPs[id][adapter.namespace].debounce !== '') {
                            adapter._influxDPs[id][adapter.namespace].debounce = parseInt(adapter._influxDPs[id][adapter.namespace].debounce, 10) || 0;
                        } else {
                            adapter._influxDPs[id][adapter.namespace].debounce = adapter.config.debounce;
                        }

                        adapter._influxDPs[id][adapter.namespace].changesOnly   = adapter._influxDPs[id][adapter.namespace].changesOnly   === 'true' || adapter._influxDPs[id][adapter.namespace].changesOnly === true;
                        adapter._influxDPs[id][adapter.namespace].ignoreZero    = adapter._influxDPs[id][adapter.namespace].ignoreZero    === 'true' || adapter._influxDPs[id][adapter.namespace].ignoreZero  === true;
                        adapter._influxDPs[id][adapter.namespace].ignoreBelowZero = adapter._influxDPs[id][adapter.namespace].ignoreBelowZero === 'true' || adapter._influxDPs[id][adapter.namespace].ignoreBelowZero === true;

                        if (adapter._influxDPs[id][adapter.namespace].changesRelogInterval !== undefined && adapter._influxDPs[id][adapter.namespace].changesRelogInterval !== null && adapter._influxDPs[id][adapter.namespace].changesRelogInterval !== '') {
                            adapter._influxDPs[id][adapter.namespace].changesRelogInterval = parseInt(adapter._influxDPs[id][adapter.namespace].changesRelogInterval, 10) || 0;
                        } else {
                            adapter._influxDPs[id][adapter.namespace].changesRelogInterval = adapter.config.changesRelogInterval;
                        }
                        if (adapter._influxDPs[id][adapter.namespace].changesMinDelta !== undefined && adapter._influxDPs[id][adapter.namespace].changesMinDelta !== null && adapter._influxDPs[id][adapter.namespace].changesMinDelta !== '') {
                            adapter._influxDPs[id][adapter.namespace].changesMinDelta = parseFloat(adapter._influxDPs[id][adapter.namespace].changesMinDelta) || 0;
                        } else {
                            adapter._influxDPs[id][adapter.namespace].changesMinDelta = adapter.config.changesMinDelta;
                        }
                        if (!adapter._influxDPs[id][adapter.namespace].storageType) adapter._influxDPs[id][adapter.namespace].storageType = false;

                        adapter._influxDPs[id].realId  = realId;
                        writeInitialValue(adapter, realId, id);
                    }
                }
            }
        }

        if (count < 20) {
            for (const _id in adapter._influxDPs) {
                if (adapter._influxDPs.hasOwnProperty(_id)) {
                    adapter.subscribeForeignStates(adapter._influxDPs[_id].realId);
                }
            }
        } else {
            adapter._subscribeAll = true;
            adapter.subscribeForeignStates('*');
        }
    });

    adapter.subscribeForeignObjects('*');

    connect(adapter);

    if (adapter._client) {
        // store all buffered data every x seconds to not lost the data
        adapter._seriesBufferChecker = setInterval(() => {
            adapter._seriesBufferFlushPlanned = true;
            storeBufferedSeries(adapter);
        }, adapter.config.seriesBufferFlushInterval * 1000);
    }
}

function writeInitialValue(adapter, realId, id) {
    adapter.getForeignState(realId, (err, state) => {
        if (state && adapter._influxDPs[id]) {
            state.from = 'system.adapter.' + adapter.namespace;
            adapter._influxDPs[id].state = state;
            adapter._tasksStart.push(id);
            if (adapter._tasksStart.length === 1 && adapter._connected) {
                processStartValues(adapter);
            }
        }
    });
}

function pushHistory(adapter, id, state, timerRelog) {
    if (timerRelog === undefined) timerRelog = false;
    // Push into InfluxDB
    if (adapter._influxDPs[id]) {
        const settings = adapter._influxDPs[id][adapter.namespace];

        if (!settings || !state) return;

        if (state && state.val === undefined) {
            adapter.log.warn(`state value undefined received for ${id} which is not allowed. Ignoring.`);
            return;
        }

        if (typeof state.val === 'string' && settings.storageType !== 'String') {
            const f = parseFloat(state.val);
            if (f == state.val) {
                state.val = f;
            }
        }
        if (adapter._influxDPs[id].state && settings.changesOnly && !timerRelog) {
            if (settings.changesRelogInterval === 0) {
                if (state.ts !== state.lc) {
                    adapter._influxDPs[id].skipped = state; // remember new timestamp
                    adapter.log.debug(`value not changed ${id}, last-value=${adapter._influxDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
                    return;
                }
            } else if (adapter._influxDPs[id].lastLogTime) {
                if ((state.ts !== state.lc) && (Math.abs(adapter._influxDPs[id].lastLogTime - state.ts) < settings.changesRelogInterval * 1000)) {
                    adapter.log.debug(`value not changed ${id}, last-value=${adapter._influxDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
                    adapter._influxDPs[id].skipped = state; // remember new timestamp
                    return;
                }
                if (state.ts !== state.lc) {
                    adapter.log.debug(`value-changed-relog ${id}, value=${state.val}, lastLogTime=${adapter._influxDPs[id].lastLogTime}, ts=${state.ts}`);
                }
            }
            if (settings.changesMinDelta !== 0 && typeof state.val === 'number' && Math.abs(adapter._influxDPs[id].state.val - state.val) < settings.changesMinDelta) {
                adapter.log.debug(`Min-Delta not reached ${id}, last-value=${adapter._influxDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
                adapter._influxDPs[id].skipped = state; // remember new timestamp
                return;
            } else if (typeof state.val === 'number') {
                adapter.log.debug(`Min-Delta reached ${id}, last-value=${adapter._influxDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
            } else {
                adapter.log.debug(`Min-Delta ignored because no number ${id}, last-value=${adapter._influxDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
            }
        }

        if (adapter._influxDPs[id].relogTimeout) {
            clearTimeout(adapter._influxDPs[id].relogTimeout);
            adapter._influxDPs[id].relogTimeout = null;
        }
        if (settings.changesRelogInterval > 0) {
            adapter._influxDPs[id].relogTimeout = setTimeout(() => reLogHelper(adapter, id), settings.changesRelogInterval * 1000);
        }

        let ignoreDebounce = false;
        if (timerRelog) {
            state.ts = Date.now();
            state.from = 'system.adapter.' + adapter.namespace;
            adapter.log.debug(`timed-relog ${id}, value=${state.val}, lastLogTime=${adapter._influxDPs[id].lastLogTime}, ts=${state.ts}`);
            ignoreDebounce = true;
        } else {
            if (settings.changesOnly && adapter._influxDPs[id].skipped) {
                adapter._influxDPs[id].state = adapter._influxDPs[id].skipped;
                pushHelper(adapter, id);
            }
            if (adapter._influxDPs[id].state && ((adapter._influxDPs[id].state.val === null && state.val !== null) || (adapter._influxDPs[id].state.val !== null && state.val === null))) {
                ignoreDebounce = true;
            } else if (!adapter._influxDPs[id].state && state.val === null) {
                ignoreDebounce = true;
            }

            // only store state if really changed
            adapter._influxDPs[id].state = state;
        }

        adapter._influxDPs[id].lastLogTime = state.ts;
        adapter._influxDPs[id].skipped = null;

        if (settings.debounce && !ignoreDebounce) {
            // Discard changes in de-bounce time to store last stable value
            adapter._influxDPs[id].timeout && clearTimeout(adapter._influxDPs[id].timeout);
            adapter._influxDPs[id].timeout = setTimeout(() => pushHelper(adapter, id), settings.debounce);
        } else {
            pushHelper(adapter, id);
        }
    }
}

function reLogHelper(adapter, _id) {
    if (!adapter._influxDPs[_id]) {
        adapter.log.info('non-existing id ' + _id);
        return;
    }
    adapter._influxDPs[_id].relogTimeout = null;
    if (adapter._influxDPs[_id].skipped) {
        adapter._influxDPs[_id].state = adapter._influxDPs[_id].skipped;
        adapter._influxDPs[_id].state.from = 'system.adapter.' + adapter.namespace;
        adapter._influxDPs[_id].skipped = null;
        pushHistory(adapter, _id, adapter._influxDPs[_id].state, true);
    }
    else {
        adapter.getForeignState(adapter._influxDPs[_id].realId, (err, state) => {
            if (err) {
                adapter.log.info(`init timed Relog: can not get State for ${_id}: ${err}`);
            } else if (!state) {
                adapter.log.info(`init timed Relog: disable relog because state not set so far for ${_id}: ${JSON.stringify(state)}`);
            } else if (adapter._influxDPs[_id]) {
                adapter.log.debug(`init timed Relog: getState ${_id}:  Value=${state.val}, ack=${state.ack}, ts=${state.ts}, lc=${state.lc}`);
                adapter._influxDPs[_id].state = state;
                pushHistory(adapter, _id, adapter._influxDPs[_id].state, true);
            }
        });
    }
}

function pushHelper(adapter, _id, cb) {
    if (!adapter._influxDPs[_id] || !adapter._influxDPs[_id].state || !adapter._influxDPs[_id][adapter.namespace]) {
        return cb && setImmediate(cb, `ID ${_id} not activated for logging`);
    }
    const _settings = adapter._influxDPs[_id][adapter.namespace];
    // if it was not deleted in this time
    adapter._influxDPs[_id].timeout = null;

    if (adapter._influxDPs[_id].state.val === null) { // InfluxDB can not handle null values
        return cb && setImmediate(cb, `null value for ${_id} can not be handled`);
    }
    if (adapter._influxDPs[_id].state.val === null) { // InfluxDB can not handle null values
        return cb && setImmediate(cb, `null value for ${_id} can not be handled`);
    }
    if (typeof adapter._influxDPs[_id].state.val === 'number' && !isFinite(adapter._influxDPs[_id].state.val)) { // InfluxDB can not handle Infinite values
        return cb && setImmediate(cb, `Non Finite value ${adapter._influxDPs[_id].state.val} for ${_id} can not be handled`);
    }

    if (typeof adapter._influxDPs[_id].state.val === 'object') {
        adapter._influxDPs[_id].state.val = JSON.stringify(adapter._influxDPs[_id].state.val);
    }

    adapter.log.debug(`Datatype ${_id}: Currently: ${typeof adapter._influxDPs[_id].state.val}, StorageType: ${_settings.storageType}`);
    if (typeof adapter._influxDPs[_id].state.val === 'string' && _settings.storageType !== 'String') {
        adapter.log.debug('Do Automatic Datatype conversion for ' + _id);
        const f = parseFloat(adapter._influxDPs[_id].state.val);
        if (f == adapter._influxDPs[_id].state.val) {
            adapter._influxDPs[_id].state.val = f;
        } else if (adapter._influxDPs[_id].state.val === 'true') {
            adapter._influxDPs[_id].state.val = true;
        } else if (adapter._influxDPs[_id].state.val === 'false') {
            adapter._influxDPs[_id].state.val = false;
        }
    }
    if (_settings.storageType === 'String' && typeof adapter._influxDPs[_id].state.val !== 'string') {
        adapter._influxDPs[_id].state.val = adapter._influxDPs[_id].state.val.toString();
    }
    else if (_settings.storageType === 'Number' && typeof adapter._influxDPs[_id].state.val !== 'number') {
        if (typeof adapter._influxDPs[_id].state.val === 'boolean') {
            adapter._influxDPs[_id].state.val = adapter._influxDPs[_id].state.val?1:0;
        }
        else {
            adapter.log.info(`Do not store value "${adapter._influxDPs[_id].state.val}" for ${_id} because no number`);
            return cb && setImmediate(cb, `do not store value for ${_id} because no number`);
        }
    }
    else if (_settings.storageType === 'Boolean' && typeof adapter._influxDPs[_id].state.val !== 'boolean') {
        adapter._influxDPs[_id].state.val = !!adapter._influxDPs[_id].state.val;
    }
    pushValueIntoDB(adapter, _id, adapter._influxDPs[_id].state, () =>
        cb && setImmediate(cb));
}

function pushValueIntoDB(adapter, id, state, cb) {
    if (!adapter._client) {
        adapter.log.warn('No connection to DB');
        return cb && cb('No connection to DB');
    }

    if (state.val === null || state.val === undefined) {
        return cb && cb('InfluxDB can not handle null/non-existing values');
    } // InfluxDB can not handle null/non-existing values
    if (typeof state.val === 'number' && !isFinite(state.val)) {
        return cb && cb(`InfluxDB can not handle non finite values like ${state.val}`);
    }

    if (adapter._influxDPs[id] && adapter._influxDPs[id][adapter.namespace] && adapter._influxDPs[id][adapter.namespace].ignoreZero && state.val === 0) {
        adapter.log.debug(`pushValueIntoDB called for ${id} was ignored because the value zero or null`);
        return cb && cb();
    } else
    if (state && adapter._influxDPs[id] && adapter._influxDPs[id][adapter.namespace] && adapter._influxDPs[id][adapter.namespace].ignoreBelowZero && typeof state.val === 'number' && state.val < 0) {
        adapter.log.debug(`pushValueIntoDB called for ${id} and state: ${JSON.stringify(state)} was ignored because the value is below 0`);
        return cb && cb();
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
        const f = parseFloat(state.val);
        if (f == state.val) state.val = f;
    }*/

    //adapter.log.debug('write value ' + state.val + ' for ' + id);
    const influxFields = {
        value: state.val,
        time:  new Date(state.ts),
        from:  state.from || '',
        q:     state.q || 0,
        ack:   !!state.ack
    };

    if ((adapter._conflictingPoints[id] || adapter.config.seriesBufferMax === 0) && (adapter._connected && adapter._client.request && adapter._client.request.getHostsAvailable().length > 0)) {
        if (adapter.config.seriesBufferMax !== 0) {
            adapter.log.debug(`Direct writePoint("${id} - ${influxFields.value} / ${influxFields.time}")`);
        }
        writeOnePointForID(adapter, id, influxFields, true, cb);
    } else {
        addPointToSeriesBuffer(adapter, id, influxFields, cb);
    }
}

function addPointToSeriesBuffer(adapter, id, stateObj, cb) {
    if ((adapter._conflictingPoints[id] || adapter.config.seriesBufferMax === 0) && (adapter._connected && adapter._client.request && adapter._client.request.getHostsAvailable().length > 0)) {
        if (adapter.config.seriesBufferMax !== 0) {
            adapter.log.debug(`Direct writePoint("${id} - ${stateObj.value} / ${stateObj.time}")`);
        }
        return void writeOnePointForID(adapter, id, stateObj, true, cb);
    }

    if (!adapter._seriesBuffer[id]) {
        adapter._seriesBuffer[id] = [];
    }
    adapter._seriesBuffer[id].push([stateObj]);
    adapter._seriesBufferCounter++;
    if (adapter._seriesBufferCounter > adapter.config.seriesBufferMax && adapter._connected && adapter._client.request && adapter._client.request.getHostsAvailable().length > 0 && !adapter._seriesBufferFlushPlanned) {
        // flush out
        adapter._seriesBufferFlushPlanned = true;
        setImmediate(() => storeBufferedSeries(adapter, cb));
    } else {
        cb && cb();
    }
}

function storeBufferedSeries(adapter, cb) {
    if (Object.keys(adapter._seriesBuffer).length === 0) {
        return cb && cb();
    }

    if (!adapter._client || adapter._client.request.getHostsAvailable().length === 0) {
        setConnected(adapter, false);
        adapter.log.info('Currently no hosts available, try later');
        adapter._seriesBufferFlushPlanned = false;
        return cb && cb('Currently no hosts available, try later');
    }
    if (!adapter._connected) {
        adapter.log.info('Not connected to InfluxDB, try later');
        adapter._seriesBufferFlushPlanned = false;
        return cb && cb('Not connected to InfluxDB, try later');
    }
    adapter._seriesBufferChecker && clearInterval(adapter._seriesBufferChecker);

    adapter.log.info(`Store ${adapter._seriesBufferCounter} buffered influxDB history points`);

    const currentBuffer = adapter._seriesBuffer;
    if (adapter._seriesBufferCounter > 15000) {
        // if we have too many data points in buffer; we better writer them per id
        adapter.log.info(`Too many data points (${adapter._seriesBufferCounter}) to write at once; write per ID`);
        writeAllSeriesPerID(adapter, currentBuffer, cb);
    } else {
        writeAllSeriesAtOnce(adapter, currentBuffer, cb);
    }
    adapter._seriesBuffer = {};
    adapter._seriesBufferCounter = 0;
    adapter._seriesBufferFlushPlanned = false;
    adapter._seriesBufferChecker = setInterval(() =>
        storeBufferedSeries(adapter), adapter.config.seriesBufferFlushInterval * 1000);
}

function writeAllSeriesAtOnce(adapter, series, cb) {
    adapter._client.writeSeries(series, (err /* , result */) => {
        if (err) {
            adapter.log.warn('Error on writeSeries: ' + err);
            if (adapter._client.request.getHostsAvailable().length === 0) {
                setConnected(adapter, false);
                adapter.log.info('Host not available, move all points back in the Buffer');
                // error caused InfluxDB adapter._client to remove the host from available for now
                Object.keys(series).forEach(id => {
                    if (series[id].length) {
                        adapter._seriesBuffer[id] = adapter._seriesBuffer[id] || [];
                        adapter._seriesBufferCounter += series[id].length;
                        series[id].forEach(s => adapter._seriesBuffer[id].push(s));
                    }
                });
                reconnect(adapter);
            } else if (err.message && typeof err.message === 'string' && err.message.includes('partial write') && !err.message.includes('field type conflict')) {
                adapter.log.warn('All possible data points were written, others can not really be corrected');
            } else {
                adapter.log.info(`Try to write ${Object.keys(series).length} Points separate to find the conflicting id`);
                // fallback and send data per id to find out problematic id!
                return writeAllSeriesPerID(adapter, series, cb);
            }
        } else {
            setConnected(adapter, true);
        }
        cb && cb();
    });
}

function writeAllSeriesPerID(adapter, series, cb, idList) {
    if (!idList) {
        idList = Object.keys(series);
    }
    if (!idList.length) {
        return cb && cb();
    }
    const id = idList.shift();
    writeSeriesPerID(adapter, id, series[id], () => writeAllSeriesPerID(adapter, series, cb, idList));
}

function writeSeriesPerID(adapter, seriesId, points, cb) {
    if (!points.length) {
        return cb && cb();
    }
    adapter.log.debug(`writePoints ${points.length} for ${seriesId} at once`);

    const pointsToSend = points.splice(0, 15000);
    if (points.length) { // We still have some left
        adapter.log.info(`Too many dataPoints (${pointsToSend.length + points.length}) for "${seriesId}" to write at once; split in 15.000 batches`);
    }
    adapter._client.writePoints(seriesId, pointsToSend, err => {
        if (err) {
            adapter.log.warn(`Error on writePoints for ${seriesId}: ${err}`);
            if ((adapter._client.request.getHostsAvailable().length === 0) || (err.message && (err.message === 'timeout' || err.message.includes('timed out') ))) {
                adapter.log.info('Host not available, move all points back in the Buffer');
                // error caused InfluxDB adapter._client to remove the host from available for now
                adapter._seriesBuffer[seriesId] = adapter._seriesBuffer[seriesId] || [];

                for (let i = 0; i < pointsToSend.length; i++) {
                    adapter._seriesBuffer[seriesId].push(pointsToSend[i]);
                    adapter._seriesBufferCounter++;
                }
                for (let i = 0; i < points.length; i++) {
                    adapter._seriesBuffer[seriesId].push(points[i]);
                    adapter._seriesBufferCounter++;
                }
                reconnect(adapter);
                return cb && cb();
            } else {
                adapter.log.warn(`Try to write ${pointsToSend.length} Points separate to find the conflicting one`);
                // we found the conflicting id
                return writePointsForID(adapter, seriesId, pointsToSend, () => writeSeriesPerID(adapter, seriesId, points, cb));
            }
        } else {
            setConnected(adapter, true);
        }
        writeSeriesPerID(adapter, seriesId, points, cb);
    });
}

function writePointsForID(adapter, seriesId, points, cb) {
    if (!points.length) {
        return cb && cb();
    }
    adapter.log.debug(`writePoint ${points.length} for ${seriesId} separate`);

    const point = points.shift();
    writeOnePointForID(adapter, seriesId, point[0], false, () =>
        setImmediate (() =>
            writePointsForID(adapter, seriesId, points, cb)));
}

function writeOnePointForID(adapter, pointId, point, directWrite, cb) {
    directWrite = directWrite || false;

    if (!adapter._connected) {
        addPointToSeriesBuffer(adapter, pointId, point);
        return cb && cb();
    }

    adapter._client.writePoint(pointId, point, null, (err /* , result */) => {
        if (err) {
            adapter.log.warn(`Error on writePoint("${JSON.stringify(point)}): ${err} / ${JSON.stringify(err.message)}"`);
            if ((adapter._client.request.getHostsAvailable().length === 0) || (err.message && err.message === 'timeout')) {
                reconnect(adapter);
                addPointToSeriesBuffer(adapter, pointId, point);
            } else if (err.message && typeof err.message === 'string' && err.message.includes('field type conflict')) {
                // retry write after type correction for some easy cases
                let retry = false;
                if (adapter._influxDPs[pointId] && adapter._influxDPs[pointId][adapter.namespace] && !adapter._influxDPs[pointId][adapter.namespace].storageType) {
                    let convertDirection = '';
                    if (err.message.includes('is type bool, already exists as type float') ||
                        err.message.includes('is type boolean, already exists as type float')) {
                        convertDirection = 'bool -> float';
                        if (point.value === true) {
                            point.value = 1;
                            retry = true;
                        }
                        else if (point.value === false) {
                            point.value = 0;
                            retry = true;
                        }
                        adapter._influxDPs[pointId][adapter.namespace].storageType = 'Number';
                        adapter._influxDPs[pointId].storageTypeAdjustedInternally = true;
                    }
                    else if (err.message.includes('is type float, already exists as type bool') ||
                        err.message.includes('is type float64, already exists as type bool')) {
                        convertDirection = 'float -> bool';
                        if (point.value === 1) {
                            point.value = true;
                            retry = true;
                        }
                        else if (point.value === 0) {
                            point.value = false;
                            retry = true;
                        }
                        adapter._influxDPs[pointId][adapter.namespace].storageType = 'Boolean';
                        adapter._influxDPs[pointId].storageTypeAdjustedInternally = true;
                    }
                    else if (err.message.includes(', already exists as type string')) {
                        point.value = point.value.toString();
                        retry = true;
                        adapter._influxDPs[pointId][adapter.namespace].storageType = 'String';
                        adapter._influxDPs[pointId].storageTypeAdjustedInternally = true;
                    }
                    if (retry) {
                        adapter.log.info(`Try to convert ${convertDirection} and re-write for ${pointId} and set storageType to ${adapter._influxDPs[pointId][adapter.namespace].storageType}`);
                        writeOnePointForID(adapter, pointId, point, true, cb);
                        const obj = {};
                        obj.common = {};
                        obj.common.custom = {};
                        obj.common.custom[adapter.namespace] = {};
                        obj.common.custom[adapter.namespace].storageType = adapter._influxDPs[pointId][adapter.namespace].storageType;
                        adapter.extendForeignObject(pointId, obj, err => {
                            if (err) {
                                adapter.log.error(`error updating history config for ${pointId} to pin datatype: ${err}`);
                            } else {
                                adapter.log.info('changed history configuration to pin detected datatype for ' + pointId);
                            }
                        });
                    }
                }
                if (!directWrite || !retry) {
                    // remember this as a pot. conflicting point and write synchronous
                    adapter._conflictingPoints[pointId]=1;
                    adapter.log.warn(`Add ${pointId} to conflicting Points (${Object.keys(adapter._conflictingPoints).length} now)`);
                }
            } else {
                if (!adapter._errorPoints[pointId]) {
                    adapter._errorPoints[pointId] = 1;
                } else {
                    adapter._errorPoints[pointId]++;
                }
                if (adapter._errorPoints[pointId] < 10) {
                    // re-add that point to buffer to try to write again
                    adapter.log.info(`Add point that had error for ${pointId} to buffer again, error-count=${adapter._errorPoints[pointId]}`);
                    addPointToSeriesBuffer(adapter, pointId, point);
                } else {
                    adapter.log.info(`Discard point that had error for ${pointId}, error-count=${adapter._errorPoints[pointId]}`);
                    adapter._errorPoints[pointId] = 0;
                }
            }
        } else {
            setConnected(adapter, true);
        }
        cb && cb();
    });
}

function writeFileBufferToDisk() {
    // write buffered values into cache file to process it by next start
    const fileData = {};
    if (adapter._seriesBufferCounter) {
        fileData.seriesBufferCounter = adapter._seriesBufferCounter;
        fileData.seriesBuffer        = adapter._seriesBuffer;
        fileData.conflictingPoints   = adapter._conflictingPoints;
        try {
            fs.writeFileSync(cacheFile, JSON.stringify(fileData), 'utf-8');
            adapter.log.warn(`Store data for ${fileData.seriesBufferCounter} points and ${Object.keys(fileData.conflictingPoints).length} conflicts`);
        }
        catch (err) {
            adapter.log.warn('Could not save non-stored data to file: ' + err);
        }
    }
    adapter._seriesBufferCounter = null;
}

function finish(adapter, callback) {
    adapter.setState && adapter.setState('info.connection', false, true);

    if (!adapter._subscribeAll) {
        // unsubscribe
        for (const _id in adapter._influxDPs) {
            adapter.unsubscribeForeignStates(adapter._influxDPs[_id].realId);
        }
    } else {
        adapter._subscribeAll = false;
        adapter.unsubscribeForeignStates('*');
    }

    if (adapter._reconnectTimeout) {
        clearTimeout(adapter._reconnectTimeout);
        adapter._reconnectTimeout = null;
    }
    if (adapter._pingInterval) {
        clearInterval(adapter._pingInterval);
        adapter._pingInterval = null;
    }
    if (adapter._finished) {
        callback && callback();
        return;
    }
    adapter._finished = true;
    if (adapter._seriesBufferChecker) {
        clearInterval(adapter._seriesBufferChecker);
        adapter._seriesBufferChecker = null;
    }
    let count = 0;
    for (const id in adapter._influxDPs) {
        if (!adapter._influxDPs.hasOwnProperty(id)) {
            continue;
        }

        if (adapter._influxDPs[id].relogTimeout) {
            clearTimeout(adapter._influxDPs[id].relogTimeout);
            adapter._influxDPs[id].relogTimeout = null;
        }
        if (adapter._influxDPs[id].timeout) {
            clearTimeout(adapter._influxDPs[id].timeout);
            adapter._influxDPs[id].timeout = null;
        }

/*        let tmpState;
        if (Object.assign) {
            tmpState = Object.assign({}, adapter._influxDPs[id].state);
        }
        else {
            tmpState = JSON.parse(JSON.stringify(adapter._influxDPs[id].state));
        }
        const state = adapter._influxDPs[id].state ? tmpState : null;
*/
        if (adapter._influxDPs[id].skipped) {
            count++;
            adapter._influxDPs[id].state = adapter._influxDPs[id].skipped;
            pushHelper(adapter, id, () => {
                if (!--count) {
                    writeFileBufferToDisk();
                    if (callback) {
                        callback();
                        callback = null;
                    } else {
                        adapter.terminate ? adapter.terminate() : process.exit();
                    }
                }
            });
            adapter._influxDPs[id].skipped = null;
        }
    }

    if (!count) {
        writeFileBufferToDisk();
        if (callback) {
            callback();
        } else {
            adapter.terminate ? adapter.terminate() : process.exit();
        }
    }
}

function interpolateData(itemOne, itemTwo, ts) {
    return {'val' : itemOne.val + (itemTwo.val - itemOne.val)/(itemTwo.ts - itemOne.ts)*(ts - itemOne.ts), 'ts': ts};
}

function getHistory(adapter, msg) {
    const options = {
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

    if (options.id && adapter._aliasMap[options.id]) {
        options.id = adapter._aliasMap[options.id];
    }

    let query = 'SELECT';
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

    query += ` from "${options.id}"`;

    if (!adapter._influxDPs[options.id]) {
        adapter.sendTo(msg.from, msg.command, {
            result: [],
            step:   0,
            error:  'No connection'
        }, msg.callback);
        return;
    }

    if (options.start > options.end) {
        const _end = options.end;
        options.end   = options.start;
        options.start = _end;
    }
    // if less 2000.01.01 00:00:00
    if (options.end   && options.end   < 946681200000) options.end   *= 1000;
    if (options.start && options.start < 946681200000) options.start *= 1000;

    if (!options.start && !options.count) {
        options.start = options.end - 86400000; // - 1 day
    }

    // query one timegroup-value more than requested originally at start and end
    // to make sure to have no 0 values because of the way InfluxDB does group by time
    if (options.aggregate !== 'onchange' && options.aggregate !== 'none' && options.aggregate !== 'minmax') {
        if (!options.step) {
            // calculate "step" based on difference between start and end using count
            options.step = parseInt((options.end - options.start) / options.count, 10);
        }
        if (options.start) {
            options.start -= options.step;
        }
        options.end += options.step;
        options.limit += 2;
    }

    query += ` WHERE `;
    if (options.start) {
        query += ` time > '${new Date(options.start).toISOString()}' AND `;
    }
    query += ` time < '${new Date(options.end).toISOString()}'`;

    if (!options.start && (options.count || options.limit)) {
        query += ` ORDER BY time DESC`;
    }

    if (options.aggregate !== 'onchange' && options.aggregate !== 'none' && options.aggregate !== 'minmax') {
        query += ` GROUP BY time(${options.step}ms) fill(previous) LIMIT ${options.limit}`;
    } else if (options.aggregate !== 'minmax') {
        query += ` LIMIT ${options.count}`;
    }

    // select one datapoint more than wanted
    if (options.aggregate === 'minmax' || options.aggregate === 'onchange' || options.aggregate === 'none') {
        let addQuery = '';
        if (options.start) {
            addQuery = `SELECT value from "${options.id}" WHERE time <= '${new Date(options.start).toISOString()}' ORDER BY time DESC LIMIT 1;`;
            query = addQuery + query;
        }
        addQuery = `;SELECT value from "${options.id}" WHERE time >= '${new Date(options.end).toISOString()}' LIMIT 1`;
        query = query + addQuery;
    }

    adapter.log.debug(query);

    // if specific id requested
    adapter._client.query(query, (err, rows) => {
        if (err) {
            if (adapter._client.request.getHostsAvailable().length === 0) {
                setConnected(adapter, false);
            }
            adapter.log.error('getHistory: ' + err);
        } else {
            setConnected(adapter, true);
        }

        adapter.log.debug('ROWS:' + JSON.stringify(rows));

        let result = [];

        if (rows && rows.length) {
            for (let qr = 0; qr < rows.length; qr++) {
                for (let rr = 0; rr < rows[qr].length; rr++) {
                    if ((rows[qr][rr].val === undefined) && (rows[qr][rr].value !== undefined)) {
                        rows[qr][rr].val = rows[qr][rr].value;
                        delete rows[qr][rr].value;
                    }
                    rows[qr][rr].ts  = new Date(rows[qr][rr].time).getTime();
                    delete rows[qr][rr].time;
                    if (rows[qr][rr].val !== null) {
                        const f = parseFloat(rows[qr][rr].val);
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
        
        if ((result.length > 2) && ((options.aggregate === 'minmax' || options.aggregate === 'onchange' || options.aggregate === 'none'))) {
            if (options.start ) {
                const startTime = new Date(options.start).getTime();
                if (startTime > result[0].ts) {
                    result[0]= {...result[0], ...interpolateData(result[0], result[1], startTime)};
                }
            }
            const endTime = new Date(options.end).getTime();
            if ( result[result.length - 1].ts > endTime ) {
                result[result.length - 1] = {...result[result.length - 1], ...interpolateData(result[result.length - 2], result[result.length - 1], endTime)};
            }   
        }

        if (result.length > 0 && options.aggregate === 'minmax') {
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

function getHistoryIflx2(adapter, msg) {
    const options = {
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

    if (options.id && adapter._aliasMap[options.id]) {
        options.id = adapter._aliasMap[options.id];
    }
    const fluxQueries = [];
    let fluxQuery = `from(bucket: "${adapter.config.dbname}") `;

    const valueColumn = adapter.config.usetags ? '_value' : 'value';

    if (!adapter._influxDPs[options.id]) {
        return adapter.sendTo(msg.from, msg.command, {
            result: [],
            step:   0,
            error:  'No connection'
        }, msg.callback);
    }

    if (options.start > options.end) {
        const _end = options.end;
        options.end   = options.start;
        options.start = _end;
    }
    // if less 2000.01.01 00:00:00
    if (options.end   && options.end   < 946681200000) options.end   *= 1000;
    if (options.start && options.start < 946681200000) options.start *= 1000;

    if (!options.start && !options.count) {
        options.start = options.end - 86400000; // - 1 day
    }

    // query one timegroup-value more than requested originally at start and end
    // to make sure to have no 0 values because of the way InfluxDB does group by time
    if (options.aggregate !== 'onchange' && options.aggregate !== 'none' && options.aggregate !== 'minmax') {
        if (!options.step) {
            // calculate "step" based on difference between start and end using count
            options.step = parseInt((options.end - options.start) / options.count, 10);
        }
        if (options.start) options.start -= options.step;
        options.end += options.step;
        options.limit += 2;
    }

    fluxQuery += ` |> range(${(options.start) ? "start: " + new Date(options.start).toISOString() + ", " : "start: -" + adapter.config.retention + "ms, "}stop: ${new Date(options.end).toISOString()})`;
    fluxQuery += ` |> filter(fn: (r) => r["_measurement"] == "${options.id}")`;

    if (adapter.config.usetags)
        fluxQuery += ' |> duplicate(column: "_value", as: "value")';
    else
        fluxQuery += ' |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")';

    if (!options.start && (options.count || options.limit)) {
        fluxQuery += ` |> sort(columns:["_time"], desc: true)`;
    }

    if (options.aggregate !== 'onchange' && options.aggregate !== 'none' && options.aggregate !== 'minmax') {
        if ((options.step !== null) && (options.step > 0))
            fluxQuery += ` |> window(every: ${options.step}ms)`;
        fluxQuery += `|> fill(column: "${valueColumn}", usePrevious: true)`;
    } else if (options.aggregate !== 'minmax') {
        fluxQuery += ` |> group() |> limit(n: ${options.count})`;
    }

    // Workaround to detect if measurement is of type bool (to skip non-sensual aggregation options)
    // There seems to be no officially supported way to detect this, so we check it by forcing a type-conflict
    const booleanTypeCheckQuery = `
        from(bucket: "${adapter.config.dbname}")
        |> range(${(options.start) ? `start: ${new Date(options.start).toISOString()}, ` : `start: -${adapter.config.retention}ms, `}stop: ${new Date(options.end).toISOString()})
        |> filter(fn: (r) => r["_measurement"] == "${options.id}" and contains(value: r._value, set: [true, false]))
    `;

    adapter._client.query(booleanTypeCheckQuery, (error, _rslt) => {
        let isBoolean;
        if (error) {
            if (error.message.match('.*type conflict: bool.*')) {
                isBoolean = false;
            } else {
                return adapter.sendTo(msg.from, msg.command, {
                    result:     [],
                    error:      error,
                    sessionId:  options.sessionId
                }, msg.callback);
            }
        } else {
            isBoolean = true;
            adapter.log.debug(`Measurement ${options.id} is of type Boolean - skipping aggregation options`);
        }

        if (options.step && !isBoolean) {

            switch (options.aggregate) {
                case 'average':
                    fluxQuery += ` |> mean(column: "${valueColumn}")`;
                    break;

                case 'max':
                    fluxQuery += ` |> max(column: "${valueColumn}")`;
                    break;

                case 'min':
                    fluxQuery += ` |> min(column: "${valueColumn}")`;
                    break;

                case 'total':
                    fluxQuery += ` |> sum(column: "${valueColumn}")`;
                    break;

                case 'count':
                    fluxQuery += ` |> count(column: "${valueColumn}")`;
                    break;

                default:
                    fluxQuery += ` |> mean(column: "${valueColumn}")`;
                    break;
            }
        }

        fluxQueries.push(fluxQuery);

        // select one datapoint more than wanted
        if (options.aggregate === 'minmax' || options.aggregate === 'onchange' || options.aggregate === 'none') {
            let addFluxQuery = '';
            if (options.start) {
                // get one entry "before" the defined timeframe for displaying purposes
                addFluxQuery = `from(bucket: "${adapter.config.dbname}") 
                |> range(start: ${new Date(options.start - (adapter.config.retention || 31536000) * 1000).toISOString()}, stop: ${new Date(options.start).toISOString()}) 
                |> filter(fn: (r) => r["_measurement"] == "${options.id}") 
                ${(!adapter.config.usetags) ? '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")' : ''}
                |> sort(columns: ["_time"], desc: true) 
                |> group() 
                |> limit(n: 1)`;

                const mainQuery = fluxQueries.pop();
                fluxQueries.push(addFluxQuery);
                fluxQueries.push(mainQuery);
            }
            // get one entry "after" the defined timeframe for displaying purposes
            addFluxQuery = `from(bucket: "${adapter.config.dbname}") 
                |> range(start: ${new Date(options.end).toISOString()}) 
                |> filter(fn: (r) => r["_measurement"] == "${options.id}") 
                ${(!adapter.config.usetags) ? '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")' : ''}
                |> sort(columns: ["_time"], desc: false) 
                |> group() 
                |> limit(n: 1)`;
            //fluxQuery = fluxQuery + addFluxQuery;
            fluxQueries.push(addFluxQuery);
        }

        adapter.log.debug('History-queries to execute: ' + fluxQueries);

        // if specific id requested
        adapter._client.queries(fluxQueries, (err, rows) => {
            if (err && !rows) {
                if (adapter._client.request.getHostsAvailable().length === 0) {
                    setConnected(adapter, false);
                }
                adapter.log.error('getHistory: ' + err);
            } else {
                setConnected(adapter, true);
            }

            adapter.log.debug('Parsing retrieved rows:' + JSON.stringify(rows));

            let result = [];

            if (rows && rows.length) {
                for (let qr = 0; qr < rows.length; qr++) {
                    for (let rr = 0; rr < rows[qr].length; rr++) {
                        if ((rows[qr][rr].val === undefined) && (rows[qr][rr].value !== undefined)) {
                            rows[qr][rr].val = rows[qr][rr].value;
                            delete rows[qr][rr].value;
                        }
                        rows[qr][rr].ts  = new Date(rows[qr][rr].time).getTime();
                        delete rows[qr][rr].time;
                        if (rows[qr][rr].val !== null) {
                            const f = parseFloat(rows[qr][rr].val);
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

            if ((result.length > 2) && ((options.aggregate === 'minmax' || options.aggregate === 'onchange' || options.aggregate === 'none'))) {
                if (options.start ) {
                    const startTime = new Date(options.start).getTime();
                    if (startTime > result[0].ts) {
                        result[0]= {...result[0], ...interpolateData(result[0], result[1], startTime)};
                    }
                }
                const endTime = new Date(options.end).getTime();
                if ( result[result.length - 1].ts > endTime ) {
                    result[result.length - 1] = {...result[result.length - 1], ...interpolateData(result[result.length - 2], result[result.length - 1], endTime)};
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
    });
}

function query(adapter, msg) {
    if (adapter._client) {
        const query = msg.message.query || msg.message;

        if (!query || typeof query !== 'string') {
          adapter.log.error('query missing: ' + query);
          adapter.sendTo(msg.from, msg.command, {
              result: [],
              error:  'Query missing'
          }, msg.callback);
          return;
        }

        adapter.log.debug('query: ' + query);

        adapter._client.query(query, (err, rows) => {
            if (err) {
                if (adapter._client.request.getHostsAvailable().length === 0) {
                    setConnected(adapter, false);
                }
                adapter.log.error('query: ' + err);
                return adapter.sendTo(msg.from, msg.command, {
                        result: [],
                        error:  'Invalid call'
                    }, msg.callback);
            } else {
                setConnected(adapter, true);
            }

            adapter.log.debug('result: ' + JSON.stringify(rows));

            for (let r = 0, l = rows.length; r < l; r++) {
                for (let rr = 0, ll = rows[r].length; rr < ll; rr++) {
                    if (rows[r][rr].time) {
                        rows[r][rr].ts = new Date(rows[r][rr].time).getTime();
                        delete rows[r][rr].time;
                    }
                }
            }

            adapter.sendTo(msg.from, msg.command, {
                result: rows,
                ts:     Date.now(),
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

function multiQuery(adapter, msg) {
    if (adapter._client) {
        const queriesString = msg.message.query || msg.message;

        let queries;
        try {
            //parse queries to array
            queries = queriesString.split(';');

            let c = 1;
            for (const query of queries) {
                if (!query || typeof query !== 'string')  {
                    throw {
                        name: 'Exception',
                        message: `Array element #${c}: Query messing`,
                        toString: function () { return `${this.name}: ${this.message}`; }
                    }
                }
                c++;
            }
        } catch (error) {
            adapter.log.warn('Error in received multiQuery: ' + error);
            adapter.sendTo(msg.from, msg.command, {
                result: [],
                error:  error
            }, msg.callback);
            return;
        }
        adapter.log.debug('queries: ' + queries);

        adapter._client.queries(queries, (err, rows) => {
            if (err) {
                if (adapter._client.request.getHostsAvailable().length === 0) {
                    setConnected(adapter, false);
                }
                adapter.log.error('queries: ' + err);
                return adapter.sendTo(msg.from, msg.command, {
                    result: [],
                    error:  'Invalid call'
                }, msg.callback);
            } else {
                setConnected(adapter, true);
            }

            adapter.log.debug('result: ' + JSON.stringify(rows));

            for (let r = 0, l = rows.length; r < l; r++) {
                for (let rr = 0, ll = rows[r].length; rr < ll; rr++) {
                    if (rows[r][rr].time) {
                        rows[r][rr].ts = new Date(rows[r][rr].time).getTime();
                        delete rows[r][rr].time;
                    }
                }
            }

            adapter.sendTo(msg.from, msg.command, {
                result: rows,
                ts:     Date.now(),
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

function storeState(adapter, msg) {
    if (!msg.message || !msg.message.id || !msg.message.state) {
        adapter.log.error('storeState called with invalid data');
        adapter.sendTo(msg.from, msg.command, {
            error: 'Invalid call'
        }, msg.callback);
        return;
    }

    let id;
    if (Array.isArray(msg.message)) {
        adapter.log.debug(`storeState: store ${msg.message.length} states for multiple ids`);
        for (let i = 0; i < msg.message.length; i++) {
            id = adapter._aliasMap[msg.message[i].id] ? adapter._aliasMap[msg.message[i].id] : msg.message[i].id;
            pushValueIntoDB(adapter, id, msg.message[i].state);
        }
    } else if (Array.isArray(msg.message.state)) {
        adapter.log.debug(`storeState: store ${msg.message.state.length} states for ${msg.message.id}`);
        for (let j = 0; j < msg.message.state.length; j++) {
            id = adapter._aliasMap[msg.message.id] ? adapter._aliasMap[msg.message.id] : msg.message.id;
            pushValueIntoDB(adapter, id, msg.message.state[j]);
        }
    } else {
        adapter.log.debug(`storeState: store 1 state for ${msg.message.id}`);
        id = adapter._aliasMap[msg.message.id] ? adapter._aliasMap[msg.message.id] : msg.message.id;
        pushValueIntoDB(adapter, id, msg.message.state);
    }

    adapter.sendTo(msg.from, msg.command, {
        success:                  true,
        connected:                adapter._connected,
        seriesBufferCounter:      adapter._seriesBufferCounter,
        seriesBufferFlushPlanned: adapter._seriesBufferFlushPlanned
    }, msg.callback);
}

function enableHistory(adapter, msg) {
    if (!msg.message || !msg.message.id) {
        adapter.log.error('enableHistory called with invalid data');
        adapter.sendTo(msg.from, msg.command, {error: 'Invalid call'}, msg.callback);
        return;
    }
    const obj = {};
    obj.common = {};
    obj.common.custom = {};
    if (msg.message.options) {
        obj.common.custom[adapter.namespace] = msg.message.options;
    } else {
        obj.common.custom[adapter.namespace] = {};
    }
    obj.common.custom[adapter.namespace].enabled = true;
    adapter.extendForeignObject(msg.message.id, obj, error => {
        if (error) {
            adapter.log.error('enableHistory: ' + error);
            adapter.sendTo(msg.from, msg.command, {error}, msg.callback);
        } else {
            adapter.log.info(JSON.stringify(obj));
            adapter.sendTo(msg.from, msg.command, {success: true}, msg.callback);
        }
    });
}

function disableHistory(adapter, msg) {
    if (!msg.message || !msg.message.id) {
        adapter.log.error('disableHistory called with invalid data');
        adapter.sendTo(msg.from, msg.command, {error: 'Invalid call'}, msg.callback);
        return;
    }
    const obj = {};
    obj.common = {};
    obj.common.custom = {};
    obj.common.custom[adapter.namespace] = {};
    obj.common.custom[adapter.namespace].enabled = false;
    adapter.extendForeignObject(msg.message.id, obj, error => {
        if (error) {
            adapter.log.error('disableHistory: ' + error);
            adapter.sendTo(msg.from, msg.command, {error}, msg.callback);
        } else {
            adapter.log.info(JSON.stringify(obj));
            adapter.sendTo(msg.from, msg.command, {success: true}, msg.callback);
        }
    });
}

function getEnabledDPs(adapter, msg) {
    const data = {};
    for (const id in adapter._influxDPs) {
        if (!adapter._influxDPs.hasOwnProperty(id)) {
            continue;
        }
        data[adapter._influxDPs[id].realId] = adapter._influxDPs[id][adapter.namespace];
    }

    adapter.sendTo(msg.from, msg.command, data, msg.callback);
}

// If started as allInOne mode => return function to create instance
if (module && module.parent) {
    module.exports = startAdapter;
} else {
    // or start the instance directly
    startAdapter();
}
