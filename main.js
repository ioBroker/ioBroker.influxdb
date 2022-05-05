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
const dataDir     = path.normalize(`${utils.controllerDir}/${require(`${utils.controllerDir}/lib/tools`).getDefaultDataDir()}`);
const cacheFile   = `${dataDir}influxdata.json`;

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
                    adapter.log.warn(`Ignoring Alias-ID because identical to ID for ${id}`);
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

            // debounceTime and debounce compatibility handling
            if (!obj.common.custom[adapter.namespace].blockTime && obj.common.custom[adapter.namespace].blockTime !== '0' && obj.common.custom[adapter.namespace].blockTime !== 0) {
                if (!obj.common.custom[adapter.namespace].debounce && obj.common.custom[adapter.namespace].debounce !== '0' && obj.common.custom[adapter.namespace].debounce !== 0) {
                    obj.common.custom[adapter.namespace].blockTime = parseInt(adapter.config.blockTime, 10) || 0;
                } else {
                    obj.common.custom[adapter.namespace].blockTime = parseInt(obj.common.custom[adapter.namespace].debounce, 10) || 0;
                }
            } else {
                obj.common.custom[adapter.namespace].blockTime = parseInt(obj.common.custom[adapter.namespace].blockTime, 10) || 0;
            }
            if (!obj.common.custom[adapter.namespace].debounceTime && obj.common.custom[adapter.namespace].debounceTime !== '0' && obj.common.custom[adapter.namespace].debounceTime !== 0) {
                obj.common.custom[adapter.namespace].debounceTime = parseInt(adapter.config.debounceTime, 10) || 0;
            } else {
                obj.common.custom[adapter.namespace].debounceTime = parseInt(obj.common.custom[adapter.namespace].debounceTime, 10) || 0;
            }

            // changesOnly
            obj.common.custom[adapter.namespace].changesOnly = obj.common.custom[adapter.namespace].changesOnly === 'true' || obj.common.custom[adapter.namespace].changesOnly === true;

            // ignoreZero
            obj.common.custom[adapter.namespace].ignoreZero = obj.common.custom[adapter.namespace].ignoreZero === 'true' || obj.common.custom[adapter.namespace].ignoreZero === true;

            // round
            if (obj.common.custom[adapter.namespace].round !== null && obj.common.custom[adapter.namespace].round !== undefined && obj.common.custom[adapter.namespace] !== '') {
                obj.common.custom[adapter.namespace].round = parseInt(obj.common.custom[adapter.namespace], 10);
                if (!isFinite(obj.common.custom[adapter.namespace].round) || obj.common.custom[adapter.namespace].round < 0) {
                    obj.common.custom[adapter.namespace].round = adapter.config.round;
                } else {
                    obj.common.custom[adapter.namespace].round = Math.pow(10, parseInt(obj.common.custom[adapter.namespace].round, 10));
                }
            } else {
                obj.common.custom[adapter.namespace].round = adapter.config.round;
            }

            // ignoreAboveNumber
            if (obj.common.custom[adapter.namespace].ignoreAboveNumber !== undefined && obj.common.custom[adapter.namespace].ignoreAboveNumber !== null && obj.common.custom[adapter.namespace].ignoreAboveNumber !== '') {
                obj.common.custom[adapter.namespace].ignoreAboveNumber = parseFloat(obj.common.custom[adapter.namespace].ignoreAboveNumber) || null;
            }

            // ignoreBelowNumber incl. ignoreBelowZero compatibility handling
            if (obj.common.custom[adapter.namespace].ignoreBelowNumber !== undefined && obj.common.custom[adapter.namespace].ignoreBelowNumber !== null && obj.common.custom[adapter.namespace].ignoreBelowNumber !== '') {
                obj.common.custom[adapter.namespace].ignoreBelowNumber = parseFloat(obj.common.custom[adapter.namespace].ignoreBelowNumber) || null;
            } else if (obj.common.custom[adapter.namespace].ignoreBelowZero === 'true' || obj.common.custom[adapter.namespace].ignoreBelowZero === true) {
                obj.common.custom[adapter.namespace].ignoreBelowNumber = 0;
            }

            // disableSkippedValueLogging
            if (obj.common.custom[adapter.namespace].disableSkippedValueLogging !== undefined && obj.common.custom[adapter.namespace].disableSkippedValueLogging !== null && obj.common.custom[adapter.namespace].disableSkippedValueLogging !== '') {
                obj.common.custom[adapter.namespace].disableSkippedValueLogging = obj.common.custom[adapter.namespace].disableSkippedValueLogging === 'true' || obj.common.custom[adapter.namespace].disableSkippedValueLogging === true;
            } else {
                obj.common.custom[adapter.namespace].disableSkippedValueLogging = adapter.config.disableSkippedValueLogging;
            }

            // enableDebugLogs
            if (obj.common.custom[adapter.namespace].enableDebugLogs !== undefined && obj.common.custom[adapter.namespace].enableDebugLogs !== null && obj.common.custom[adapter.namespace].enableDebugLogs !== '') {
                obj.common.custom[adapter.namespace].enableDebugLogs = obj.common.custom[adapter.namespace].enableDebugLogs === 'true' || obj.common.custom[adapter.namespace].enableDebugLogs === true;
            } else {
                obj.common.custom[adapter.namespace].enableDebugLogs = adapter.config.enableDebugLogs;
            }

            // changesRelogInterval
            if (obj.common.custom[adapter.namespace].changesRelogInterval || obj.common.custom[adapter.namespace].changesRelogInterval === 0) {
                obj.common.custom[adapter.namespace].changesRelogInterval = parseInt(obj.common.custom[adapter.namespace].changesRelogInterval, 10) || 0;
            } else {
                obj.common.custom[adapter.namespace].changesRelogInterval = adapter.config.changesRelogInterval;
            }

            // changesMinDelta
            if (obj.common.custom[adapter.namespace].changesMinDelta || obj.common.custom[adapter.namespace].changesMinDelta === 0) {
                obj.common.custom[adapter.namespace].changesMinDelta = parseFloat(obj.common.custom[adapter.namespace].changesMinDelta.toString().replace(/,/g, '.')) || 0;
            } else {
                obj.common.custom[adapter.namespace].changesMinDelta = adapter.config.changesMinDelta;
            }

            // storageType
            if (!obj.common.custom[adapter.namespace].storageType) {
                obj.common.custom[adapter.namespace].storageType = false;
            }


            if (adapter._influxDPs[formerAliasId] && !adapter._influxDPs[formerAliasId].storageTypeAdjustedInternally && adapter._influxDPs[formerAliasId][adapter.namespace] && isEqual(obj.common.custom[adapter.namespace], adapter._influxDPs[formerAliasId][adapter.namespace])) {
                obj.common.custom[adapter.namespace].enableDebugLogs && adapter.log.debug(`Object ${id} unchanged. Ignore`);
                return;
            }

            // relogTimeout
            if (adapter._influxDPs[formerAliasId] && adapter._influxDPs[formerAliasId].relogTimeout) {
                clearTimeout(adapter._influxDPs[formerAliasId].relogTimeout);
                adapter._influxDPs[formerAliasId].relogTimeout = null;
            }

            const state = adapter._influxDPs[formerAliasId] ? adapter._influxDPs[formerAliasId].state : null;
            const skipped = adapter._influxDPs[formerAliasId] ? adapter._influxDPs[formerAliasId].skipped : null;
            const timeout = adapter._influxDPs[formerAliasId] ? adapter._influxDPs[formerAliasId].timeout : null;

            adapter._influxDPs[id] = obj.common.custom;
            adapter._influxDPs[id].realId = realId;
            adapter._influxDPs[id].state = state;
            adapter._influxDPs[id].skipped = skipped;
            adapter._influxDPs[id].timeout = timeout;

            writeInitialValue(adapter, realId, id);

            adapter.log.info(`enabled logging of ${id}, Alias=${id !== realId}`);
        } else {
            if (adapter._aliasMap[id]) {
                adapter.log.debug(`Removed Alias: ${id} !-> ${adapter._aliasMap[id]}`);
                delete adapter._aliasMap[id];
            }

            id = formerAliasId;

            if (adapter._influxDPs[id] && adapter._influxDPs[id][adapter.namespace]) {
                adapter._influxDPs[id].relogTimeout && clearTimeout(adapter._influxDPs[id].relogTimeout);
                adapter._influxDPs[id].timeout && clearTimeout(adapter._influxDPs[id].timeout);

                delete adapter._influxDPs[id];
                adapter.log.info(`disabled logging of ${id}`);
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

function setConnected(adapter, isConnected) {
    if (adapter._connected !== isConnected) {
        adapter._connected = isConnected;
        adapter.setState('info.connection', adapter._connected, true, err =>
            // analyse if the state could be set (because of permissions)
            err ? adapter.log.error(`Can not update adapter._connected state: ${err}`) :
                adapter.log.debug(`connected set to ${adapter._connected}`));
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
        .then(hosts => {
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
    adapter.config.validateSSL = adapter.config.validateSSL !== undefined ? !!adapter.config.validateSSL :  true;

    adapter.config.seriesBufferMax = parseInt(adapter.config.seriesBufferMax, 10) || 0;

    adapter.log.info(`Influx DB Version used: ${adapter.config.dbversion}`);

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
                adapter.config.requestTimeout,
                adapter.config.validateSSL,
                adapter.config.usetags
            )
            break;
        case '1.x':
        default:
            if (/[\x00-\x08\x0E-\x1F\x80-\xFF]/.test(adapter.config.password)) {
                return adapter.log.error('Password error: Please re-enter the password in Admin. Stopping');
            }

            adapter._client = new DatabaseInfluxDB1x(
                adapter.log,
                adapter.config.host,
                adapter.config.port, // optional, default 8086
                adapter.config.protocol, // optional, default 'http'
                adapter.config.user,
                adapter.config.password,
                adapter.config.dbname,
                adapter.config.requestTimeout,
                'ms'
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
    adapter.log.debug(`testConnection msg-object: ${JSON.stringify(msg)}`);
    msg.message.config.port = parseInt(msg.message.config.port, 10) || 0;
    msg.message.config.requestTimeout = parseInt(msg.message.config.requestTimeout) || 30000;

    let timeout;
    try {
        timeout = setTimeout(() => {
            timeout = null;
            adapter.sendTo(msg.from, msg.command, {error: 'connect timeout'}, msg.callback);
        }, 5000);

        let lClient;
        adapter.log.debug(`TEST DB Version: ${msg.message.config.dbversion}`);
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
                    msg.message.config.dbname || appName,
                    msg.message.config.requestTimeout
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
                    msg.message.config.dbname || appName,
                    msg.message.config.requestTimeout
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
                    adapter.getForeignObject(`system.adapter.${adapter.namespace}`, (err, obj) => {
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
        // influxdb 1
        if (adapter.config.dbversion === '1.x') {
            adapter.sendTo(msg.from, msg.command, {supportedFeatures: ['update', 'delete', 'deleteRange', 'deleteAll', 'storeState']}, msg.callback);
        } else {
            // not yet implemented
            adapter.sendTo(msg.from, msg.command, {supportedFeatures: ['storeState']}, msg.callback);
        }
    }
    else if (msg.command === 'update') {
        updateState(adapter, msg);
    } else if (msg.command === 'delete') {
        deleteState(adapter, msg);
    } else if (msg.command === 'deleteAll') {
        deleteStateAll(adapter, msg);
    } else if (msg.command === 'deleteRange') {
        deleteState(adapter, msg);
    } else if (msg.command === 'storeState') {
        storeState(adapter, msg);
    } else if (msg.command === 'getHistory') {
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

    if (adapter.config.round !== null && adapter.config.round !== undefined && adapter.config.round !== '') {
        adapter.config.round = parseInt(adapter.config.round, 10);
        if (!isFinite(adapter.config.round) || adapter.config.round < 0) {
            adapter.config.round = null;
            adapter.log.info(`Invalid round value: ${adapter.config.round} - ignore, do not round values`);
        } else {
            adapter.config.round = Math.pow(10, parseInt(adapter.config.round, 10));
        }
    } else {
        adapter.config.round = null;
    }
    if (adapter.config.changesRelogInterval !== null && adapter.config.changesRelogInterval !== undefined) {
        adapter.config.changesRelogInterval = parseInt(adapter.config.changesRelogInterval, 10);
    } else {
        adapter.config.changesRelogInterval = 0;
    }

    adapter.config.seriesBufferFlushInterval = parseInt(adapter.config.seriesBufferFlushInterval, 10) || 600;

    adapter.config.requestTimeout = parseInt(adapter.config.requestTimeout, 10) || 30000;

    if (adapter.config.changesMinDelta !== null && adapter.config.changesMinDelta !== undefined) {
        adapter.config.changesMinDelta = parseFloat(adapter.config.changesMinDelta.toString().replace(/,/g, '.'));
    } else {
        adapter.config.changesMinDelta = 0;
    }

    if (adapter.config.blockTime !== null && adapter.config.blockTime !== undefined) {
        adapter.config.blockTime = parseInt(adapter.config.blockTime, 10) || 0;
    } else {
        if (adapter.config.debounce !== null && adapter.config.debounce !== undefined) {
            adapter.config.debounce = parseInt(adapter.config.debounce, 10) || 0;
        } else {
            adapter.config.blockTime = 0;
        }
    }

    if (adapter.config.debounceTime !== null && adapter.config.debounceTime !== undefined) {
        adapter.config.debounceTime = parseInt(adapter.config.debounceTime, 10) || 0;
    } else {
        adapter.config.debounceTime = 0;
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
        err && adapter.log.error(`main/getObjectView: ${err}`);
        let count = 0;
        if (doc && doc.rows) {
            const l = doc.rows.length;
            for (let i = 0; i < l; i++) {
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

                        // debounceTime and debounce compatibility handling
                        if (!adapter._influxDPs[id][adapter.namespace].blockTime && adapter._influxDPs[id][adapter.namespace].blockTime !== '0' && adapter._influxDPs[id][adapter.namespace].blockTime !== 0) {
                            if (!adapter._influxDPs[id][adapter.namespace].debounce && adapter._influxDPs[id][adapter.namespace].debounce !== '0' && adapter._influxDPs[id][adapter.namespace].debounce !== 0) {
                                adapter._influxDPs[id][adapter.namespace].blockTime = parseInt(adapter.config.blockTime, 10) || 0;
                            } else {
                                adapter._influxDPs[id][adapter.namespace].blockTime = parseInt(adapter._influxDPs[id][adapter.namespace].debounce, 10) || 0;
                            }
                        } else {
                            adapter._influxDPs[id][adapter.namespace].blockTime = parseInt(adapter._influxDPs[id][adapter.namespace].blockTime, 10) || 0;
                        }
                        if (!adapter._influxDPs[id][adapter.namespace].debounceTime && adapter._influxDPs[id][adapter.namespace].debounceTime !== '0' && adapter._influxDPs[id][adapter.namespace].debounceTime !== 0) {
                            adapter._influxDPs[id][adapter.namespace].debounceTime = parseInt(adapter.config.debounceTime, 10) || 0;
                        } else {
                            adapter._influxDPs[id][adapter.namespace].debounceTime = parseInt(adapter._influxDPs[id][adapter.namespace].debounceTime, 10) || 0;
                        }

                        adapter._influxDPs[id][adapter.namespace].changesOnly   = adapter._influxDPs[id][adapter.namespace].changesOnly   === 'true' || adapter._influxDPs[id][adapter.namespace].changesOnly === true;
                        adapter._influxDPs[id][adapter.namespace].ignoreZero    = adapter._influxDPs[id][adapter.namespace].ignoreZero    === 'true' || adapter._influxDPs[id][adapter.namespace].ignoreZero  === true;

                        // round
                        if (adapter._influxDPs[id][adapter.namespace].round !== null && adapter._influxDPs[id][adapter.namespace].round !== undefined && adapter._influxDPs[id][adapter.namespace] !== '') {
                            adapter._influxDPs[id][adapter.namespace].round = parseInt(adapter._influxDPs[id][adapter.namespace], 10);
                            if (!isFinite(adapter._influxDPs[id][adapter.namespace].round) || adapter._influxDPs[id][adapter.namespace].round < 0) {
                                adapter._influxDPs[id][adapter.namespace].round = adapter.config.round;
                            } else {
                                adapter._influxDPs[id][adapter.namespace].round = Math.pow(10, parseInt(adapter._influxDPs[id][adapter.namespace].round, 10));
                            }
                        } else {
                            adapter._influxDPs[id][adapter.namespace].round = adapter.config.round;
                        }

                        // ignoreAboveNumber
                        if (adapter._influxDPs[id][adapter.namespace].ignoreAboveNumber !== undefined && adapter._influxDPs[id][adapter.namespace].ignoreAboveNumber !== null && adapter._influxDPs[id][adapter.namespace].ignoreAboveNumber !== '') {
                            adapter._influxDPs[id][adapter.namespace].ignoreAboveNumber = parseFloat(adapter._influxDPs[id][adapter.namespace].ignoreAboveNumber) || null;
                        }

                        // ignoreBelowNumber and ignoreBelowZero compatibility handling
                        if (adapter._influxDPs[id][adapter.namespace].ignoreBelowNumber !== undefined && adapter._influxDPs[id][adapter.namespace].ignoreBelowNumber !== null && adapter._influxDPs[id][adapter.namespace].ignoreBelowNumber !== '') {
                            adapter._influxDPs[id][adapter.namespace].ignoreBelowNumber = parseFloat(adapter._influxDPs[id][adapter.namespace].ignoreBelowNumber) || null;
                        } else if (adapter._influxDPs[id][adapter.namespace].ignoreBelowZero === 'true' || adapter._influxDPs[id][adapter.namespace].ignoreBelowZero === true) {
                            adapter._influxDPs[id][adapter.namespace].ignoreBelowNumber = 0;
                        }

                        // disableSkippedValueLogging
                        if (adapter._influxDPs[id][adapter.namespace].disableSkippedValueLogging !== undefined && adapter._influxDPs[id][adapter.namespace].disableSkippedValueLogging !== null && adapter._influxDPs[id][adapter.namespace].disableSkippedValueLogging !== '') {
                            adapter._influxDPs[id][adapter.namespace].disableSkippedValueLogging = adapter._influxDPs[id][adapter.namespace].disableSkippedValueLogging === 'true' || adapter._influxDPs[id][adapter.namespace].disableSkippedValueLogging === true;
                        } else {
                            adapter._influxDPs[id][adapter.namespace].disableSkippedValueLogging = adapter.config.disableSkippedValueLogging;
                        }

                        // enableDebugLogs
                        if (adapter._influxDPs[id][adapter.namespace].enableDebugLogs !== undefined && adapter._influxDPs[id][adapter.namespace].enableDebugLogs !== null && adapter._influxDPs[id][adapter.namespace].enableDebugLogs !== '') {
                            adapter._influxDPs[id][adapter.namespace].enableDebugLogs = adapter._influxDPs[id][adapter.namespace].enableDebugLogs === 'true' || adapter._influxDPs[id][adapter.namespace].enableDebugLogs === true;
                        } else {
                            adapter._influxDPs[id][adapter.namespace].enableDebugLogs = adapter.config.enableDebugLogs;
                        }

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

                        adapter._influxDPs[id][adapter.namespace].storageType = adapter._influxDPs[id][adapter.namespace].storageType || false;

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
            state.from = `system.adapter.${adapter.namespace}`;
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
            if (isFinite(state.val)) {
                state.val = parseFloat(state.val);
            }
        }

        settings.enableDebugLogs && adapter.log.debug(`new value received for ${id} (storageType ${settings.storageType}), new-value=${state.val}, ts=${state.ts}, relog=${timerRelog}`);

        let ignoreDebonce = false;

        if (!timerRelog) {
            const valueUnstable = !!adapter._influxDPs[id].timeout;
            // When a debounce timer runs and the value is the same as the last one, ignore it
            if (adapter._influxDPs[id].timeout && state.ts !== state.lc) {
                settings.enableDebugLogs && adapter.log.debug(`value not changed debounce ${id}, value=${state.val}, ts=${state.ts}, debounce timer keeps running`);
                return;
            } else if (adapter._influxDPs[id].timeout) { // if value changed, clear timer
                settings.enableDebugLogs && adapter.log.debug(`value changed during debounce time ${id}, value=${state.val}, ts=${state.ts}, debounce timer restarted`);
                clearTimeout(adapter._influxDPs[id].timeout);
                adapter._influxDPs[id].timeout = null;
            }

            if (!valueUnstable && settings.blockTime && adapter._influxDPs[id].state && (adapter._influxDPs[id].state.ts + settings.blockTime) > state.ts) {
                settings.enableDebugLogs && adapter.log.debug(`value ignored blockTime ${id}, value=${state.val}, ts=${state.ts}, lastState.ts=${adapter._influxDPs[id].state.ts}, blockTime=${settings.blockTime}`);
                return;
            }

            if (settings.ignoreZero && (state.val === undefined || state.val === null || state.val === 0)) {
                settings.enableDebugLogs && adapter.log.debug(`value ignore because zero or null ${id}, new-value=${state.val}, ts=${state.ts}`);
                return;
            } else
            if (typeof settings.ignoreBelowNumber === 'number' && typeof state.val === 'number' && state.val < settings.ignoreBelowNumber) {
                settings.enableDebugLogs && adapter.log.debug(`value ignored because below ${settings.ignoreBelowNumber} for ${id}, new-value=${state.val}, ts=${state.ts}`);
                return;
            }
            if (typeof settings.ignoreAboveNumber === 'number' && typeof state.val === 'number' && state.val > settings.ignoreAboveNumber) {
                settings.enableDebugLogs && adapter.log.debug(`value ignored because above ${settings.ignoreAboveNumber} for ${id}, new-value=${state.val}, ts=${state.ts}`);
                return;
            }

            if (adapter._influxDPs[id].state && settings.changesOnly) {
                if (settings.changesRelogInterval === 0) {
                    if ((adapter._influxDPs[id].state.val !== null || state.val === null) && state.ts !== state.lc) {
                        // remember new timestamp
                        if (!valueUnstable && !settings.disableSkippedValueLogging) {
                            adapter._influxDPs[id].skipped = state;
                        }
                        settings.enableDebugLogs && adapter.log.debug(`value not changed ${id}, last-value=${adapter._influxDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
                        return;
                    }
                } else if (adapter._influxDPs[id].lastLogTime) {
                    if ((adapter._influxDPs[id].state.val !== null || state.val === null) && (state.ts !== state.lc) && (Math.abs(adapter._influxDPs[id].lastLogTime - state.ts) < settings.changesRelogInterval * 1000)) {
                        // remember new timestamp
                        if (!valueUnstable && !settings.disableSkippedValueLogging) {
                            adapter._influxDPs[id].skipped = state;
                        }
                        settings.enableDebugLogs && adapter.log.debug(`value not changed ${id}, last-value=${adapter._influxDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
                        return;
                    }
                    if (state.ts !== state.lc) {
                        settings.enableDebugLogs && adapter.log.debug(`value-not-changed-relog ${id}, value=${state.val}, lastLogTime=${adapter._influxDPs[id].lastLogTime}, ts=${state.ts}`);
                        ignoreDebonce = true;
                    }
                }
                if (typeof state.val === 'number') {
                    if (
                        adapter._influxDPs[id].state.val !== null &&
                        settings.changesMinDelta !== 0 &&
                        Math.abs(adapter._influxDPs[id].state.val - state.val) < settings.changesMinDelta
                    ) {
                        if (!valueUnstable && !settings.disableSkippedValueLogging) {
                            adapter._influxDPs[id].skipped = state;
                        }
                        settings.enableDebugLogs && adapter.log.debug(`Min-Delta not reached ${id}, last-value=${adapter._influxDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
                        return;
                    } else if (settings.changesMinDelta !== 0) {
                        settings.enableDebugLogs && adapter.log.debug(`Min-Delta reached ${id}, last-value=${adapter._influxDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
                    }
                } else {
                    settings.enableDebugLogs && adapter.log.debug(`Min-Delta ignored because no number ${id}, last-value=${adapter._influxDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
                }
            }
        }

        if (adapter._influxDPs[id].relogTimeout) {
            clearTimeout(adapter._influxDPs[id].relogTimeout);
            adapter._influxDPs[id].relogTimeout = null;
        }
        if (timerRelog) {
            state = Object.assign({}, state);
            state.ts = Date.now();
            state.from = `system.adapter.${adapter.namespace}`;
            settings.enableDebugLogs && adapter.log.debug(`timed-relog ${id}, value=${state.val}, lastLogTime=${adapter._influxDPs[id].lastLogTime}, ts=${state.ts}`);
            ignoreDebonce = true;
        } else {
            if (settings.changesOnly && adapter._influxDPs[id].skipped) {
                settings.enableDebugLogs && adapter.log.debug(`Skipped value logged ${id}, value=${adapter._influxDPs[id].skipped.val}, ts=${adapter._influxDPs[id].skipped.ts}`);
                pushHelper(adapter, id, adapter._influxDPs[id].skipped);
                adapter._influxDPs[id].skipped = null;
            }
            if (adapter._influxDPs[id].state && ((adapter._influxDPs[id].state.val === null && state.val !== null) || (adapter._influxDPs[id].state.val !== null && state.val === null))) {
                ignoreDebonce = true;
            } else if (!adapter._influxDPs[id].state && state.val === null) {
                ignoreDebonce = true;
            }
        }

        if (settings.debounceTime && !ignoreDebonce && !timerRelog) {
            // Discard changes in de-bounce time to store last stable value
            adapter._influxDPs[id].timeout && clearTimeout(adapter._influxDPs[id].timeout);
            adapter._influxDPs[id].timeout = setTimeout((id, state) => {
                adapter._influxDPs[id].timeout = null;
                adapter._influxDPs[id].state = state;
                adapter._influxDPs[id].lastLogTime = state.ts;
                settings.enableDebugLogs && adapter.log.debug(`Value logged ${id}, value=${adapter._influxDPs[id].state.val}, ts=${adapter._influxDPs[id].state.ts}`);
                pushHelper(adapter, id);
                if (settings.changesRelogInterval > 0) {
                    adapter._influxDPs[id].relogTimeout = setTimeout(reLogHelper, settings.changesRelogInterval * 1000, adapter, id);
                }
            }, settings.debounceTime, id, state);
        } else {
            if (!timerRelog) {
                adapter._influxDPs[id].state = state;
            }
            adapter._influxDPs[id].lastLogTime = state.ts;

            settings.enableDebugLogs && adapter.log.debug(`Value logged ${id}, value=${adapter._influxDPs[id].state.val}, ts=${adapter._influxDPs[id].state.ts}`);
            pushHelper(adapter, id, state);
            if (settings.changesRelogInterval > 0) {
                adapter._influxDPs[id].relogTimeout = setTimeout(reLogHelper, settings.changesRelogInterval * 1000, adapter, id);
            }
        }
    }
}

function reLogHelper(adapter, _id) {
    if (!adapter._influxDPs[_id]) {
        adapter.log.info(`non-existing id ${_id}`);
        return;
    }
    adapter._influxDPs[_id].relogTimeout = null;
    adapter._influxDPs[_id].relogTimeout = null;
    if (adapter._influxDPs[_id].skipped) {
        pushHistory(adapter, _id, adapter._influxDPs[_id].skipped, true);
    } else if (adapter._influxDPs[_id].state) {
        pushHistory(adapter, _id, adapter._influxDPs[_id].state, true);
    } else {        adapter.getForeignState(adapter._influxDPs[_id].realId, (err, state) => {
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

function pushHelper(adapter, _id, state, cb) {
    if (!adapter._influxDPs[_id] || !adapter._influxDPs[_id][adapter.namespace] || (!adapter._influxDPs[_id].state && !state)) {
        return cb && setImmediate(cb, `ID ${_id} not activated for logging`);
    }
    if (!state) {
        state = adapter._influxDPs[_id].state;
    }

    const _settings = adapter._influxDPs[_id][adapter.namespace];

    if (state.val === null) { // InfluxDB can not handle null values
        return cb && setImmediate(cb, `null value for ${_id} can not be handled`);
    }
    if (typeof state.val === 'number' && !isFinite(state.val)) { // InfluxDB can not handle Infinite values
        return cb && setImmediate(cb, `Non Finite value ${state.val} for ${_id} can not be handled`);
    }

    if (state.val !== null && (typeof state.val === 'object' || typeof state.val === 'undefined')) {
        state.val = JSON.stringify(state.val);
    }

    _settings.enableDebugLogs && adapter.log.debug(`Datatype ${_id}: Currently: ${typeof state.val}, StorageType: ${_settings.storageType}`);
    if (typeof state.val === 'string' && _settings.storageType !== 'String') {
        _settings.enableDebugLogs && adapter.log.debug(`Do Automatic Datatype conversion for ${_id}`);
        if (isFinite(state.val)) {
            state.val = parseFloat(state.val);
        } else if (state.val === 'true') {
            state.val = true;
        } else if (state.val === 'false') {
            state.val = false;
        }
    }
    if (_settings.storageType === 'String' && typeof state.val !== 'string') {
        state.val = state.val.toString();
    }
    else if (_settings.storageType === 'Number' && typeof state.val !== 'number') {
        if (typeof state.val === 'boolean') {
            state.val = state.val ? 1 : 0;
        }
        else {
            adapter.log.info(`Do not store value "${state.val}" for ${_id} because no number`);
            return cb && setImmediate(cb, `do not store value for ${_id} because no number`);
        }
    }
    else if (_settings.storageType === 'Boolean' && typeof state.val !== 'boolean') {
        state.val = !!state.val;
    }
    pushValueIntoDB(adapter, _id, state, () => cb && setImmediate(cb));
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

    state.ts = parseInt(state.ts, 10);

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

function storeBufferedSeries(adapter, id, cb) {
    if (typeof id === 'function') {
        cb = id;
        id = null;
    }
    if (id && (!adapter._seriesBuffer[id] || !adapter._seriesBuffer[id].length)) {
        return cb && cb();
    }
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

    if (id) {
        const idSeries = adapter._seriesBuffer[id];
        adapter._seriesBuffer[id] = [];
        adapter.log.debug(`Store ${idSeries.length} buffered influxDB history points for ${id}`);
        adapter._seriesBufferCounter -= idSeries.length;
        writeSeriesPerID(adapter, id, idSeries, cb);
        return;
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
            adapter.log.warn(`Error on writeSeries: ${err}`);
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
    writeSeriesPerID(adapter, id, series[id], () =>
        writeAllSeriesPerID(adapter, series, cb, idList));
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
            if (!adapter._client.request.getHostsAvailable().length || (err.message && err.message === 'timeout')) {
                reconnect(adapter);
                addPointToSeriesBuffer(adapter, pointId, point);
            } else if (err.message && typeof err.message === 'string' && err.message.includes('field type conflict')) {
                // retry write after type correction for some easy cases
                let retry = false;
                let adjustType = false;
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
                        adjustType = true;
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
                        adjustType = true;
                        adapter._influxDPs[pointId][adapter.namespace].storageType = 'Boolean';
                        adapter._influxDPs[pointId].storageTypeAdjustedInternally = true;
                    }
                    else if (err.message.includes(', already exists as type string')) {
                        point.value = point.value.toString();
                        retry = true;
                        adjustType = true;
                        adapter._influxDPs[pointId][adapter.namespace].storageType = 'String';
                        adapter._influxDPs[pointId].storageTypeAdjustedInternally = true;
                    } else if (err.message.includes('is type string, already exists as type float')) {
                        if (isFinite(point.value)) {
                            point.value = parseFloat(point.value);
                            retry = true;
                        }
                        adjustType = true;
                        adapter._influxDPs[pointId][adapter.namespace].storageType = 'Number';
                        adapter._influxDPs[pointId].storageTypeAdjustedInternally = true;
                    }
                    if (retry) {
                        adapter.log.info(`Try to convert ${convertDirection} and re-write for ${pointId} and set storageType to ${adapter._influxDPs[pointId][adapter.namespace].storageType}`);
                        writeOnePointForID(adapter, pointId, point, true, cb);
                    }
                    if (adjustType) {
                        const obj = {};
                        obj.common = {};
                        obj.common.custom = {};
                        obj.common.custom[adapter.namespace] = {};
                        obj.common.custom[adapter.namespace].storageType = adapter._influxDPs[pointId][adapter.namespace].storageType;
                        adapter.extendForeignObject(pointId, obj, err => {
                            if (err) {
                                adapter.log.error(`error updating history config for ${pointId} to pin datatype: ${err}`);
                            } else {
                                adapter.log.info(`changed history configuration to pin detected datatype for ${pointId}`);
                            }
                        });
                    }
                }
                if (!directWrite || !retry) {
                    // remember this as a pot. conflicting point and write synchronous
                    adapter._conflictingPoints[pointId] = 1;
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
            adapter.log.warn(`Could not save non-stored data to file: ${err}`);
        }
    }
    adapter._seriesBufferCounter = null;
}

function _delete(adapter, id, state, cb) {
    if (!adapter._connected) {
        return cb && cb('not connected');
    }

    if (adapter.config.dbversion === '1.x') {
        let query;
        if (state.ts) {
            query = `DELETE FROM "${id}" WHERE time = '${new Date(state.ts).toISOString()}'`;
        } else if (state.start) {
            query = `DELETE FROM "${id}" WHERE time >= '${new Date(state.start).toISOString()}'${state.end ? ` AND time <= '${new Date(state.end).toISOString()}'` : ''}`;
        } else if (state.end) {
            query = `DELETE FROM "${id}" WHERE time <= '${new Date(state.end).toISOString()}`;
        } else {
            query = `DELETE FROM "${id}" WHERE time >= '2000-01-01T00:00:00.000Z'`; // delete all
        }

        adapter._client.query(query, err => {
            if (err) {
                adapter.log.warn(`Error on delete("${query}): ${err} / ${JSON.stringify(err.message)}"`);
                cb && cb(err);
            } else {
                setConnected(adapter, true);
            }
            cb && cb();
        });
    } else {
        cb && cb('not implemented');
    }
}

function deleteState(adapter, msg) {
    if (!msg.message) {
        adapter.log.error('deleteState called with invalid data');
        return adapter.sendTo(msg.from, msg.command, {
            error: `Invalid call: ${JSON.stringify(msg)}`
        }, msg.callback);
    }
    let id;
    if (Array.isArray(msg.message)) {
        adapter.log.debug(`deleteState ${msg.message.length} items`);

        for (let i = 0; i < msg.message.length; i++) {
            id = adapter._aliasMap[msg.message[i].id] ? adapter._aliasMap[msg.message[i].id] : msg.message[i].id;

            // {id: 'blabla', ts: 892}
            if (msg.message[i].ts) {
                _delete(adapter, id, {ts: msg.message[i].ts});
            } else
            if (msg.message[i].start) {
                if (typeof msg.message[i].start === 'string') {
                    msg.message[i].start = new Date(msg.message[i].start).getTime();
                }
                if (typeof msg.message[i].end === 'string') {
                    msg.message[i].end = new Date(msg.message[i].end).getTime();
                }
                _delete(adapter, id, {start: msg.message[i].start, end: msg.message[i].end || Date.now()});
            } else
            if (typeof msg.message[i].state === 'object' && msg.message[i].state && msg.message[i].state.ts) {
                _delete(adapter, id, {ts: msg.message[i].state.ts});
            } else
            if (typeof msg.message[i].state === 'object' && msg.message[i].state && msg.message[i].state.start) {
                if (typeof msg.message[i].state.start === 'string') {
                    msg.message[i].state.start = new Date(msg.message[i].state.start).getTime();
                }
                if (typeof msg.message[i].state.end === 'string') {
                    msg.message[i].state.end = new Date(msg.message[i].state.end).getTime();
                }
                _delete(adapter, id, {start: msg.message[i].state.start, end: msg.message[i].state.end || Date.now()});
            } else {
                adapter.log.warn(`Invalid state for ${JSON.stringify(msg.message[i])}`);
            }
        }
    } else if (msg.message.state && Array.isArray(msg.message.state)) {
        adapter.log.debug(`deleteState ${msg.message.state.length} items`);
        id = adapter._aliasMap[msg.message.id] ? adapter._aliasMap[msg.message.id] : msg.message.id;

        for (let j = 0; j < msg.message.state.length; j++) {
            if (msg.message.state[j] && typeof msg.message.state[j] === 'object') {
                if (msg.message.state[j].ts) {
                    _delete(adapter, id, {ts: msg.message.state[j].ts});
                } else if (msg.message.state[j].start) {
                    if (typeof msg.message.state[j].start === 'string') {
                        msg.message.state[j].start = new Date(msg.message.state[j].start).getTime();
                    }
                    if (typeof msg.message.state[j].end === 'string') {
                        msg.message.state[j].end = new Date(msg.message.state[j].end).getTime();
                    }
                    _delete(adapter, id, {start: msg.message.state[j].start, end: msg.message.state[j].end || Date.now()});
                }
            } else if (msg.message.state[j] && typeof msg.message.state[j] === 'number') {
                _delete(adapter, id, {ts: msg.message.state[j]});
            } else {
                adapter.log.warn(`Invalid state for ${JSON.stringify(msg.message.state[j])}`);
            }
        }
    } else if (msg.message.ts && Array.isArray(msg.message.ts)) {
        adapter.log.debug(`deleteState ${msg.message.ts.length} items`);
        id = adapter._aliasMap[msg.message.id] ? adapter._aliasMap[msg.message.id] : msg.message.id;
        for (let j = 0; j < msg.message.ts.length; j++) {
            if (msg.message.ts[j] && typeof msg.message.ts[j] === 'number') {
                _delete(adapter, id, {ts: msg.message.ts[j]});
            } else {
                adapter.log.warn(`Invalid state for ${JSON.stringify(msg.message.ts[j])}`);
            }
        }
    } else if (msg.message.id && msg.message.state && typeof msg.message.state === 'object') {
        adapter.log.debug('deleteState 1 item');
        id = adapter._aliasMap[msg.message.id] ? adapter._aliasMap[msg.message.id] : msg.message.id;
        return _delete(adapter, id, {ts: msg.message.state.ts}, error => adapter.sendTo(msg.from, msg.command, {
            success: !error,
            error,
            connected: !!adapter._connected
        }, msg.callback));
    } else if (msg.message.id && msg.message.ts && typeof msg.message.ts === 'number') {
        adapter.log.debug('deleteState 1 item');
        id = adapter._aliasMap[msg.message.id] ? adapter._aliasMap[msg.message.id] : msg.message.id;
        return _delete(adapter, id, {ts: msg.message.ts}, error => adapter.sendTo(msg.from, msg.command, {
            success: !error,
            error,
            connected: !!adapter._connected
        }, msg.callback));
    } else {
        adapter.log.error('deleteState called with invalid data');
        return adapter.sendTo(msg.from, msg.command, {error: `Invalid call: ${JSON.stringify(msg)}`}, msg.callback);
    }

    setTimeout(() => {
        adapter.sendTo(msg.from, msg.command, {
            success: true,
            connected: !!adapter._connected
        }, msg.callback);
    }, 300);
}

function deleteStateAll(adapter, msg) {
    if (!msg.message) {
        adapter.log.error('deleteState called with invalid data');
        return adapter.sendTo(msg.from, msg.command, {
            error: `Invalid call: ${JSON.stringify(msg)}`
        }, msg.callback);
    }
    let id;
    if (Array.isArray(msg.message)) {
        adapter.log.debug(`deleteStateAll ${msg.message.length} items`);
        for (let i = 0; i < msg.message.length; i++) {
            id = adapter._aliasMap[msg.message[i].id] ? adapter._aliasMap[msg.message[i].id] : msg.message[i].id;
            _delete(adapter, id, {}, error => {
                adapter.sendTo(msg.from, msg.command, {
                    success: !error,
                    error,
                    connected: !!adapter._connected
                }, msg.callback);
            });
        }
    } else if (msg.message.id) {
        adapter.log.debug('deleteStateAll 1 item');
        id = adapter._aliasMap[msg.message.id] ? adapter._aliasMap[msg.message.id] : msg.message.id;
        _delete(adapter, id, {}, error => {
            adapter.sendTo(msg.from, msg.command, {
                success: !error,
                error,
                connected: !!adapter._connected
            }, msg.callback);
        });
    } else {
        adapter.log.error('deleteStateAll called with invalid data');
        return adapter.sendTo(msg.from, msg.command, {error: `Invalid call: ${JSON.stringify(msg)}`}, msg.callback);
    }
}

function update(adapter, id, state, cb) {
    if (!adapter._connected) {
        return cb && cb('not connected');
    }

    if (adapter.config.dbversion === '1.x') {
        let query = `SELECT * FROM "${id}" WHERE time = '${new Date(state.ts).toISOString()}'`;

        adapter._client.query(query, (err, result) => {
            if (err) {
                adapter.log.warn(`Error on update("${query}): ${err} / ${JSON.stringify(err.message)}"`);
                cb && cb(err);
            } else {
                setConnected(adapter, true);

                console.log(JSON.stringify(result));
                if (result[0] && result[0][0]) {
                    const stored = result[0][0];
                    if (state.val !== undefined) {
                        stored.val = state.val;
                    }
                    if (state.ack !== undefined) {
                        stored.ack = state.ack;
                    }
                    if (state.q !== undefined) {
                        stored.q = state.q;
                    }
                    if (state.from) {
                        stored.from = state.from;
                    }
                    stored.ts = state.ts;
                    delete stored.time;

                    _delete(adapter, id, {ts: stored.ts}, error => {
                        if (error) {
                            adapter.log.error(`Cannot delete value for ${id}: ${JSON.stringify(state)}`);
                            cb && cb(err);
                        } else {
                            pushValueIntoDB(adapter, id, stored);
                            cb && cb();
                        }
                    });
                } else {
                    adapter.log.error(`Cannot find value to delete for ${id}: ${JSON.stringify(state)}`);
                    cb && cb('not found');
                }
            }
        });
    } else {
        cb && cb('not implemented');
    }
}

function updateState(adapter, msg) {
    if (!msg.message) {
        adapter.log.error('updateState called with invalid data');
        return adapter.sendTo(msg.from, msg.command, {
            error: `Invalid call: ${JSON.stringify(msg)}`
        }, msg.callback);
    }
    let id;
    if (Array.isArray(msg.message)) {
        adapter.log.debug(`updateState ${msg.message.length} items`);
        for (let i = 0; i < msg.message.length; i++) {
            id = adapter._aliasMap[msg.message[i].id] ? adapter._aliasMap[msg.message[i].id] : msg.message[i].id;

            if (msg.message[i].state && typeof msg.message[i].state === 'object') {
                update(adapter, id, msg.message[i].state);
            } else {
                adapter.log.warn(`Invalid state for ${JSON.stringify(msg.message[i])}`);
            }
        }
        adapter.sendTo(msg.from, msg.command, {
            success: true,
            connected: !!adapter._connected
        }, msg.callback);
    } else if (msg.message.state && Array.isArray(msg.message.state)) {
        adapter.log.debug(`updateState ${msg.message.state.length} items`);
        id = adapter._aliasMap[msg.message.id] ? adapter._aliasMap[msg.message.id] : msg.message.id;
        for (let j = 0; j < msg.message.state.length; j++) {
            if (msg.message.state[j] && typeof msg.message.state[j] === 'object') {
                update(adapter, id, msg.message.state[j]);
            } else {
                adapter.log.warn(`Invalid state for ${JSON.stringify(msg.message.state[j])}`);
            }
        }
        adapter.sendTo(msg.from, msg.command, {
            success: true,
            connected: !!adapter._connected
        }, msg.callback);
    } else if (msg.message.id && msg.message.state && typeof msg.message.state === 'object') {
        adapter.log.debug('updateState 1 item');
        id = adapter._aliasMap[msg.message.id] ? adapter._aliasMap[msg.message.id] : msg.message.id;
        update(adapter, id, msg.message.state, error => adapter.sendTo(msg.from, msg.command, {
            success: !error,
            error,
            connected: !!adapter._connected
        }, msg.callback));
    } else {
        adapter.log.error('updateState called with invalid data');
        adapter.sendTo(msg.from, msg.command, {
            error: `Invalid call: ${JSON.stringify(msg)}`
        }, msg.callback);
    }
}

function storeState(adapter, msg) {
    if (!msg.message) {
        adapter.log.error('storeState called with invalid data');
        return adapter.sendTo(msg.from, msg.command, {
            error:  `Invalid call: ${JSON.stringify(msg)}`
        }, msg.callback);
    }

    let id;
    if (Array.isArray(msg.message)) {
        adapter.log.debug(`storeState ${msg.message.length} items`);
        for (let i = 0; i < msg.message.length; i++) {
            id = adapter._aliasMap[msg.message[i].id] ? adapter._aliasMap[msg.message[i].id] : msg.message[i].id;
            if (msg.message[i].state && typeof msg.message[i].state === 'object') {
                pushValueIntoDB(adapter, id, msg.message[i].state);
            } else {
                adapter.log.warn(`Invalid state for ${JSON.stringify(msg.message[i])}`);
            }
        }
    } else if (msg.message.state && Array.isArray(msg.message.state)) {
        adapter.log.debug(`storeState ${msg.message.state.length} items`);
        id = adapter._aliasMap[msg.message.id] ? adapter._aliasMap[msg.message.id] : msg.message.id;
        for (let j = 0; j < msg.message.state.length; j++) {
            if (msg.message.state[j] && typeof msg.message.state[j] === 'object') {
                pushValueIntoDB(adapter, id, msg.message.state[j]);
            } else {
                adapter.log.warn(`Invalid state for ${JSON.stringify(msg.message.state[j])}`);
            }
        }
    } else if (msg.message.id && msg.message.state && typeof msg.message.state === 'object') {
        adapter.log.debug('storeState 1 item');
        id = adapter._aliasMap[msg.message.id] ? adapter._aliasMap[msg.message.id] : msg.message.id;
        pushValueIntoDB(adapter, id, msg.message.state);
    } else {
        adapter.log.error('storeState called with invalid data');
        return adapter.sendTo(msg.from, msg.command, {
            error: `Invalid call: ${JSON.stringify(msg)}`
        }, msg.callback);
    }

    adapter.sendTo(msg.from, msg.command, {
        success:    true,
        connected: !!adapter._connected,
        seriesBufferCounter:      adapter._seriesBufferCounter,
        seriesBufferFlushPlanned: adapter._seriesBufferFlushPlanned
    }, msg.callback);
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
        if (adapter._influxDPs[id].skipped && !(adapter._influxDPs[id][adapter.namespace] && adapter._influxDPs[id][adapter.namespace].disableSkippedValueLogging)) {
            count++;
            pushHelper(adapter, id, adapter._influxDPs[id].skipped, () => {
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

function sortByTs(a, b) {
    const aTs = a.ts;
    const bTs = b.ts;
    return (aTs < bTs) ? -1 : ((aTs > bTs) ? 1 : 0);
}

function getHistory(adapter, msg) {
    const startTime = Date.now();

    if (!msg.message || !msg.message.options) {
        return adapter.sendTo(msg.from, msg.command, {
            error:  'Invalid call. No options for getHistory provided'
        }, msg.callback);
    }

    const options = {
        id:         msg.message.id === '*' ? null : msg.message.id,
        start:      msg.message.options.start,
        end:        msg.message.options.end || ((new Date()).getTime() + 5000000),
        step:       parseInt(msg.message.options.step,  10) || null,
        count:      parseInt(msg.message.options.count, 10) || 500,
        aggregate:  msg.message.options.aggregate || 'average', // One of: max, min, average, total
        limit:      parseInt(msg.message.options.limit, 10) || parseInt(msg.message.options.count, 10) || adapter.config.limit || 2000,
        addId:      msg.message.options.addId || false,
        ignoreNull: true,
        sessionId:  msg.message.options.sessionId,
        returnNewestEntries: msg.message.options.returnNewestEntries || false,
        percentile: msg.message.options.aggregate === 'percentile' ? parseInt(msg.message.options.percentile, 10) || 50 : null,
        quantile: msg.message.options.aggregate === 'quantile' ? parseFloat(msg.message.options.quantile) || 0.5 : null,
        integralUnit: msg.message.options.aggregate === 'integral' ? parseInt(msg.message.options.integralUnit, 10) || 60 : null,
        integralInterpolation: msg.message.options.aggregate === 'integral' ? msg.message.options.integralInterpolation || 'none' : null,
        removeBorderValues: msg.message.options.removeBorderValues || false
    };

    if (!options.start && options.count) {
        options.returnNewestEntries = true;
    }

    if (msg.message.options.round !== null && msg.message.options.round !== undefined && msg.message.options.round !== '') {
        msg.message.options.round = parseInt(msg.message.options.round, 10);
        if (!isFinite(msg.message.options.round) || msg.message.options.round < 0) {
            options.round = adapter.config.round;
        } else {
            options.round = Math.pow(10, parseInt(msg.message.options.round, 10));
        }
    } else {
        options.round = adapter.config.round;
    }

    const debugLog = options.debugLog = !!(adapter._influxDPs[options.id] && adapter._influxDPs[options.id][adapter.namespace] && adapter._influxDPs[options.id][adapter.namespace].enableDebugLogs);

    debugLog && adapter.log.debug(`getHistory (InfluxDB1) call: ${JSON.stringify(options)}`);

    if (options.id && adapter._aliasMap[options.id]) {
        options.id = adapter._aliasMap[options.id];
    }

    if (options.aggregate === 'percentile' && options.percentile < 0 || options.percentile > 100) {
        adapter.log.error(`Invalid percentile value: ${options.percentile}, use 50 as default`);
        options.percentile = 50;
    }

    if (options.aggregate === 'quantile' && options.quantile < 0 || options.quantile > 1) {
        adapter.log.error(`Invalid quantile value: ${options.quantile}, use 0.5 as default`);
        options.quantile = 0.5;
    }

    if (options.aggregate === 'integral' && (typeof options.integralUnit !== 'number' || options.integralUnit <= 0)) {
        adapter.log.error(`Invalid integralUnit value: ${options.integralUnit}, use 60s as default`);
        options.integralUnit = 60;
    }

    if (!adapter._influxDPs[options.id]) {
        adapter.sendTo(msg.from, msg.command, {
            result: [],
            step:   0,
            error:  `Logging not activated for ${options.id}`
        }, msg.callback);
        return;
    }

    if (options.start > options.end) {
        const _end = options.end;
        options.end   = options.start;
        options.start = _end;
    }

    if (!options.start && !options.count) {
        options.start = options.end - 86400000; // - 1 day
    }

    let resultsFromInfluxDB = options.aggregate !== 'onchange' && options.aggregate !== 'none' && options.aggregate !== 'minmax';
    if (options.aggregate === 'integral' && options.integralInterpolation === 'linear') {
        resultsFromInfluxDB = false;
    }

    // query one timegroup-value more than requested originally at start and end
    // to make sure to have no 0 values because of the way InfluxDB does group by time

    if (resultsFromInfluxDB) {
        if (!options.step) {
            // calculate "step" based on difference between start and end using count
            options.step = Math.round((options.end - options.start) / options.count);
        }
        if (options.start) {
            options.start -= options.step;
        }
        options.end += options.step;
        options.limit += 2;
    }

    options.preAggregated = true;
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

            case 'percentile':
                query += ` percentile(value, ${options.percentile}) as val`;
                break;

            case 'quantile':
                query += ` percentile(value, ${options.quantile * 100}) as val`;
                break;

            case 'integral':
                if (options.integralInterpolation === 'linear') {
                    query += ' value';
                    options.preAggregated = false;
                } else {
                    query += ` integral(value, ${options.integralUnit}s) as val`;
                }
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
                options.preAggregated = false;
                break;

            default:
                query += ' mean(value) as val';
                break;
        }

    } else {
        query += ' *';
    }

    query += ` from "${options.id}"`;

    query += ` WHERE `;
    if (options.start) {
        query += ` time > '${new Date(options.start).toISOString()}' AND `;
    }
    query += ` time < '${new Date(options.end).toISOString()}'`;

    if (resultsFromInfluxDB) {
        query += ` GROUP BY time(${options.step}ms) fill(previous)`;
    }

    if ((!options.start && options.count) || (options.aggregate === 'none' && options.count && options.returnNewestEntries) ) {
        query += ` ORDER BY time DESC`;
    } else {
        query += ' ORDER BY time ASC';
    }

    if (resultsFromInfluxDB) {
        query += ` LIMIT ${options.limit}`;
    } else if (options.aggregate !== 'minmax' && (options.aggregate !== 'integral')) {
        query += ` LIMIT ${options.count}`;
    }

    // select one datapoint more than wanted
    if (!options.removeBorderValues) {
        let addQuery = '';
        if (options.start) {
            addQuery = `SELECT value from "${options.id}" WHERE time <= '${new Date(options.start).toISOString()}' ORDER BY time DESC LIMIT 1;`;
            query = addQuery + query;
        }
        addQuery = `;SELECT value from "${options.id}" WHERE time >= '${new Date(options.end).toISOString()}' LIMIT 1`;
        query = query + addQuery;
    }

    debugLog && adapter.log.debug(query);

    storeBufferedSeries(adapter, options.id, () => {
        // if specific id requested
        adapter._client.query(query, (error, rows) => {
            if (error) {
                if (adapter._client.request.getHostsAvailable().length === 0) {
                    setConnected(adapter, false);
                }
                adapter.log.error(`getHistory: ${error}`);
            } else {
                setConnected(adapter, true);
            }

            debugLog && adapter.log.debug(`Response rows: ${JSON.stringify(rows)}`);

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
                            if (isFinite(rows[qr][rr].val)) {
                                rows[qr][rr].val = parseFloat(rows[qr][rr].val);
                                if (options.round) {
                                    rows[qr][rr].val = Math.round(rows[qr][rr].val * options.round) / options.round;
                                }
                            }
                        }

                        if (options.addId) {
                            rows[qr][rr].id = options.id;
                        }
                        result.push(rows[qr][rr]);
                    }
                }
                result = result.sort(sortByTs);
            }

            Aggregate.sendResponse(adapter, msg, options, (error ? error.toString() : null) || result, startTime);
        });
    });
}

function getHistoryIflx2(adapter, msg) {
    const startTime = Date.now();

    if (!msg.message || !msg.message.options) {
        return adapter.sendTo(msg.from, msg.command, {
            error:  'Invalid call. No options for getHistory provided'
        }, msg.callback);
    }

    const options = {
        id:         msg.message.id === '*' ? null : msg.message.id,
        start:      msg.message.options.start,
        end:        msg.message.options.end || ((new Date()).getTime() + 5000000),
        step:       parseInt(msg.message.options.step,  10) || null,
        count:      parseInt(msg.message.options.count, 10) || 500,
        aggregate:  msg.message.options.aggregate || 'average', // One of: max, min, average, total
        limit:      parseInt(msg.message.options.limit, 10) || parseInt(msg.message.options.count, 10) || adapter.config.limit || 2000,
        addId:      msg.message.options.addId || false,
        ignoreNull: true,
        sessionId:  msg.message.options.sessionId,
        returnNewestEntries: msg.message.options.returnNewestEntries || false,
        percentile: msg.message.options.aggregate === 'percentile' ? parseInt(msg.message.options.percentile, 10) || 50 : null,
        quantile: msg.message.options.aggregate === 'quantile' ? parseFloat(msg.message.options.quantile) || 0.5 : null,
        integralUnit: msg.message.options.aggregate === 'integral' ? parseInt(msg.message.options.integralUnit, 10) || 60 : null,
        integralInterpolation: msg.message.options.aggregate === 'integral' ? msg.message.options.integralInterpolation || 'none' : null,
        removeBorderValues: msg.message.options.removeBorderValues || false
    };

    if (!options.start && options.count) {
        options.returnNewestEntries = true;
    }

    if (msg.message.options.round !== null && msg.message.options.round !== undefined && msg.message.options.round !== '') {
        msg.message.options.round = parseInt(msg.message.options.round, 10);
        if (!isFinite(msg.message.options.round) || msg.message.options.round < 0) {
            options.round = adapter.config.round;
        } else {
            options.round = Math.pow(10, parseInt(msg.message.options.round, 10));
        }
    } else {
        options.round = adapter.config.round;
    }

    const debugLog = options.debugLog = !!(adapter._influxDPs[options.id] && adapter._influxDPs[options.id][adapter.namespace] && adapter._influxDPs[options.id][adapter.namespace].enableDebugLogs);

    debugLog && adapter.log.debug(`getHistory (InfluxDB2) call: ${JSON.stringify(options)}`);

    if (options.id && adapter._aliasMap[options.id]) {
        options.id = adapter._aliasMap[options.id];
    }

    if (options.aggregate === 'percentile' && options.percentile < 0 || options.percentile > 100) {
        adapter.log.error(`Invalid percentile value: ${options.percentile}, use 50 as default`);
        options.percentile = 50;
    }

    if (options.aggregate === 'quantile' && options.quantile < 0 || options.quantile > 1) {
        adapter.log.error(`Invalid quantile value: ${options.quantile}, use 0.5 as default`);
        options.quantile = 0.5;
    }

    if (options.aggregate === 'integral' && (typeof options.integralUnit !== 'number' || options.integralUnit <= 0)) {
        adapter.log.error(`Invalid integralUnit value: ${options.integralUnit}, use 60s as default`);
        options.integralUnit = 60;
    }

    if (!adapter._influxDPs[options.id] || !adapter._influxDPs[options.id][adapter.namespace]) {
        return adapter.sendTo(msg.from, msg.command, {
            result: [],
            step:   0,
            error:  `Logging not activated for ${options.id}`
        }, msg.callback);
    }

    if (options.start > options.end) {
        const _end = options.end;
        options.end   = options.start;
        options.start = _end;
    }

    if (!options.start && !options.count) {
        options.start = options.end - 86400000; // - 1 day
    }

    const resultsFromInfluxDB = options.aggregate !== 'onchange' && options.aggregate !== 'none' && options.aggregate !== 'minmax';

    // query one timegroup-value more than requested originally at start and end
    // to make sure to have no 0 values because of the way InfluxDB does group by time
    if (resultsFromInfluxDB) {
        if (!options.step) {
            // calculate "step" based on difference between start and end using count
            options.step = Math.round((options.end - options.start) / options.count);
        }
        if (options.start) {
            options.start -= options.step;
        }
        options.end += options.step;
        options.limit += 2;
    }

    const fluxQueries = [];
    let fluxQuery = `from(bucket: "${adapter.config.dbname}") `;

    const valueColumn = adapter.config.usetags ? '_value' : 'value';

    fluxQuery += ` |> range(${(options.start) ? `start: ${new Date(options.start).toISOString()}, ` : `start: ${new Date(options.end - adapter.config.retention).toISOString()}ms, `}stop: ${new Date(options.end).toISOString()})`;
    fluxQuery += ` |> filter(fn: (r) => r["_measurement"] == "${options.id}")`;

    if (adapter.config.usetags)
        fluxQuery += ' |> duplicate(column: "_value", as: "value")';
    else
        fluxQuery += ' |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")';

    if (resultsFromInfluxDB) {
        if ((options.step !== null) && (options.step > 0))
            fluxQuery += ` |> window(every: ${options.step}ms)`;
        fluxQuery += `|> fill(column: "${valueColumn}", usePrevious: true)`;
    } else if (options.aggregate !== 'minmax' && options.aggregate !== 'integral') {
        fluxQuery += ` |> group() |> limit(n: ${options.count})`;
    }

    if ((!options.start && options.count) || (options.aggregate === 'none' && options.count && options.returnNewestEntries) ) {
        fluxQuery += ` |> sort(columns:["_time"], desc: true)`;
    } else {
        fluxQuery += ` |> sort(columns:["_time"], desc: false)`;
    }


    // Workaround to detect if measurement is of type bool (to skip non-sensual aggregation options)
    // There seems to be no officially supported way to detect this, so we check it by forcing a type-conflict
    const booleanTypeCheckQuery = `
        from(bucket: "${adapter.config.dbname}")
        |> range(${(options.start) ? `start: ${new Date(options.start).toISOString()}, ` : `start: -${adapter.config.retention}ms, `}stop: ${new Date(options.end).toISOString()})
        |> filter(fn: (r) => r["_measurement"] == "${options.id}" and contains(value: r._value, set: [true, false]))
    `;

    storeBufferedSeries(adapter, options.id, () => {
        adapter._client.query(booleanTypeCheckQuery, (error, rslt) => {
            let supportsAggregates;
            if (error) {
                if (error.message.includes('type conflict: bool')) {
                    supportsAggregates = true;
                    error = null;
                } else {
                    return adapter.sendTo(msg.from, msg.command, {
                        result:     [],
                        error:      error,
                        sessionId:  options.sessionId
                    }, msg.callback);
                }
            } else {
                if (rslt.find(r => r.error && r.error.includes('type conflict: bool'))) {
                    supportsAggregates = true;
                } else {
                    supportsAggregates = false;
                    debugLog && adapter.log.debug(`Measurement ${options.id} seems to be no number - skipping aggregation options`);
                }
            }
            if (supportsAggregates) {
                if (adapter._influxDPs[options.id][adapter.namespace].storageType && adapter._influxDPs[options.id][adapter.namespace].storageType !== 'Number') {
                    supportsAggregates = false;
                } else if (adapter._influxDPs[options.id][adapter.namespace].state && typeof adapter._influxDPs[options.id][adapter.namespace].state.val !== 'number') {
                    supportsAggregates = false;
                } else if (adapter._influxDPs[options.id][adapter.namespace].skipped && typeof adapter._influxDPs[options.id][adapter.namespace].skipped.val !== 'number') {
                    supportsAggregates = false;
                }
            }

            options.preAggregated = true;
            if (options.step && supportsAggregates) {
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

                    case 'percentile':
                        fluxQuery += ` |> quantile(column: "${valueColumn}", q: ${options.percentile / 100})`;
                        break;

                    case 'quantile':
                        fluxQuery += ` |> quantile(column: "${valueColumn}", q: ${options.quantile})`;
                        break;

                    case 'integral':
                        fluxQuery += ` |> integral(column: "${valueColumn}", unit: ${options.integralUnit}s, interpolate: ${options.integralInterpolation === 'linear' ? '"linear"' : '""'})`;
                        break;

                    case 'total':
                        fluxQuery += ` |> sum(column: "${valueColumn}")`;
                        break;

                    case 'count':
                        fluxQuery += ` |> count(column: "${valueColumn}")`;
                        break;

                    default:
                        fluxQuery += ` |> mean(column: "${valueColumn}")`;
                        options.preAggregated = false;
                        break;
                }
            }

            fluxQueries.push(fluxQuery);

            // select one datapoint more than wanted
            if (!options.removeBorderValues) {
                let addFluxQuery = '';
                if (options.start) {
                    // get one entry "before" the defined timeframe for displaying purposes
                    addFluxQuery = `from(bucket: "${adapter.config.dbname}") 
                    |> range(start: ${new Date(options.start - (adapter.config.retention || 31536000) * 1000).toISOString()}, stop: ${new Date(options.start).toISOString()}) 
                    |> filter(fn: (r) => r["_measurement"] == "${options.id}") 
                    ${(!adapter.config.usetags) ? '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")' : ''}
                    |> group() 
                    |> sort(columns: ["_time"], desc: true) 
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
                    |> group() 
                    |> sort(columns: ["_time"], desc: false) 
                    |> limit(n: 1)`;
                fluxQueries.push(addFluxQuery);
            }

            debugLog && adapter.log.debug(`History-queries to execute: ${fluxQueries}`);

            // if specific id requested
            adapter._client.queries(fluxQueries, (err, rows) => {
                if (err && !rows) {
                    if (adapter._client.request.getHostsAvailable().length === 0) {
                        setConnected(adapter, false);
                    }
                    adapter.log.error(`getHistory: ${err}`);
                } else {
                    setConnected(adapter, true);
                }

                debugLog && adapter.log.debug(`Parsing retrieved rows:${JSON.stringify(rows)}`);

                let result = [];

                if (rows && rows.length) {
                    for (let qr = 0; qr < rows.length; qr++) {
                        for (let rr = 0; rr < rows[qr].length; rr++) {
                            if ((rows[qr][rr].val === undefined) && (rows[qr][rr].value !== undefined)) {
                                rows[qr][rr].val = rows[qr][rr].value;
                                delete rows[qr][rr].value;
                            }

                            if (rows[qr][rr].val !== null) {
                                if (isFinite(rows[qr][rr].val)) {
                                    rows[qr][rr].val = parseFloat(rows[qr][rr].val);
                                    if (options.round) {
                                        rows[qr][rr].val = Math.round(rows[qr][rr].val * options.round) / options.round;
                                    }
                                }
                            }

                            if (rows[qr][rr].time) {
                                rows[qr][rr].ts = new Date(rows[qr][rr].time).getTime();
                                delete rows[qr][rr].time;
                            } else if (rows[qr][rr]._start && rows[qr][rr]._stop) {
                                const startTime = new Date(rows[qr][rr]._start).getTime();
                                const stopTime = new Date(rows[qr][rr]._stop).getTime();
                                rows[qr][rr].ts = startTime + (stopTime - startTime) / 2;
                                delete rows[qr][rr]._start;
                                delete rows[qr][rr]._stop;
                            }

                            delete rows[qr][rr].result;
                            delete rows[qr][rr].table;

                            if (options.addId) {
                                rows[qr][rr].id = rows[qr][rr]._measurement || options.id;
                            }
                            delete rows[qr][rr]._measurement;

                            result.push(rows[qr][rr]);
                        }
                    }
                    result = result.sort(sortByTs);
                }

                if (options.debugLog) {
                    options.log = adapter.log.debug;
                }

                Aggregate.sendResponse(adapter, msg, options, (error ? error.toString() : null) || result, startTime);
            });
        });
    });
}

function query(adapter, msg) {
    if (adapter._client) {
        const query = msg.message.query || msg.message;

        if (!query || typeof query !== 'string') {
          adapter.log.error(`query missing: ${query}`);
          adapter.sendTo(msg.from, msg.command, {
              result: [],
              error:  'Query missing'
          }, msg.callback);
          return;
        }

        adapter.log.debug(`query: ${query}`);

        adapter._client.query(query, (err, rows) => {
            if (err) {
                if (adapter._client.request.getHostsAvailable().length === 0) {
                    setConnected(adapter, false);
                }
                adapter.log.error(`query: ${err}`);
                return adapter.sendTo(msg.from, msg.command, {
                        result: [],
                        error:  'Invalid call'
                    }, msg.callback);
            } else {
                setConnected(adapter, true);
            }

            adapter.log.debug(`result: ${JSON.stringify(rows)}`);

            for (let r = 0, l = rows.length; r < l; r++) {
                for (let rr = 0, ll = rows[r].length; rr < ll; rr++) {
                    if (rows[r][rr].time) {
                        rows[r][rr].ts = new Date(rows[r][rr].time).getTime();
                        delete rows[r][rr].time;
                    } else if (rows[r][rr]._start && rows[r][rr]._stop) {
                        const startTime = new Date(rows[r][rr]._start).getTime();
                        const stopTime = new Date(rows[r][rr]._stop).getTime();
                        rows[r][rr].ts = startTime + (stopTime - startTime) / 2;
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
            adapter.log.warn(`Error in received multiQuery: ${error}`);
            adapter.sendTo(msg.from, msg.command, {
                result: [],
                error:  error
            }, msg.callback);
            return;
        }
        adapter.log.debug(`queries: ${queries}`);

        adapter._client.queries(queries, (err, rows) => {
            if (err) {
                if (adapter._client.request.getHostsAvailable().length === 0) {
                    setConnected(adapter, false);
                }
                adapter.log.error(`queries: ${err}`);
                return adapter.sendTo(msg.from, msg.command, {
                    result: [],
                    error:  'Invalid call'
                }, msg.callback);
            } else {
                setConnected(adapter, true);
            }

            adapter.log.debug(`result: ${JSON.stringify(rows)}`);

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
            adapter.log.error(`enableHistory: ${error}`);
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
            adapter.log.error(`disableHistory: ${error}`);
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
        if (adapter._influxDPs.hasOwnProperty(id) && adapter._influxDPs[id] && adapter._influxDPs[id][adapter.namespace] && adapter._influxDPs[id][adapter.namespace].enabled) {
            data[adapter._influxDPs[id].realId] = adapter._influxDPs[id][adapter.namespace];
        }
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
