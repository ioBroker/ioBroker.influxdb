/* jshint -W097 */
/* jshint strict: false */
/* jslint node: true */
/* jshint expr: true */
const expect = require('chai').expect;
const setup = require('./lib/setup');
const tests = require('./lib/testcases');

let objects = null;
let states = null;
let onStateChanged = null;
let sendToID = 1;

const adapterShortName = setup.adapterName.substring(setup.adapterName.indexOf('.') + 1);

let now;

function checkConnectionOfAdapter(cb, counter) {
    counter ||= 0;
    console.log(`Try check #${counter}`);
    if (counter > 30) {
        cb?.('Cannot check connection');
        return;
    }

    console.log(`Checking alive key for key: influx-buffer`);
    states.getState(`system.adapter.influxdb.0.alive`, (err, state) => {
        if (err) {
            console.error(err);
        }
        if (state && state.val) {
            cb?.();
        } else {
            setTimeout(() => checkConnectionOfAdapter(cb, counter + 1), 1000);
        }
    });
}

function sendTo(target, command, message, callback) {
    onStateChanged = (id, state) => {
        if (id === 'messagebox.system.adapter.test.0') {
            callback(state.message);
        }
    };

    states.pushMessage(`system.adapter.${target}`, {
        command: command,
        message: message,
        from: 'system.adapter.test.0',
        callback: {
            message: message,
            id: sendToID++,
            ack: false,
            time: Date.now(),
        },
    });
}

describe(`Test influx-buffer adapter with Buffered write`, function () {
    before(`Test influx-buffer adapter: Start js-controller`, function (_done) {
        this.timeout(600000); // because of first install from npm

        setup.setupController(async () => {
            const config = await setup.getAdapterConfig();
            // enable adapter
            config.common.enabled = true;
            config.common.loglevel = 'debug';

            if (process.env.INFLUXDB2) {
                let authToken;
                if (process.env.AUTHTOKEN) {
                    authToken = JSON.parse(process.env.AUTHTOKEN).token;
                    console.log(`AUTHTOKEN=${process.env.AUTHTOKEN}`);
                    console.log(`extracted token =${authToken}`);
                }
                config.native.dbversion = '2.x';

                let secret = await setup.getSecret();
                if (secret === null) {
                    secret = 'Zgfr56gFe87jJOM';
                }

                console.log(`############SECRET: ${secret}`);
                config.native.token = setup.encrypt(secret, process.env.INFLUXDB2_TOKEN || 'test-token'); //authToken;
                config.native.organization = process.env.INFLUXDB2_ORG || 'test-org';
            } else if (process.env.INFLUX_DB1_HOST) {
                config.native.host = process.env.INFLUX_DB1_HOST;
            }

            config.native.seriesBufferFlushInterval = 30;
            config.native.seriesBufferMax = 5;
            config.native.retention = -1;
            config.native.customRetentionDuration = 10;
            config.native.dbname = 'otheriobroker';
            // without start values the timed relog has to start on its own, see "Timed relog starts without an update"
            config.native.relogLastValueOnStart = false;

            config.native.enableDebugLogs = true;

            await setup.setAdapterConfig(config.common, config.native);

            setup.startController(
                true,
                (id, obj) => {},
                (id, state) => onStateChanged && onStateChanged(id, state),
                async (_objects, _states) => {
                    objects = _objects;
                    states = _states;

                    await tests.preInit(objects, states, sendTo, adapterShortName);

                    _done();
                },
            );
        });
    });

    it(`Test influx-buffer adapter: Check if adapter started`, function (done) {
        this.timeout(60000);

        checkConnectionOfAdapter(res => {
            if (res) {
                console.log(res);
            }
            expect(res).not.to.be.equal('Cannot check connection');

            objects.setObject(
                'system.adapter.test.0',
                {
                    common: {},
                    type: 'instance',
                },
                () => {
                    states.subscribeMessage('system.adapter.test.0');

                    sendTo(
                        'influxdb.0',
                        'enableHistory',
                        {
                            id: 'system.adapter.influxdb.0.memHeapTotal',
                            options: {
                                changesOnly: true,
                                debounce: 0,
                                retention: 31536000,
                                storageType: 'String',
                            },
                        },
                        result => {
                            expect(result.error).to.be.undefined;
                            expect(result.success).to.be.true;

                            sendTo(
                                'influxdb.0',
                                'enableHistory',
                                {
                                    id: 'system.adapter.influxdb.0.uptime',
                                    options: {
                                        changesOnly: false,
                                        debounce: 0,
                                        retention: 31536000,
                                        storageType: 'Boolean',
                                    },
                                },
                                result => {
                                    expect(result.error).to.be.undefined;
                                    expect(result.success).to.be.true;

                                    sendTo(
                                        'influxdb.0',
                                        'enableHistory',
                                        {
                                            id: 'system.adapter.influxdb.0.memHeapUsed',
                                            options: {
                                                changesOnly: false,
                                                debounce: 0,
                                                retention: 31536000,
                                            },
                                        },
                                        result => {
                                            expect(result.error).to.be.undefined;
                                            expect(result.success).to.be.true;

                                            setTimeout(() => done(), 2000);
                                        },
                                    );
                                },
                            );
                        },
                    );
                },
            );
        });
    });

    tests.register(it, expect, sendTo, adapterShortName, false, 0, 3, 'influx-buffer');

    it(`Test influx-buffer: Write string value for memHeapUsed into DB to force a type conflict`, function (done) {
        this.timeout(5000);
        now = Date.now();

        states.setState(
            'system.adapter.influxdb.0.memHeapUsed',
            { val: 'Blubb', ts: now - 20000, from: 'test.0' },
            err => {
                if (err) {
                    console.log(err);
                }
                setTimeout(() => {
                    //sendTo('influxdb.0', 'flushBuffer', {id: 'system.adapter.influxdb.0.memHeapUsed'}, result => {
                    sendTo('influxdb.0', 'flushBuffer', {}, result => {
                        expect(result.error).to.be.not.ok;
                        done();
                    });
                }, 1000);
            },
        );
    });

    it(`Test influx-buffer: Read values from DB using query`, function (done) {
        this.timeout(10000);

        states.getState('influxdb.0.testValueCounter', (err, state) => {
            let query = 'SELECT * FROM "influxdb.0.testValue"';

            if (process.env.INFLUXDB2) {
                query = `from(bucket: "iobroker") |> range(start: -2d) |> filter(fn: (r) => r["_measurement"] == "influxdb.0.testValue") |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value") |> group() |> sort(columns:["_time"], desc: false)`;
            }

            sendTo('influxdb.0', 'query', query, result => {
                console.log(JSON.stringify(result.result, null, 2));
                console.log(`Expected results: ${state.val}`);
                expect(result.result[0].length).to.be.at.least(5);
                let found = 0;
                for (let i = 0; i < result.result[0].length; i++) {
                    if (result.result[0][i].value >= 1 && result.result[0][i].value <= 3) {
                        found++;
                    }
                }
                expect(found).to.be.within(13, 14);

                done();
            });
        });
    });

    it(`Test influx-buffer: Check Datapoint Types`, function (done) {
        this.timeout(65000);

        if (process.env.INFLUXDB2) {
            // TODO: FIndFlux equivalent!
            return done();
        }

        setTimeout(() => {
            sendTo('influxdb.0', 'query', 'SHOW FIELD KEYS FROM "influxdb.0.testValue"', result => {
                console.log(`result: ${JSON.stringify(result.result, null, 2)}`);
                let found = false;
                for (let i = 0; i < result.result[0].length; i++) {
                    if (result.result[0][i].fieldKey === 'value') {
                        found = true;
                        expect(result.result[0][i].fieldType).to.be.equal('float');
                        break;
                    }
                }
                expect(found).to.be.true;

                sendTo(
                    'influxdb.0',
                    'query',
                    'SHOW FIELD KEYS FROM "system.adapter.influxdb.0.memHeapTotal"',
                    result2 => {
                        console.log(`result2: ${JSON.stringify(result2.result, null, 2)}`);
                        let found = false;
                        for (let i = 0; i < result2.result[0].length; i++) {
                            if (result2.result[0][i].fieldKey === 'value') {
                                found = true;
                                expect(result2.result[0][i].fieldType).to.be.equal('string');
                                break;
                            }
                        }
                        expect(found).to.be.true;

                        sendTo(
                            'influxdb.0',
                            'query',
                            'SHOW FIELD KEYS FROM "system.adapter.influxdb.0.uptime"',
                            result3 => {
                                console.log(`result3: ${JSON.stringify(result3.result, null, 2)}`);
                                let found = false;
                                for (let i = 0; i < result3.result[0].length; i++) {
                                    if (result3.result[0][i].fieldKey === 'value') {
                                        found = true;
                                        expect(result3.result[0][i].fieldType).to.be.equal('boolean');
                                        break;
                                    }
                                }
                                expect(found).to.be.true;

                                setTimeout(() => done(), 3000);
                            },
                        );
                    },
                );
            });
        }, 60000);
    });

    it(`Test influx-buffer: Check that storageType is set now for memHeapUsed`, function (done) {
        this.timeout(5000);

        objects.getObject('system.adapter.influxdb.0.memHeapUsed', (err, obj) => {
            expect(err).to.be.null;
            console.log(JSON.stringify(obj, null, 2));
            expect(obj.common.custom['influxdb.0'].storageType).to.be.equal('Number');
            done();
        });
    });

    after(`Test influx-buffer adapter: Stop js-controller`, function (done) {
        this.timeout(15000);

        setup.stopController(normalTerminated => {
            console.log('Adapter normal terminated: ' + normalTerminated);
            setTimeout(done, 2000);
        });
    });
});
