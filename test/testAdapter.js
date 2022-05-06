/* jshint -W097 */
/* jshint strict: false */
/* jslint node: true */
/* jshint expr: true */
const expect = require('chai').expect;
const setup  = require('./lib/setup');
const tests = require('./lib/testcases');

let objects = null;
let states  = null;
let onStateChanged = null;
let sendToID = 1;

const adapterShortName = setup.adapterName.substring(setup.adapterName.indexOf('.') + 1);

let now;

function checkConnectionOfAdapter(cb, counter) {
    counter = counter || 0;
    console.log(`Try check #${counter}`);
    if (counter > 30) {
        cb && cb('Cannot check connection');
        return;
    }

    console.log(`Checking alive key for key : ${adapterShortName}`);
    states.getState(`system.adapter.${adapterShortName}.0.alive`, (err, state) => {
        err && console.error(err);
        if (state && state.val) {
            cb && cb();
        } else {
            setTimeout(() =>
                checkConnectionOfAdapter(cb, counter + 1)
            , 1000);
        }
    });
}

function checkValueOfState(id, value, cb, counter) {
    counter = counter || 0;
    if (counter > 20) {
        cb && cb(`Cannot check value Of State ${id}`);
        return;
    }

    states.getState(id, (err, state) => {
        err && console.error(err);
        if (value === null && !state) {
            cb && cb();
        } else
        if (state && (value === undefined || state.val === value)) {
            cb && cb();
        } else {
            setTimeout(() =>
                checkValueOfState(id, value, cb, counter + 1)
            , 500);
        }
    });
}

function sendTo(target, command, message, callback) {
    onStateChanged = function (id, state) {
        if (id === 'messagebox.system.adapter.test.0') {
            callback(state.message);
        }
    };

    states.pushMessage(`system.adapter.${target}`, {
        command:    command,
        message:    message,
        from:       'system.adapter.test.0',
        callback: {
            message: message,
            id:      sendToID++,
            ack:     false,
            time:    Date.now()
        }
    });
}

describe(`Test ${adapterShortName} adapter`, function () {
    before(`Test ${adapterShortName} adapter: Start js-controller`, function (_done) {
        this.timeout(600000); // because of first install from npm

        setup.setupController(async () => {
            const config = await setup.getAdapterConfig();
            // enable adapter
            config.common.enabled  = true;
            config.common.loglevel = 'debug';

            if (process.env.INFLUXDB2) {
                const authToken = JSON.parse(process.env.AUTHTOKEN).token;
                console.log(`AUTHTOKEN=${process.env.AUTHTOKEN}`);
                console.log(`extracted token =${authToken}`);
                config.native.dbversion = '2.x';

                let secret = await setup.getSecret();
                if (secret === null) {
                    secret = 'Zgfr56gFe87jJOM';
                }

                console.log(`############SECRET: ${secret}`);
                config.native.token = setup.encrypt(secret, 'test-token'); //authToken;
                config.native.organization = 'test-org';
            } else if (process.env.INFLUX_DB1_HOST) {
                config.native.host = process.env.INFLUX_DB1_HOST;
            }
            config.native.enableDebugLogs = true;

            await setup.setAdapterConfig(config.common, config.native);

            setup.startController(
                true,
                (id, obj) => {},
                (id, state) => onStateChanged && onStateChanged(id, state),
                async (_objects, _states) => {
                    objects = _objects;
                    states  = _states;

                    await tests.preInit(objects, states, sendTo, adapterShortName);

                    _done();
                });
        });
    });

    it(`Test ${adapterShortName} adapter: Check if adapter started`, function (done) {
        this.timeout(60000);

        checkConnectionOfAdapter(res => {
            res && console.log(res);
            expect(res).not.to.be.equal('Cannot check connection');
            sendTo('influxdb.0', 'enableHistory', {
                id: 'system.adapter.influxdb.0.memHeapTotal',
                options: {
                    changesOnly:  true,
                    debounce:     0,
                    retention:    31536000,
                    storageType: 'String'
                }
            }, result => {
                expect(result.error).to.be.undefined;
                expect(result.success).to.be.true;

                sendTo('influxdb.0', 'enableHistory', {
                    id: 'system.adapter.influxdb.0.uptime',
                    options: {
                        changesOnly:  false,
                        debounce:     0,
                        retention:    31536000,
                        storageType: 'Boolean'
                    }
                }, result => {
                    expect(result.error).to.be.undefined;
                    expect(result.success).to.be.true;

                    sendTo('influxdb.0', 'enableHistory', {
                        id: 'system.adapter.influxdb.0.memHeapUsed',
                        options: {
                            changesOnly:  false,
                            debounce:     0,
                            retention:    31536000,
                        }
                    }, result => {
                        expect(result.error).to.be.undefined;
                        expect(result.success).to.be.true;

                        // wait till adapter receives the new settings
                        setTimeout(() =>
                            done(), 2000);
                    });
                });
            });
        });
    });

    tests.register(it, expect, sendTo, adapterShortName, false, 0, 3);

    it(`Test ${adapterShortName}: Write string value for memHeapUsed into DB to force a type conflict`, function (done) {
        this.timeout(5000);
        now = Date.now();

        states.setState('system.adapter.influxdb.0.memHeapUsed', {val: 'Blubb', ts: now - 20000, from: 'test.0'}, err => {
            err && console.log(err);
            done();
        });
    });


    it(`Test ${adapterShortName}: Read values from DB using query`, function (done) {
        this.timeout(10000);

        let query = 'SELECT * FROM "influxdb.0.testValue"';
        if (process.env.INFLUXDB2) {
            const date = Date.now();
            query = `from(bucket: "iobroker") |> range(start: ${new Date(date - 24*60*60*1000).toISOString()}, stop: ${new Date(date).toISOString()}) |> filter(fn: (r) => r.["_measurement"] == "influxdb.0.testValue") |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value") |> group() |> sort(columns:["_time"], desc: false)`;
        }
        sendTo('influxdb.0', 'query', query, result => {
            console.log(JSON.stringify(result.result, null, 2));
            expect(result.result[0].length).to.be.at.least(5);
            let found = 0;
            for (let i = 0; i < result.result[0].length; i++) {
                if (result.result[0][i].value >= 1 && result.result[0][i].value <= 3) {
                    found ++;
                }
            }
            expect(found).to.be.equal(14);

            done();
        });
    });

    it(`Test ${adapterShortName}: Check Datapoint Types`, function (done) {
        this.timeout(65000);

        if (process.env.INFLUXDB2) {
            // TODO: Find Flux equivalent!
            return done();
        }

        setTimeout(function() {
            let query = 'SHOW FIELD KEYS FROM "influxdb.0.testValue"';
            sendTo('influxdb.0', 'query', query, result => {
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

                sendTo('influxdb.0', 'query', 'SHOW FIELD KEYS FROM "system.adapter.influxdb.0.memHeapTotal"', result2 => {
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

                    sendTo('influxdb.0', 'query', 'SHOW FIELD KEYS FROM "system.adapter.influxdb.0.uptime"', result3 => {
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

                        setTimeout(() =>
                            done(), 3000);
                    });
                });
            });
        }, 60000);
    });

    it(`Test ${adapterShortName}: Check that storageType is set now for memHeapUsed`, function (done) {
        this.timeout(5000);

        objects.getObject('system.adapter.influxdb.0.memHeapUsed', (err, obj) => {
            expect(err).to.be.null;
            console.log(JSON.stringify(obj, null, 2));
            expect(obj.common.custom['influxdb.0'].storageType).to.be.equal('Number');
            done();
        });
    });

    after(`Test ${adapterShortName} adapter: Stop js-controller`, function (done) {
        this.timeout(12000);

        setup.stopController(normalTerminated => {
            console.log(`Adapter normal terminated: ${normalTerminated}`);
            setTimeout(done, 2000);
        });
    });
});
