/* jshint -W097 */
/* jshint strict: false */
/* jslint node: true */
/* jshint expr: true */
'use strict';

let now;
let preInitTime;
let objects = null;
let states = null;

function setStateAsync(id, state) {
    if (!states || !states.setState) {
        return Promise.reject(new Error('No states defined'));
    }

    return new Promise(resolve => {
        states.setState(id, state, err => {
            if (err) {
                console.log(err);
            }
            resolve();
        });
    });
}

function getStateAsync(id) {
    if (!states?.setState) {
        return Promise.reject(new Error('No states defined'));
    }

    return new Promise((resolve, reject) => {
        states.getState(id, (err, state) => {
            if (err) {
                console.log(err);
            }
            resolve(state);
        });
    });
}

function setTimeoutAsync(timeout) {
    return new Promise(resolve => {
        setTimeout(() => {
            resolve();
        }, timeout);
    });
}

function calculateIntegralUnit(from, to, states, unitSeconds = 1) {
    if (!states || states.length < 2) {
        return 0;
    }

    // Ensure sorted by timestamp
    states = states
        .slice()
        .sort((a, b) => a.ts - b.ts)
        .filter(s => s.ts >= from && s.ts <= to);
    if (states.length < 2) {
        return 0;
    }
    let integral = 0;

    for (let i = 0; i < states.length - 1; i++) {
        const dt = (states[i + 1].ts - states[i].ts) / 1000; // ms → seconds
        const avgVal = (states[i].val + states[i + 1].val) / 2;
        integral += avgVal * dt;
    }

    // Scale like Flux does
    return integral / unitSeconds;
}

async function preInit(_objects, _states, sendTo, adapterShortName) {
    objects = _objects;
    states = _states;
    preInitTime = Date.now();

    const instanceName = `${adapterShortName}.0`;
    let obj = {
        common: {
            type: 'number',
            role: 'state',
            custom: {},
        },
        type: 'state',
    };
    obj.common.custom[instanceName] = {
        enabled: true,
        changesOnly: true,
        debounce: 0,
        retention: 31536000,
        maxLength: 3,
        changesMinDelta: 0.5,
    };
    await objects.setObjectAsync(`${instanceName}.testValue`, obj);
    delete obj.common.custom;
    // Count changes of `${instanceName}.testValue`
    await objects.setObjectAsync(`${instanceName}.testValueCounter`, obj);
    obj = {
        common: {
            type: 'number',
            role: 'state',
            custom: {},
            def: 0,
        },
        type: 'state',
    };
    obj.common.custom = {};
    obj.common.custom[instanceName] = {
        enabled: true,
        changesOnly: true,
        changesRelogInterval: 10,
        debounceTime: 500,
        retention: 31536000,
        maxLength: 3,
        changesMinDelta: 0.5,
        ignoreBelowNumber: -1,
        ignoreAboveNumber: 100,
        ignoreZero: true,
        aliasId: `${instanceName}.testValueDebounce alias`,
    };
    await objects.setObjectAsync(`${instanceName}.testValueDebounce`, obj);
    obj = {
        common: {
            type: 'number',
            role: 'state',
            custom: {},
        },
        type: 'state',
    };
    obj.common.custom[instanceName] = {
        enabled: true,
        changesOnly: true,
        changesRelogInterval: 10,
        debounceTime: 500,
        retention: 31536000,
        maxLength: 0,
        changesMinDelta: 0.5,
        disableSkippedValueLogging: true,
        ignoreBelowZero: true,
        ignoreAboveNumber: 100,
        storageType: 'Number',
    };
    await objects.setObjectAsync(`${instanceName}.testValueDebounceRaw`, obj);
    obj = {
        common: {
            type: 'number',
            role: 'state',
            custom: {},
        },
        type: 'state',
    };
    obj.common.custom[instanceName] = {
        enabled: true,
        changesOnly: true,
        changesRelogInterval: 10,
        debounceTime: 0,
        blockTime: 1500,
        retention: 31536000,
        maxLength: 3,
        changesMinDelta: 0.5,
        ignoreBelowNumber: -1,
        ignoreAboveNumber: 100,
    };
    await objects.setObjectAsync(`${instanceName}.testValueBlocked`, obj);

    await objects.setObjectAsync('system.adapter.test.0', {
        common: {},
        type: 'instance',
    });
    states.subscribeMessage('system.adapter.test.0');
}

function register(it, expect, sendTo, adapterShortName, writeNulls, assumeExistingData, additionalActiveObjects, testsName) {
    const instanceName = `${adapterShortName}.0`;

    // An expectation that fails inside a sendTo callback is swallowed by the messaging layer: done() is
    // never reached and mocha reports "Timeout of 25000ms exceeded" instead of the value that was wrong.
    // Re-throwing it outside the callback makes it an uncaught exception, which mocha attributes to the
    // test that is running - the same mechanism test/mocha.setup.js uses for unhandled rejections.
    const rawSendTo = sendTo;
    sendTo = (target, command, message, callback) =>
        rawSendTo(target, command, message, (...args) => {
            try {
                return callback(...args);
            } catch (error) {
                setImmediate(() => {
                    throw error;
                });
            }
        });
    if (testsName) {
        adapterShortName = testsName;
    }
    if (writeNulls) {
        adapterShortName += '-writeNulls';
    }
    if (assumeExistingData) {
        adapterShortName += '-existing';
    }

    function sendToAsync(instance, command, message) {
        return new Promise((resolve, reject) => {
            sendTo(instance, command, message, result => {
                if (result.error) {
                    reject(result.error);
                } else {
                    resolve(result);
                }
            });
        });
    }

    it(`Test ${adapterShortName}: Setup test objects after start`, function (done) {
        this.timeout(5000);

        objects.setObject(
            `${instanceName}.testValue2`,
            {
                common: {
                    type: 'number',
                    role: 'state',
                },
                type: 'state',
            },
            () =>
                sendTo(
                    instanceName,
                    'enableHistory',
                    {
                        id: `${instanceName}.testValue2`,
                        options: {
                            changesOnly: true,
                            debounce: 0,
                            retention: 31536000,
                            maxLength: 0,
                            changesMinDelta: 0.5,
                            aliasId: `${instanceName}.testValue2-alias`,
                        },
                    },
                    result => {
                        expect(result.error).to.be.undefined;
                        expect(result.success).to.be.true;
                        // wait till the adapter receives the new settings
                        setTimeout(() => done(), 2000);
                    },
                ),
        );
    });

    it(`Test ${adapterShortName}: Check Enabled Points after Enable`, function (done) {
        this.timeout(5000);

        sendTo(instanceName, 'getEnabledDPs', {}, result => {
            console.log(JSON.stringify(result));
            expect(Object.keys(result).length).to.be.equal(5 + additionalActiveObjects);
            expect(result[`${instanceName}.testValue`].enabled).to.be.true;
            done();
        });
    });

    it(`Test ${adapterShortName}: Write values into DB`, function (done) {
        this.timeout(25000);
        now = Date.now();

        (async () => {
            let state = await getStateAsync(`${instanceName}.testValueCounter`);
            if (!state?.val) {
                state = { val: 0 };
            }
            await setStateAsync(`${instanceName}.testValue`, { val: 1, ts: now + 1000 });
            state.val++;
            await setTimeoutAsync(100);
            await setStateAsync(`${instanceName}.testValue`, { val: 2, ts: now + 10000 });
            state.val++;
            await setTimeoutAsync(100);
            await setStateAsync(`${instanceName}.testValue`, { val: 2, ts: now + 13000 });
            state.val++;
            await setTimeoutAsync(100);
            await setStateAsync(`${instanceName}.testValue`, { val: 2, ts: now + 15000 });
            state.val++;
            await setTimeoutAsync(100);
            await setStateAsync(`${instanceName}.testValue`, { val: 2.2, ts: now + 16000 });
            state.val++;
            await setTimeoutAsync(100);
            await setStateAsync(`${instanceName}.testValue`, { val: 2.5, ts: now + 17000 });
            state.val++;
            await setTimeoutAsync(100);
            await setStateAsync(`${instanceName}.testValue`, { val: '+003.00', ts: now + 19000 });
            state.val++;
            await setStateAsync(`${instanceName}.testValueCounter`, { val: state.val });
            await setTimeoutAsync(100);
            await setStateAsync(`${instanceName}.testValue2`, { val: 1, ts: now + 12000 });
            await setTimeoutAsync(100);
            await setStateAsync(`${instanceName}.testValue2`, { val: 3, ts: now + 19000 });
            await setTimeoutAsync(1000);
        })().then(() => done(), done);
    });

    it(`Test ${adapterShortName}: Read values from DB using GetHistory`, function (done) {
        this.timeout(25000);

        (async () => {
            try {
                let result = await sendToAsync(instanceName, 'getHistory', {
                    id: `${instanceName}.testValue`,
                    options: {
                        start: now,
                        end: now + 30000,
                        count: 50,
                        aggregate: 'none',
                    },
                });
                console.log(JSON.stringify(result.result, null, 2));
                expect(result.result.length).to.be.at.least(4);
                let found = 0;
                for (let i = 0; i < result.result.length; i++) {
                    if (result.result[i].val >= 1 && result.result[i].val <= 3) {
                        found++;
                    }
                }
                expect(found).to.be.equal(5); // additionally, null value by start of adapter.

                result = await sendToAsync(instanceName, 'getHistory', {
                    id: `${instanceName}.testValue`,
                    options: {
                        start: now,
                        end: now + 30000,
                        count: 2,
                        aggregate: 'none',
                    },
                });
                console.log(JSON.stringify(result.result, null, 2));
                expect(result.result.length).to.be.equal(2);
                found = 0;
                for (let i = 0; i < result.result.length; i++) {
                    if (result.result[i].val >= 1 && result.result[i].val <= 3) {
                        found++;
                    }
                }
                expect(found).to.be.equal(2);
                expect(result.result[0].id).to.be.undefined;

                const latestTs = result.result[result.result.length - 1].ts;

                result = await sendToAsync(instanceName, 'getHistory', {
                    id: `${instanceName}.testValue`,
                    options: {
                        start: now,
                        end: now + 30000,
                        count: 2,
                        aggregate: 'none',
                        addId: true,
                        returnNewestEntries: true,
                    },
                });
                console.log(JSON.stringify(result.result, null, 2));
                expect(result.result.length).to.be.equal(2);
                found = 0;
                for (let i = 0; i < result.result.length; i++) {
                    if (result.result[i].val >= 2.5 && result.result[i].val <= 3) {
                        found++;
                    }
                }
                expect(found).to.be.equal(2);
                expect(result.result[0].ts >= latestTs).to.be.true;
                expect(result.result[0].id).to.be.equal(`${instanceName}.testValue`);
            } catch (err) {
                console.error(err);
            }
        })().then(() => done(), done);
    });

    it(`Test ${adapterShortName}: Read average from DB using GetHistory`, function (done) {
        this.timeout(25000);

        states.getState(`${instanceName}.testValueCounter`, (err, state) => {
            sendTo(
                instanceName,
                'getHistory',
                {
                    id: `${instanceName}.testValue`,
                    options: {
                        start: now + 100,
                        end: now + 30001,
                        count: 2,
                        aggregate: 'average',
                        ignoreNull: true,
                        addId: true,
                    },
                },
                result => {
                    console.log(JSON.stringify(result.result, null, 2));
                    console.log(`Expected counter: ${state.val}`);
                    if (instanceName !== 'influxdb.0') {
                        expect(result.result.length).to.be.equal(4);
                        expect(result.result[1].val).to.be.equal(1.5);
                        expect(result.result[2].val).to.be.equal(2.57);
                        expect(result.result[3].val).to.be.equal(2.57);
                    } else {
                        expect(result.result.length).to.be.within(4, 5);
                        expect(result.result[1].val).to.be.within(1, 1.5);
                        expect(result.result[2].val).to.be.within(2, 3);
                        expect(result.result[3].val).to.be.within(2, 3);
                    }
                    expect(result.result[0].id).to.be.equal(`${instanceName}.testValue`);
                    done();
                },
            );
        });
    });

    it(`Test ${adapterShortName}: Read minmax values from DB using GetHistory`, function (done) {
        this.timeout(10000);

        sendTo(
            instanceName,
            'getHistory',
            {
                id: `${instanceName}.testValue`,
                options: {
                    start: now - 30000,
                    end: now + 30000,
                    count: 4,
                    aggregate: 'minmax',
                    addId: true,
                },
            },
            result => {
                console.log(JSON.stringify(result.result, null, 2));
                expect(result.result.length).to.be.at.least(4);
                expect(result.result[0].id).to.be.equal(`${instanceName}.testValue`);
                done();
            },
        );
    });

    it(`Test ${adapterShortName}: Read values from DB using GetHistory for aliased testValue2`, function (done) {
        this.timeout(25000);

        sendTo(
            instanceName,
            'getHistory',
            {
                id: `${instanceName}.testValue2`,
                options: {
                    start: now,
                    end: now + 30000,
                    count: 50,
                    aggregate: 'none',
                },
            },
            result => {
                console.log(JSON.stringify(result.result, null, 2));
                expect(result.result.length).to.be.equal(2);

                sendTo(
                    instanceName,
                    'getHistory',
                    {
                        id: `${instanceName}.testValue2-alias`,
                        options: {
                            start: now,
                            end: now + 30000,
                            count: 50,
                            aggregate: 'none',
                        },
                    },
                    result2 => {
                        console.log(JSON.stringify(result2.result, null, 2));
                        expect(result2.result.length).to.be.equal(2);
                        for (let i = 0; i < result2.result.length; i++) {
                            expect(result2.result[i].val).to.be.equal(result.result[i].val);
                        }

                        done();
                    },
                );
            },
        );
    });

    async function logSampleData(stateId, waitMultiplier) {
        waitMultiplier ||= 1;
        await states.setStateAsync(stateId, { val: 1 }); // expect logged
        await setTimeoutAsync(600 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 2 }); // Expect not logged debounce
        await setTimeoutAsync(20 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 2.1 }); // Expect not logged debounce
        await setTimeoutAsync(20 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 1.5 }); // Expect not logged debounce
        await setTimeoutAsync(20 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 2.3 }); // Expect not logged debounce
        await setTimeoutAsync(20 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 2.5 }); // Expect not logged debounce
        await setTimeoutAsync(600 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 2.9 }); // Expect logged skipped
        await setTimeoutAsync(600 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 3.0 }); // Expect logged
        await setTimeoutAsync(600 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 4 }); // Expect logged
        await setTimeoutAsync(600 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 4.4 }); // expect logged skipped
        await setTimeoutAsync(600 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 5 }); // expect logged
        await setTimeoutAsync(20 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 5 }); // expect not logged debounce
        await setTimeoutAsync(600 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 5 }); // expect logged skipped
        await setTimeoutAsync(600 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 6 }); // expect logged
        await setTimeoutAsync(10100);
        for (let i = 1; i < 10; i++) {
            await states.setStateAsync(stateId, { val: 6 + i * 0.05 }); // expect logged skipped
            await setTimeoutAsync(70 * waitMultiplier);
        }
        await states.setStateAsync(stateId, { val: 7 }); // expect logged
        await setTimeoutAsync(5000);
        await states.setStateAsync(stateId, { val: -5 }); // expect not logged, too low
        await states.setStateAsync(stateId, { val: 101 }); // expect not logged, too high
        await setTimeoutAsync(7000);
    }

    it(`Test ${adapterShortName}: Write debounced Raw values into DB`, async function () {
        this.timeout(45000);
        now = Date.now();

        try {
            await logSampleData(`${instanceName}.testValueDebounceRaw`);
        } catch (err) {
            console.log(err);
            expect(err).to.be.not.ok;
        }

        return new Promise(resolve => {
            sendTo(
                instanceName,
                'getHistory',
                {
                    id: `${instanceName}.testValueDebounceRaw`,
                    options: {
                        start: now,
                        end: Date.now(),
                        count: 50,
                        aggregate: 'none',
                    },
                },
                result => {
                    console.log(JSON.stringify(result.result, null, 2));
                    expect(result.result.length).to.be.at.least(9);
                    expect(result.result[0].val).to.be.equal(1);
                    expect(result.result[1].val).to.be.equal(2.5);
                    expect(result.result[2].val).to.be.equal(3.0);
                    expect(result.result[3].val).to.be.equal(4);
                    expect(result.result[4].val).to.be.equal(5);
                    expect(result.result[5].val).to.be.equal(6);
                    expect(result.result[6].val).to.be.equal(6);
                    expect(result.result[7].val).to.be.equal(7);
                    expect(result.result[8].val).to.be.equal(7);

                    setTimeout(resolve, 2000);
                },
            );
        });
    });

    it(`Test ${adapterShortName}: Write debounced values into DB`, async function () {
        this.timeout(45000);
        now = Date.now();

        try {
            await logSampleData(`${instanceName}.testValueDebounce`);
        } catch (err) {
            console.log(err);
            expect(err).to.be.not.ok;
        }

        return new Promise(resolve => {
            sendTo(
                instanceName,
                'getHistory',
                {
                    id: `${instanceName}.testValueDebounce alias`,
                    options: {
                        start: now,
                        end: Date.now(),
                        count: 50,
                        aggregate: 'none',
                    },
                },
                result => {
                    console.log(JSON.stringify(result.result, null, 2));
                    expect(result.result.length).to.be.at.least(12);

                    const expectedVals = [1, 2.5, 3, 4, 5, 5, 6, 7, 7];
                    let expectedId = 0;
                    for (let i = 0; i < result.result.length; i++) {
                        console.log(
                            `${i}: check ${result.result[i].val} vs ${expectedVals[expectedId]} (${expectedId})`,
                        );
                        expect(result.result[i].val).to.be.lessThanOrEqual(expectedVals[expectedId]);
                        if (result.result[i].val === expectedVals[expectedId] && expectedId < expectedVals.length - 1) {
                            expectedId++;
                        }
                    }
                    expect(expectedId).to.be.equal(expectedVals.length - 1);

                    resolve();
                },
            );
        });
    });

    it(`Test ${adapterShortName}: Read percentile 50+95 values from DB using GetHistory`, function (done) {
        this.timeout(15000);

        sendTo(
            instanceName,
            'getHistory',
            {
                id: `${instanceName}.testValueDebounce alias`,
                options: {
                    start: now,
                    end: Date.now(),
                    count: 1,
                    aggregate: 'percentile',
                    percentile: 50,
                    removeBorderValues: true,
                    addId: true,
                },
            },
            result => {
                console.log(JSON.stringify(result.result, null, 2));
                if (instanceName !== 'influxdb.0') {
                    expect(result.result.length).to.be.equal(1);
                    expect(result.result[0].val).to.be.equal(5);
                    expect(result.result[0].id).to.be.equal(`${instanceName}.testValueDebounce alias`);
                } else {
                    if (process.env.INFLUXDB2) {
                        expect(result.result.length).to.be.within(1, 3);
                        expect(result.result[1] ? result.result[1].val : result.result[0].val).to.be.within(5, 7);
                    } else {
                        expect(result.result.length).to.be.within(1, 2);
                        expect(result.result[1] ? result.result[1].val : result.result[0].val).to.be.within(5, 7);
                    }
                    expect(result.result[0].id).to.be.equal(`${instanceName}.testValueDebounce alias`);
                }

                sendTo(
                    instanceName,
                    'getHistory',
                    {
                        id: `${instanceName}.testValueDebounce alias`,
                        options: {
                            start: now,
                            end: Date.now(),
                            count: 1,
                            aggregate: 'percentile',
                            percentile: 95,
                            removeBorderValues: true,
                            addId: true,
                        },
                    },
                    result => {
                        console.log(JSON.stringify(result.result, null, 2));
                        if (instanceName !== 'influxdb.0') {
                            expect(result.result.length).to.be.equal(1);
                            expect(result.result[0].val).to.be.equal(7);
                        } else {
                            expect(result.result.length).to.be.within(1, 3);
                            expect(result.result[result.result.length - 1].val).to.be.equal(7);
                            expect(result.result[0].id).to.be.equal(`${instanceName}.testValueDebounce alias`);
                        }

                        expect(result.result[0].id).to.be.equal(`${instanceName}.testValueDebounce alias`);
                        done();
                    },
                );
            },
        );
    });

    it(`Test ${adapterShortName}: Read integral from DB using GetHistory`, function (done) {
        this.timeout(25000);

        sendTo(
            instanceName,
            'getHistory',
            {
                id: `${instanceName}.testValueDebounce`,
                options: {
                    start: now,
                    end: Date.now(),
                    count: 5,
                    aggregate: 'integral',
                    integralUnit: 5,
                    removeBorderValues: true,
                    addId: true,
                },
            },
            result => {
                console.log(JSON.stringify(result.result, null, 2));
                // Flux aligns the windows of `window(every: <step>)` to the epoch, not to the queried
                // range, and the adapter queries one step more at each end for the chart. Whether those
                // two extra buckets fall outside the requested range - and are dropped again by
                // removeBorderValues - depends on where "now" sits on that grid, so the result holds
                // count..count+2 buckets. Asserting an exact number makes the test fail at random.
                expect(result.result.length).to.be.within(3, 7);
                expect(result.result[0].id).to.be.equal(`${instanceName}.testValueDebounce alias`);
                done();
            },
        );
    });

    it(`Test ${adapterShortName}: Read linear integral from DB using GetHistory`, function (done) {
        this.timeout(25000);

        sendTo(
            instanceName,
            'getHistory',
            {
                id: `${instanceName}.testValueDebounce`,
                options: {
                    start: now,
                    end: Date.now(),
                    count: 5,
                    aggregate: 'integral',
                    integralUnit: 5,
                    integralInterpolation: 'linear',
                    removeBorderValues: true,
                    addId: true,
                },
            },
            result => {
                console.log(JSON.stringify(result.result, null, 2));
                // see the comment in the test above: count..count+2 buckets, depending on the epoch grid
                expect(result.result.length).to.be.within(3, 7);
                expect(result.result[0].id).to.be.equal(`${instanceName}.testValueDebounce alias`);
                done();
            },
        );
    });

    it(`Test ${adapterShortName}: Write with 1s block values into DB`, async function () {
        this.timeout(45000);
        now = Date.now();

        try {
            await logSampleData(`${instanceName}.testValueBlocked`, 1.5);
        } catch (err) {
            console.log(err);
            expect(err).to.be.not.ok;
        }

        return new Promise(resolve => {
            sendTo(
                instanceName,
                'getHistory',
                {
                    id: `${instanceName}.testValueBlocked`,
                    options: {
                        start: now,
                        end: Date.now(),
                        count: 50,
                        aggregate: 'none',
                    },
                },
                result => {
                    console.log(JSON.stringify(result.result, null, 2));
                    expect(result.result.length).to.be.at.least(9);
                    expect(result.result[0].val).to.be.equal(1);
                    expect(result.result[1].val).to.be.at.least(2.3);
                    expect(result.result[2].val).to.be.equal(4);
                    expect(result.result[3].val).to.be.equal(5);
                    expect(result.result[4].val).to.be.equal(6);
                    expect(result.result[5].val).to.be.equal(6);
                    expect(result.result[6].val).to.be.equal(6.45);
                    expect(result.result[7].val).to.be.equal(7);
                    expect(result.result[8].val).to.be.equal(7);

                    resolve();
                },
            );
        });
    });

    it(`Test ${adapterShortName}: Tests with more sample data`, async function () {
        this.timeout(60000);
        const now = new Date();
        const nowSampleI1 = now.getTime() - 29 * 3_600_000;
        const nowSampleI21 = now.getTime() - 28 * 3_600_000;
        const nowSampleI22 = now.getTime() - 27 * 3_600_000;
        const nowSampleI23 = now.getTime() - 26 * 3_600_000;
        const nowSampleI24 = now.getTime() - 25 * 3_600_000;

        let counterState = getStateAsync(`${instanceName}.testValueCounter`);

        const states = [
            { val: 2.064, ack: true, ts: nowSampleI1 }, // 1s = 3732.66
            { val: 2.116, ack: true, ts: nowSampleI1 + 6 * 60_000 },
            { val: 2.028, ack: true, ts: nowSampleI1 + 12 * 60_000 },
            { val: 2.126, ack: true, ts: nowSampleI1 + 18 * 60_000 },
            { val: 2.041, ack: true, ts: nowSampleI1 + 24 * 60_000 },
            { val: 2.051, ack: true, ts: nowSampleI1 + 30 * 60_000 },

            { val: -2, ack: true, ts: nowSampleI21 }, // 10s none = 50.0
            { val: 10, ack: true, ts: nowSampleI21 + 10 * 1000 },
            { val: 7, ack: true, ts: nowSampleI21 + 20 * 1000 },
            { val: 17, ack: true, ts: nowSampleI21 + 30 * 1000 },
            { val: 15, ack: true, ts: nowSampleI21 + 40 * 1000 },
            { val: 4, ack: true, ts: nowSampleI21 + 50 * 1000 },

            { val: 19, ack: true, ts: nowSampleI22 }, // 10s none = 43
            { val: 4, ack: true, ts: nowSampleI22 + 10 * 1000 },
            { val: -3, ack: true, ts: nowSampleI22 + 20 * 1000 },
            { val: 19, ack: true, ts: nowSampleI22 + 30 * 1000 },
            { val: 13, ack: true, ts: nowSampleI22 + 40 * 1000 },
            { val: 1, ack: true, ts: nowSampleI22 + 50 * 1000 },

            { val: -2, ack: true, ts: nowSampleI23 }, // 10s linear = 25
            { val: 7, ack: true, ts: nowSampleI23 + 20 * 1000 },
            { val: 4, ack: true, ts: nowSampleI23 + 50 * 1000 },

            { val: 4, ack: true, ts: nowSampleI24 + 10 * 1000 }, // 10s linear = 32.5
            { val: -3, ack: true, ts: nowSampleI24 + 20 * 1000 },
            { val: 19, ack: true, ts: nowSampleI24 + 30 * 1000 },
            { val: 1, ack: true, ts: nowSampleI24 + 50 * 1000 },
        ];
        if (!counterState?.val) {
            counterState = { val: 0 };
        }
        counterState.val += states.length;
        await setStateAsync(`${instanceName}.testValueCounter`, { val: counterState.val });

        let result = await sendToAsync(instanceName, 'storeState', {
            id: `${instanceName}.testValue`,
            state: states,
        });
        expect(result.success).to.be.true;

        await setTimeoutAsync(1000);
        // BF: The tests are very strange, as integral must be equal between all implementations and versions of InfluxDB
        let integral = calculateIntegralUnit(nowSampleI1, nowSampleI1 + 30 * 60_000, states, 1);
        result = await sendToAsync(instanceName, 'getHistory', {
            id: `${instanceName}.testValue`,
            options: {
                start: nowSampleI1,
                end: nowSampleI1 + 30 * 60_000, // 30min
                count: 1,
                aggregate: 'integral',
                removeBorderValues: true,
                integralUnit: 1,
                integralInterpolation: 'none',
            },
        });
        let sum = result.result.map(it => it.val).reduce((acc, val) => acc + val, 0);
        console.log(`Sample I1-1: ${JSON.stringify(result.result, null, 2)} => ${integral} ?? ${sum}`);
        if (instanceName !== 'influxdb.0') {
            expect(result.result.length).to.be.equal(1);
            if (assumeExistingData) {
                expect(result.result[0].val).to.be.within(3700, 3755);
            } else {
                expect(result.result[0].val).to.be.within(3700, 3800);
            }
        } else {
            if (assumeExistingData) {
                expect(result.result.length).to.be.within(1, 3);
                if (process.env.INFLUXDB2) {
                    // expect((result.result[0].val + result.result[1].val)).to.be.within(3780, 4000);
                } else {
                    // expect(result.result[0].val).to.be.within(2980, 3000);
                }
            } else {
                expect(result.result.length).to.be.within(1, 2);
                if (process.env.INFLUXDB2) {
                    expect(sum).to.be.within(2980, 3000);
                } else {
                    expect(parseFloat(sum.toFixed(2))).to.be.equal(3732.66);
                }
            }
        }
        // Result Influxdb1 Doku = 3732.66

        integral = calculateIntegralUnit(nowSampleI1, nowSampleI1 + 30 * 60_000, states, 60);
        result = await sendToAsync(instanceName, 'getHistory', {
            id: `${instanceName}.testValue`,
            options: {
                start: nowSampleI1,
                end: nowSampleI1 + 30 * 60_000,
                count: 1,
                aggregate: 'integral',
                removeBorderValues: true,
                integralUnit: 60,
                integralInterpolation: 'none',
            },
        });
        sum = result.result.map(it => it.val).reduce((acc, val) => acc + val, 0);
        console.log(`Sample I1-60: ${JSON.stringify(result.result, null, 2)} => ${integral} ?? ${sum}`);
        if (instanceName !== 'influxdb.0') {
            expect(result.result.length).to.be.equal(1);
            if (assumeExistingData) {
                expect(result.result[0].val).to.be.lessThan(62.25);
            } else {
                expect(result.result[0].val).to.be.equal(62.25);
            }
        } else {
            expect(result.result.length).to.be.within(1, 3);
            const sum = result.result.map(it => it.val).reduce((acc, val) => acc + val, 0);

            if (process.env.INFLUXDB2) {
                if (assumeExistingData) {
                    expect(parseFloat(sum.toFixed(2))).to.be.within(49, 80);
                } else {
                    expect(parseFloat(sum.toFixed(2))).to.be.within(49, 50);
                }
            } else {
                if (assumeExistingData) {
                    expect(parseFloat(sum.toFixed(2))).to.be.within(60, 90);
                } else {
                    expect(parseFloat(sum.toFixed(2))).to.be.equal(62.21);
                }
            }
        }
        // Result Influxdb1 Doku = 62.211

        integral = calculateIntegralUnit(nowSampleI21, nowSampleI21 + 60_000, states, 10);
        result = await sendToAsync(instanceName, 'getHistory', {
            id: `${instanceName}.testValue`,
            options: {
                start: nowSampleI21,
                end: nowSampleI21 + 60_000,
                count: 1,
                aggregate: 'integral',
                removeBorderValues: true,
                integralUnit: 10,
                integralInterpolation: 'none',
            },
        });
        sum = result.result.map(it => it.val).reduce((acc, val) => acc + val, 0);
        console.log(`Sample I21: ${JSON.stringify(result.result, null, 2)} => ${integral} ?? ${sum}`);
        if (instanceName !== 'influxdb.0') {
            expect(result.result.length).to.be.equal(1);
            expect(result.result[0].val).to.be.equal(51);
        } else {
            const sum = result.result.map(it => it.val).reduce((acc, val) => acc + val, 0);
            expect(result.result.length).to.be.within(1, 2);
            expect(sum).to.be.within(30, 50);
        }
        // Result Influxdb21 Doku = 50.0

        integral = calculateIntegralUnit(nowSampleI22, nowSampleI22 + 60_000, states, 10);
        result = await sendToAsync(instanceName, 'getHistory', {
            id: `${instanceName}.testValue`,
            options: {
                start: nowSampleI22,
                end: nowSampleI22 + 60_000,
                count: 1,
                aggregate: 'integral',
                removeBorderValues: true,
                integralUnit: 10,
                integralInterpolation: 'none',
            },
        });
        sum = result.result.map(it => it.val).reduce((acc, val) => acc + val, 0);

        console.log(`Sample I22: ${JSON.stringify(result.result, null, 2)} => ${integral} ?? ${sum}`);
        if (instanceName !== 'influxdb.0') {
            expect(result.result.length).to.be.equal(1);
            expect(result.result[0].val).to.be.equal(53);
        } else {
            expect(result.result.length).to.be.within(1, 2);
            expect(result.result[0].val + (result.result[1] ? result.result[1].val : 0)).to.be.within(27, 43);
        }
        // Result Influxdb22 Doku = 43

        integral = calculateIntegralUnit(nowSampleI23, nowSampleI23 + 60_000, states, 10);
        result = await sendToAsync(instanceName, 'getHistory', {
            id: `${instanceName}.testValue`,
            options: {
                start: nowSampleI23,
                end: nowSampleI23 + 60_000,
                count: 1,
                aggregate: 'integral',
                removeBorderValues: true,
                integralUnit: 10,
                integralInterpolation: 'linear',
            },
        });
        sum = result.result.map(it => it.val).reduce((acc, val) => acc + val, 0);

        console.log(`Sample I23: ${JSON.stringify(result.result, null, 2)} => ${integral} ?? ${sum}`);
        if (instanceName !== 'influxdb.0') {
            expect(result.result.length).to.be.equal(1);
            expect(result.result[0].val).to.be.equal(25.5);
        } else {
            expect(result.result.length).to.be.within(1, 2);
            if (process.env.INFLUXDB2) {
                //expect(result.result[0].val).to.be.equal(25.5);
            } else {
                expect(result.result[0].val).to.be.equal(34.5);
            }
        }
        // Result Influxdb23 Doku = 25.0

        integral = calculateIntegralUnit(nowSampleI24, nowSampleI24 + 60_000, states, 10);
        result = await sendToAsync(instanceName, 'getHistory', {
            id: `${instanceName}.testValue`,
            options: {
                start: nowSampleI24,
                end: nowSampleI24 + 60_000,
                count: 1,
                aggregate: 'integral',
                removeBorderValues: true,
                integralUnit: 10,
                integralInterpolation: 'linear',
            },
        });
        sum = result.result.map(it => it.val).reduce((acc, val) => acc + val, 0);
        console.log(`Sample I24: ${JSON.stringify(result.result, null, 2)} => ${integral} ?? ${sum}`);

        if (instanceName !== 'influxdb.0') {
            expect(result.result.length).to.be.equal(1);
            if (assumeExistingData) {
                expect(result.result[0].val).to.be.within(31, 32);
            } else {
                expect(result.result[0].val).to.be.within(32, 33.5);
            }
        } else {
            expect(result.result.length).to.be.within(1, 2);
            if (process.env.INFLUXDB2) {
                //expect(result.result[0].val).to.be.equal(25.5);
            } else {
                if (assumeExistingData) {
                    expect(result.result[0].val).to.be.within(31, 34);
                } else {
                    expect(result.result[0].val).to.be.within(32, 33.5);
                }
            }
        }
        // Result Influxdb24 Doku = 32.5

        integral = calculateIntegralUnit(nowSampleI22, nowSampleI22 + 60_000, states, 10);
        result = await sendToAsync(instanceName, 'getHistory', {
            id: `${instanceName}.testValue`,
            options: {
                start: nowSampleI22,
                end: nowSampleI22 + 60_000,
                count: 1,
                aggregate: 'quantile',
                quantile: 0.8,
            },
        });
        sum = result.result.map(it => it.val).reduce((acc, val) => acc + val, 0);
        console.log(`Sample I22-Quantile: ${JSON.stringify(result.result, null, 2)} => ${integral} ?? ${sum}`);
        if (instanceName !== 'influxdb.0') {
            expect(result.result.length).to.be.equal(3);
            expect(result.result[1].val).to.be.equal(19);
        } else {
            expect(result.result.length).to.be.within(3, 4);
            expect(result.result[1].val).to.be.within(4, 19);
        }
    });

    it(`Test ${adapterShortName}: Read data two weeks around now GetHistory`, function (done) {
        this.timeout(25000);

        const start1week = Date.now() - 7 * 24 * 3_600_000;

        sendTo(
            instanceName,
            'getHistory',
            {
                id: `${instanceName}.testValue`,
                options: {
                    start: start1week,
                    end: start1week + 7 * 24 * 3_600_000,
                    step: 24 * 3_600_000,
                    aggregate: 'integral',
                    integralUnit: 3600,
                    addId: true,
                },
            },
            result => {
                console.log(JSON.stringify(result.result, null, 2));
                expect(result.result.length).to.be.equal(4);
                expect(result.result[0].id).to.be.equal(`${instanceName}.testValue`);
                done();
            },
        );
    });

    it(`Test ${adapterShortName}: Remove Alias-ID`, function (done) {
        this.timeout(5000);

        sendTo(
            instanceName,
            'enableHistory',
            {
                id: `${instanceName}.testValue2`,
                options: {
                    aliasId: '',
                },
            },
            result => {
                expect(result.error).to.be.undefined;
                expect(result.success).to.be.true;
                // wait till the adapter receives the new settings
                setTimeout(() => done(), 2000);
            },
        );
    });

    it(`Test ${adapterShortName}: Add Alias-ID again`, function (done) {
        this.timeout(5000);

        sendTo(
            instanceName,
            'enableHistory',
            {
                id: `${instanceName}.testValue2`,
                options: {
                    aliasId: 'this.is.a.test-value',
                },
            },
            result => {
                expect(result.error).to.be.undefined;
                expect(result.success).to.be.true;
                // wait till the adapter receives the new settings
                setTimeout(() => done(), 2000);
            },
        );
    });

    it(`Test ${adapterShortName}: Change Alias-ID`, function (done) {
        this.timeout(5000);

        sendTo(
            instanceName,
            'enableHistory',
            {
                id: `${instanceName}.testValue2`,
                options: {
                    aliasId: 'this.is.another.test-value',
                },
            },
            result => {
                expect(result.error).to.be.undefined;
                expect(result.success).to.be.true;
                // wait till the adapter receives the new settings
                setTimeout(() => done(), 2000);
            },
        );
    });

    it(`Test ${adapterShortName}: Disable Datapoint again`, function (done) {
        this.timeout(5000);

        sendTo(
            instanceName,
            'disableHistory',
            {
                id: `${instanceName}.testValue`,
            },
            result => {
                expect(result.error).to.be.undefined;
                expect(result.success).to.be.true;
                setTimeout(done, 2000);
            },
        );
    });

    it(`Test ${adapterShortName}: Check Enabled Points after Disable`, function (done) {
        this.timeout(5000);

        sendTo(instanceName, 'getEnabledDPs', {}, result => {
            console.log(JSON.stringify(result));
            expect(Object.keys(result).length).to.be.equal(4 + additionalActiveObjects);
            done();
        });
    });

    it(`Test ${adapterShortName}: Enable testValue Datapoint again`, function (done) {
        this.timeout(5000);

        sendTo(
            instanceName,
            'enableHistory',
            {
                id: `${instanceName}.testValue`,
            },
            result => {
                expect(result.error).to.be.undefined;
                expect(result.success).to.be.true;
                setTimeout(done, 2000);
            },
        );
    });

    it(`Test ${adapterShortName}: Check for written Null values`, function (done) {
        this.timeout(25000);

        sendTo(
            instanceName,
            'getHistory',
            {
                id: `${instanceName}.testValue`,
                options: {
                    start: preInitTime,
                    count: 500,
                    aggregate: 'none',
                },
            },
            result => {
                console.log(JSON.stringify(result.result, null, 2));
                expect(result.result.length).to.be.at.least(5);
                let found = 0;
                for (let i = 0; i < result.result.length; i++) {
                    if (result.result[i].val === null) {
                        found++;
                    }
                }
                if (writeNulls) {
                    expect(found).to.be.equal(3);
                } else {
                    expect(found).to.be.equal(0);
                }

                done();
            },
        );
    });

    it(`Test ${adapterShortName}: Check for written Data in general`, function (done) {
        this.timeout(25000);

        sendTo(
            instanceName,
            'getHistory',
            {
                id: `${instanceName}.testValue`,
                options: {
                    count: 500,
                    aggregate: 'none',
                },
            },
            result => {
                console.log(JSON.stringify(result.result, null, 2));
                // 32 without existing data, 37
                expect(result.result.length).to.be.at.least((writeNulls ? 3 : 0) + (assumeExistingData + 1) * 30);

                done();
            },
        );
    });

    it(`Test ${adapterShortName}: Read minmax values from DB using GetHistory with 1mio slices`, function (done) {
        this.timeout(20000);

        sendTo(
            instanceName,
            'getHistory',
            {
                id: `${instanceName}.testValue`,
                options: {
                    start: Date.now() - 7 * 24 * 3_600_000,
                    end: Date.now(),
                    count: 1000000,
                    limit: 1000000,
                    aggregate: 'minmax',
                    addId: true,
                },
            },
            result => {
                console.log(JSON.stringify(result.result, null, 2));
                expect(result.result.length).to.be.at.least(4);
                expect(result.result[0].id).to.be.equal(`${instanceName}.testValue`);
                done();
            },
        );
    });

    it(`Test ${adapterShortName}: storeState and getHistory for unknown Id`, function (done) {
        this.timeout(25000);

        const customNow = Date.now();
        sendTo(
            instanceName,
            'storeState',
            {
                id: `my.own.unknown.value-${customNow}`,
                state: [
                    { val: 1, ack: true, ts: customNow - 5000 },
                    { val: 2, ack: true, ts: customNow - 4000 },
                    { val: 3, ack: true, ts: customNow - 3000 },
                ],
            },
            result => {
                expect(result.success).to.be.true;
                expect(result.successCount).to.be.equal(3);

                setTimeout(() => {
                    sendTo(
                        instanceName,
                        'getHistory',
                        {
                            id: `my.own.unknown.value-${customNow}`,
                            options: {
                                start: customNow - 10000,
                                count: 500,
                                aggregate: 'none',
                            },
                        },
                        result => {
                            console.log(JSON.stringify(result.result, null, 2));
                            expect(result.result.length).to.be.equal(3);

                            done();
                        },
                    );
                }, 1000);
            },
        );
    });

    it(`Test ${adapterShortName}: storeState error for unknown Id with rules parameter`, function (done) {
        this.timeout(25000);

        const customNow2 = Date.now();
        sendTo(
            instanceName,
            'storeState',
            {
                id: `my.own.unknown.value-${customNow2}`,
                rules: true,
                state: [
                    { val: 1, ack: true, ts: customNow2 - 5000 },
                    { val: 2, ack: true, ts: customNow2 - 4000 },
                    '37',
                ],
            },
            result => {
                expect(result.success).to.be.not.ok;
                expect(result.successCount).to.be.equal(0);
                expect(result.error).to.be.equal('3 errors happened while storing data');
                expect(Array.isArray(result.errors)).to.be.true;
                expect(
                    result.errors[0].endsWith(
                        ` not enabled for my.own.unknown.value-${customNow2}, so can not apply the rules as requested`,
                    ),
                ).to.be.true;
                expect(result.errors[2]).to.be.equal(`State "37" for my.own.unknown.value-${customNow2} is not valid`);

                done();
            },
        );
    });

    // Custom tags (https://github.com/ioBroker/ioBroker.influxdb/issues/32). Kept at the end, because the
    // tests above count the enabled datapoints
    const taggedId = `${instanceName}.testValueTagged`;
    let taggedNow;

    /** Read the raw points of the tagged datapoint, optionally only those with the given tags */
    async function queryTaggedPoints(tags) {
        const adapterObj = await objects.getObjectAsync(`system.adapter.${instanceName}`);
        const dbname = adapterObj.native.dbname || 'iobroker';
        const start = taggedNow - 10000;
        const stop = taggedNow + 10000;
        let query;
        if (process.env.INFLUXDB2) {
            const tagFilter = Object.keys(tags)
                .map(name => ` and r["${name}"] == "${tags[name]}"`)
                .join('');
            query =
                `from(bucket: "${dbname}") |> range(start: ${new Date(start).toISOString()}, stop: ${new Date(stop).toISOString()}) ` +
                `|> filter(fn: (r) => r["_measurement"] == "${taggedId}" and r["_field"] == "value"${tagFilter}) ` +
                `|> group() |> sort(columns: ["_time"])`;
        } else {
            const tagFilter = Object.keys(tags)
                .map(name => ` AND "${name}" = '${tags[name]}'`)
                .join('');
            query = `SELECT * FROM "${taggedId}" WHERE time >= ${start}ms AND time <= ${stop}ms${tagFilter}`;
        }
        const result = await sendToAsync(instanceName, 'query', query);
        console.log(`${query}\n=> ${JSON.stringify(result.result)}`);
        return result.result[0] || [];
    }

    it(`Test ${adapterShortName}: Enable datapoint with custom tags`, async function () {
        this.timeout(10000);

        await objects.setObjectAsync(taggedId, {
            common: { type: 'number', role: 'state' },
            type: 'state',
        });
        const result = await sendToAsync(instanceName, 'enableHistory', {
            id: taggedId,
            options: {
                changesOnly: false,
                debounceTime: 0,
                blockTime: 0,
                disableSkippedValueLogging: true,
                customTags: [
                    { name: 'room', value: 'kitchen' },
                    { name: ' device ', value: 'sensor 1' },
                    // ignored: reserved name, an empty row (as added by "+" in the admin), a name without value
                    { name: 'q', value: 'reserved' },
                    { name: '', value: '' },
                    { name: 'empty', value: '' },
                ],
            },
        });
        expect(result.success).to.be.true;
        // wait till the adapter receives the new settings
        await setTimeoutAsync(2000);
    });

    it(`Test ${adapterShortName}: Check normalized custom tags of the enabled datapoint`, async function () {
        this.timeout(5000);

        const result = await sendToAsync(instanceName, 'getEnabledDPs', {});
        expect(result[taggedId]).to.be.ok;
        expect(result[taggedId].customTags).to.be.deep.equal({ room: 'kitchen', device: 'sensor 1' });
    });

    it(`Test ${adapterShortName}: Write values with custom tags into DB`, async function () {
        this.timeout(10000);

        taggedNow = Date.now();
        await setStateAsync(taggedId, { val: 1, ts: taggedNow - 3000, ack: true, from: 'test.0' });
        await setTimeoutAsync(100);
        await setStateAsync(taggedId, { val: 2, ts: taggedNow - 2000, ack: true, from: 'test.0' });
        await setTimeoutAsync(100);
        await setStateAsync(taggedId, { val: 3, ts: taggedNow - 1000, ack: true, from: 'test.0' });
        await setTimeoutAsync(500);
        await sendToAsync(instanceName, 'flushBuffer', {});
    });

    it(`Test ${adapterShortName}: Read values filtered by custom tags from DB`, async function () {
        this.timeout(10000);

        const rows = await queryTaggedPoints({ room: 'kitchen', device: 'sensor 1' });
        // the initial value written on enabling has no timestamp in this range in every suite, so only
        // count the values written above
        const values = rows.map(row => row.value ?? row._value).filter(val => val >= 1 && val <= 3);
        expect(values).to.be.deep.equal([1, 2, 3]);
        for (const row of rows) {
            expect(row.room).to.be.equal('kitchen');
            expect(row.device).to.be.equal('sensor 1');
            expect(row.empty).to.be.not.ok;
        }

        const otherRows = await queryTaggedPoints({ room: 'bedroom' });
        expect(otherRows.length).to.be.equal(0);
    });

    it(`Test ${adapterShortName}: Read values with custom tags using GetHistory`, async function () {
        this.timeout(10000);

        const result = await sendToAsync(instanceName, 'getHistory', {
            id: taggedId,
            options: {
                start: taggedNow - 5000,
                end: taggedNow,
                count: 50,
                aggregate: 'none',
                removeBorderValues: true,
            },
        });
        console.log(JSON.stringify(result.result));
        expect(result.result.map(entry => entry.val)).to.be.deep.equal([1, 2, 3]);
    });

    it(`Test ${adapterShortName}: Remove all custom tags`, async function () {
        this.timeout(10000);

        const result = await sendToAsync(instanceName, 'enableHistory', {
            id: taggedId,
            options: { customTags: [] },
        });
        expect(result.success).to.be.true;
        await setTimeoutAsync(2000);

        const enabled = await sendToAsync(instanceName, 'getEnabledDPs', {});
        expect(enabled[taggedId].customTags).to.be.deep.equal({});

        await setStateAsync(taggedId, { val: 4, ts: taggedNow + 1000, ack: true, from: 'test.0' });
        await setTimeoutAsync(500);
        await sendToAsync(instanceName, 'flushBuffer', {});

        const taggedRows = await queryTaggedPoints({ room: 'kitchen' });
        expect(taggedRows.map(row => row.value ?? row._value)).to.not.include(4);
        const allRows = await queryTaggedPoints({});
        const untagged = allRows.filter(row => (row.value ?? row._value) === 4);
        expect(untagged.length).to.be.equal(1);
        expect(untagged[0].room).to.be.not.ok;
    });

    it(`Test ${adapterShortName}: Disable datapoint with custom tags`, async function () {
        this.timeout(5000);

        const result = await sendToAsync(instanceName, 'disableHistory', { id: taggedId });
        expect(result.success).to.be.true;
        await setTimeoutAsync(1000);
    });

    // Statistics tab (getDpStatistics / cleanupOrphaned). Kept at the very end: the tests above
    // count the enabled datapoints, and the orphan created here must not disturb them
    const orphanId = `${instanceName}.testValueOrphan`;

    /** Index the statistics by datapoint ID, so a test can look at the one it cares about */
    async function readStatistics(message) {
        const result = await sendToAsync(instanceName, 'getDpStatistics', message || {});
        expect(result.success).to.be.true;
        expect(result.result).to.be.an('array');
        const byId = {};
        result.result.forEach(row => (byId[row.id] = row));
        return { ...result, byId };
    }

    it(`Test ${adapterShortName}: Read datapoint statistics`, async function () {
        this.timeout(60000);

        const stats = await readStatistics();
        const testValue = stats.byId[`${instanceName}.testValue`];
        expect(testValue, 'the statistics must contain testValue').to.be.ok;
        expect(testValue.count).to.be.above(0);
        expect(testValue.status).to.be.equal('active');
        expect(testValue.type).to.be.equal('Number');
        expect(testValue.firstTs).to.be.a('number');
        expect(testValue.lastTs).to.be.at.least(testValue.firstTs);
        // every datapoint of the tests is written without custom tags at this point, so one series
        expect(testValue.cardinality).to.be.at.least(1);

        // the summary has to agree with the rows it summarizes
        expect(stats.summary.datapoints).to.be.equal(stats.result.length);
        expect(stats.summary.values).to.be.equal(stats.result.reduce((sum, row) => sum + row.count, 0));
        expect(stats.summary.byStatus.active.datapoints).to.be.above(0);
    });

    it(`Test ${adapterShortName}: Statistics honour the examined time range`, async function () {
        this.timeout(60000);

        // a range far behind everything the tests write - the datapoints stay listed, but empty
        const future = await readStatistics({ start: Date.now() + 365 * 24 * 3600000, end: Date.now() + 366 * 24 * 3600000 });
        expect(future.result.length).to.be.above(0);
        expect(future.summary.values).to.be.equal(0);
        for (const row of future.result) {
            expect(row.count, `${row.id} must have no value in an empty range`).to.be.equal(0);
            expect(row.firstTs).to.be.null;
            expect(row.lastTs).to.be.null;
        }
    });

    it(`Test ${adapterShortName}: A deleted state keeps its data and is reported as orphaned`, async function () {
        this.timeout(60000);

        await objects.setObjectAsync(orphanId, {
            common: { type: 'number', role: 'state' },
            type: 'state',
        });
        await sendToAsync(instanceName, 'enableHistory', {
            id: orphanId,
            options: { changesOnly: false, debounceTime: 0, blockTime: 0, disableSkippedValueLogging: true },
        });
        await setTimeoutAsync(2000);

        await setStateAsync(orphanId, { val: 42, ts: Date.now(), ack: true, from: 'test.0' });
        await setTimeoutAsync(500);
        await sendToAsync(instanceName, 'flushBuffer', {});

        // while it is logged and the object exists it counts as active
        const active = await readStatistics();
        expect(active.byId[orphanId], 'the new datapoint must be listed').to.be.ok;
        expect(active.byId[orphanId].status).to.be.equal('active');
        expect(active.byId[orphanId].count).to.be.above(0);

        // switching the logging off is not the same as deleting the state
        await sendToAsync(instanceName, 'disableHistory', { id: orphanId });
        await setTimeoutAsync(2000);
        const disabled = await readStatistics();
        expect(disabled.byId[orphanId].status).to.be.equal('loggingDisabled');

        // now the state itself disappears, its values stay in the database
        await new Promise(resolve => objects.delObject(orphanId, () => resolve()));
        await setTimeoutAsync(2000);
        const orphaned = await readStatistics();
        expect(orphaned.byId[orphanId], 'the data of a deleted state stays listed').to.be.ok;
        expect(orphaned.byId[orphanId].status).to.be.equal('objectMissing');
        expect(orphaned.byId[orphanId].count).to.be.above(0);
    });

    it(`Test ${adapterShortName}: Cleanup reports the orphan without deleting it`, async function () {
        this.timeout(60000);

        const preview = await sendToAsync(instanceName, 'cleanupOrphaned', {});
        expect(preview.success).to.be.true;
        expect(preview.dryRun).to.be.true;
        expect(preview.items.map(item => item.id)).to.include(orphanId);
        expect(preview.datapoints).to.be.equal(preview.items.length);
        expect(preview.values).to.be.equal(preview.items.reduce((sum, item) => sum + item.count, 0));
        // a dry run must never select a datapoint that is still being logged
        expect(preview.items.find(item => item.status === 'active')).to.be.undefined;

        // without `confirm` nothing may be gone
        const stats = await readStatistics();
        expect(stats.byId[orphanId], 'a dry run must not delete anything').to.be.ok;
    });

    it(`Test ${adapterShortName}: Cleanup keeps a datapoint whose logging is only switched off`, async function () {
        this.timeout(60000);

        // taggedId still exists as a state, its logging was switched off further up
        const defaultScope = await sendToAsync(instanceName, 'cleanupOrphaned', {});
        expect(defaultScope.items.map(item => item.id)).to.not.include(taggedId);

        const withDisabled = await sendToAsync(instanceName, 'cleanupOrphaned', {
            scope: { objectMissing: true, loggingDisabled: true },
        });
        expect(withDisabled.dryRun).to.be.true;
        expect(withDisabled.items.map(item => item.id)).to.include(taggedId);
        expect(withDisabled.items.map(item => item.id)).to.include(orphanId);
    });
}

module.exports = {
    register,
    preInit,
};
