/* jshint -W097 */// jshint strict:false
/*jslint node: true */
/*jshint expr: true*/
var expect = require('chai').expect;
var setup  = require(__dirname + '/lib/setup');

var objects = null;
var states  = null;
var onStateChanged = null;
var onObjectChanged = null;
var sendToID = 1;

var adapterShortName = setup.adapterName.substring(setup.adapterName.indexOf('.')+1);

var now;

function checkConnectionOfAdapter(cb, counter) {
    counter = counter || 0;
    console.log('Try check #' + counter);
    if (counter > 30) {
        if (cb) cb('Cannot check connection');
        return;
    }

    console.log('Checking alive key for key : ' + adapterShortName);
    states.getState('system.adapter.' + adapterShortName + '.0.alive', function (err, state) {
        if (err) console.error(err);
        if (state && state.val) {
            if (cb) cb();
        } else {
            setTimeout(function () {
                checkConnectionOfAdapter(cb, counter + 1);
            }, 1000);
        }
    });
}

function checkValueOfState(id, value, cb, counter) {
    counter = counter || 0;
    if (counter > 20) {
        if (cb) cb('Cannot check value Of State ' + id);
        return;
    }

    states.getState(id, function (err, state) {
        if (err) console.error(err);
        if (value === null && !state) {
            if (cb) cb();
        } else
        if (state && (value === undefined || state.val === value)) {
            if (cb) cb();
        } else {
            setTimeout(function () {
                checkValueOfState(id, value, cb, counter + 1);
            }, 500);
        }
    });
}

function sendTo(target, command, message, callback) {
    onStateChanged = function (id, state) {
        if (id === 'messagebox.system.adapter.test.0') {
            callback(state.message);
        }
    };

    states.pushMessage('system.adapter.' + target, {
        command:    command,
        message:    message,
        from:       'system.adapter.test.0',
        callback: {
            message: message,
            id:      sendToID++,
            ack:     false,
            time:    (new Date()).getTime()
        }
    });
}

describe('Test ' + adapterShortName + ' adapter with Buffered write', function() {
    before('Test ' + adapterShortName + ' adapter: Start js-controller', function (_done) {
        this.timeout(600000); // because of first install from npm

        setup.setupController(function () {
            var config = setup.getAdapterConfig();
            // enable adapter
            config.common.enabled  = true;
            config.common.loglevel = 'debug';

            config.native.seriesBufferMax = 3;
            config.native.dbname = 'otheriobroker';

            //config.native.dbtype   = 'sqlite';

            setup.setAdapterConfig(config.common, config.native);

            setup.startController(true, function(id, obj) {}, function (id, state) {
                    if (onStateChanged) onStateChanged(id, state);
                },
                function (_objects, _states) {
                    objects = _objects;
                    states  = _states;
                    objects.setObject('influxdb.0.memRss', {
                        common: {
                            type: 'number',
                            role: 'state',
                            custom: {
                                "influxdb.0": {
                                    enabled: true,
                                    changesOnly:  true,
                                    debounce:     0,
                                    retention:    31536000,
                                    maxLength:    3,
                                    changesMinDelta: 0.5
                                }
                            }
                        },
                        type: 'state'
                    }, () =>
                        states.setState('influxdb.0.memRss', {val: 0, from: 'test.0'}, _done));
                });
        });
    });

    it('Test ' + adapterShortName + ' adapter: Check if adapter started', function (done) {
        this.timeout(60000);
        checkConnectionOfAdapter(function (res) {
            if (res) console.log(res);
            expect(res).not.to.be.equal('Cannot check connection');
            objects.setObject('system.adapter.test.0', {
                common: {

                },
                type: 'instance'
            },
            function () {
                states.subscribeMessage('system.adapter.test.0');
                sendTo('influxdb.0', 'enableHistory', {
                    id: 'system.adapter.influxdb.0.memHeapTotal',
                    options: {
                        changesOnly:  true,
                        debounce:     0,
                        retention:    31536000,
                        storageType: 'String'
                    }
                }, function (result) {
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
                    }, function (result) {
                        expect(result.error).to.be.undefined;
                        expect(result.success).to.be.true;
                        sendTo('influxdb.0', 'enableHistory', {
                            id: 'system.adapter.influxdb.0.memHeapUsed',
                            options: {
                                changesOnly:  false,
                                debounce:     0,
                                retention:    31536000,
                            }
                        }, function (result) {
                            expect(result.error).to.be.undefined;
                            expect(result.success).to.be.true;
                            objects.setObject('influxdb.0.testValue2', {
                                common: {
                                    type: 'number',
                                    role: 'state'
                                },
                                type: 'state'
                            },
                            function () {
                                sendTo('influxdb.0', 'enableHistory', {
                                    id: 'influxdb.0.testValue2',
                                    options: {
                                        changesOnly:  true,
                                        debounce:     0,
                                        retention:    31536000,
                                        maxLength:    3,
                                        changesMinDelta: 0.5,
                                        aliasId: 'influxdb.0.testValue2-alias'
                                    }
                                }, function (result) {
                                    expect(result.error).to.be.undefined;
                                    expect(result.success).to.be.true;
                                    // wait till adapter receives the new settings
                                    setTimeout(function () {
                                        done();
                                    }, 2000);
                                });
                            });
                        });
                    });
                });
            });
        });
    });
    it('Test ' + adapterShortName + ': Write string value for memHeapUsed into DB to force a type conflict', function (done) {
        this.timeout(5000);
        now = new Date().getTime();

        states.setState('system.adapter.influxdb.0.memHeapUsed', {val: 'Blubb', ts: now - 20000, from: 'test.0'}, function (err) {
            if (err) {
                console.log(err);
            }
            done();
        });
    });
    it('Test ' + adapterShortName + ': Check Enabled Points after Enable', function (done) {
        this.timeout(5000);

        sendTo('influxdb.0', 'getEnabledDPs', {}, function (result) {
            console.log(JSON.stringify(result));
            expect(Object.keys(result).length).to.be.equal(5);
            expect(result['influxdb.0.memRss'].enabled).to.be.true;
            done();
        });
    });
    it('Test ' + adapterShortName + ': Write values into DB', function (done) {
        this.timeout(45000);
        now = new Date().getTime();

        states.setState('influxdb.0.memRss', {val: 2, ts: now - 20000, from: 'test.0'}, function (err) {
            if (err) {
                console.log(err);
            }
            setTimeout(function () {
                states.setState('influxdb.0.memRss', {val: true, ts: now - 10000, from: 'test.0'}, function (err) {
                    if (err) {
                        console.log(err);
                    }
                    setTimeout(function () {
                        states.setState('influxdb.0.memRss', {val: 2, ts: now - 5000, from: 'test.0'}, function (err) {
                            if (err) {
                                console.log(err);
                            }
                            setTimeout(function () {
                                states.setState('influxdb.0.memRss', {val: 2.2, ts: now - 4000, from: 'test.0'}, function (err) {
                                    if (err) {
                                        console.log(err);
                                    }
                                    setTimeout(function () {
                                        states.setState('influxdb.0.memRss', {val: 2.3, ts: now - 3500, from: 'test.0'}, function (err) {
                                            if (err) {
                                                console.log(err);
                                            }
                                            setTimeout(function () {
                                                states.setState('influxdb.0.memRss', {val: '2.5', ts: now - 3000, from: 'test.0'}, function (err) {
                                                    if (err) {
                                                        console.log(err);
                                                    }
                                                    setTimeout(function () {
                                                        states.setState('influxdb.0.memRss', {val: 3, ts: now - 1000, from: 'test.0'}, function (err) {
                                                            if (err) {
                                                                console.log(err);
                                                            }
                                                            setTimeout(function () {
                                                                states.setState('influxdb.0.memRss', {val: 'Test', ts: now - 500, from: 'test.0'}, function (err) {
                                                                    if (err) {
                                                                        console.log(err);
                                                                    }
                                                                    setTimeout(function () {
                                                                        states.setState('influxdb.0.testValue2', {val: 1, ts: now - 2000, from: 'test.0'}, function (err) {
                                                                            if (err) {
                                                                                console.log(err);
                                                                            }
                                                                            setTimeout(function () {
                                                                                states.setState('influxdb.0.testValue2', {val: 3, ts: now - 1000, from: 'test.0'}, function (err) {
                                                                                    if (err) {
                                                                                        console.log(err);
                                                                                    }
                                                                                    setTimeout(done, 20000);
                                                                                });
                                                                            }, 100);
                                                                        });
                                                                    }, 100);
                                                                });
                                                            }, 100);
                                                        });
                                                    }, 100);
                                                });
                                            }, 100);
                                        });
                                    }, 100);
                                });
                            }, 100);
                        });
                    }, 100);
                });
            }, 100);
        });
    });
    it('Test ' + adapterShortName + ': Read values from DB using query', function (done) {
        this.timeout(10000);

        sendTo('influxdb.0', 'query', 'SELECT * FROM "influxdb.0.memRss"', function (result) {
            console.log(JSON.stringify(result.result, null, 2));
            expect(result.result[0].length).to.be.at.least(5);
            var found = 0;
            for (var i = 0; i < result.result[0].length; i++) {
                if (result.result[0][i].value >= 1 && result.result[0][i].value <= 3) found ++;
            }
            expect(found).to.be.equal(7);

            done();
        });
    });
    it('Test ' + adapterShortName + ': Read values from DB using GetHistory', function (done) {
        this.timeout(10000);

        sendTo('influxdb.0', 'getHistory', {
            id: 'influxdb.0.memRss',
            options: {
                start:     now - 30000,
                count:     50,
                aggregate: 'none'
            }
        }, function (result) {
            console.log(JSON.stringify(result.result, null, 2));
            expect(result.result.length).to.be.at.least(5);
            var found = 0;
            var found22 = false;
            var found23 = false;
            for (var i = 0; i < result.result.length; i++) {
                if (result.result[i].val >= 1 && result.result[i].val <= 3) found ++;
                if (result.result[i].val === 2.2) found22 = true;
                if (result.result[i].val === 2.3) found23 = true;
            }
            expect(found).to.be.equal(7);
            expect(found22).to.be.false;
            expect(found23).to.be.true;

            sendTo('influxdb.0', 'getHistory', {
                id: 'influxdb.0.memRss',
                options: {
                    start:     now - 15000,
                    count:     2,
                    aggregate: 'none'
                }
            }, function (result) {
                console.log(JSON.stringify(result.result, null, 2));
                expect(result.result.length).to.be.equal(3);
                done();
            });
        });
    });
    it('Test ' + adapterShortName + ': Check Datapoint Types', function (done) {
        this.timeout(65000);

        setTimeout(function() {
            sendTo('influxdb.0', 'query', 'SHOW FIELD KEYS FROM "influxdb.0.memRss"', function (result) {
                console.log('result: ' + JSON.stringify(result.result, null, 2));
                var found = false;
                for (var i = 0; i < result.result[0].length; i++) {
                    if (result.result[0][i].fieldKey === 'value') {
                        found = true;
                        expect(result.result[0][i].fieldType).to.be.equal('float');
                        break;
                    }
                }
                expect(found).to.be.true;

                sendTo('influxdb.0', 'query', 'SHOW FIELD KEYS FROM "system.adapter.influxdb.0.memHeapTotal"', function (result2) {
                    console.log('result2: ' + JSON.stringify(result2.result, null, 2));
                    var found = false;
                    for (var i = 0; i < result2.result[0].length; i++) {
                        if (result2.result[0][i].fieldKey === 'value') {
                            found = true;
                            expect(result2.result[0][i].fieldType).to.be.equal('string');
                            break;
                        }
                    }
                    expect(found).to.be.true;

                    sendTo('influxdb.0', 'query', 'SHOW FIELD KEYS FROM "system.adapter.influxdb.0.uptime"', function (result3) {
                        console.log('result3: ' + JSON.stringify(result3.result, null, 2));
                        var found = false;
                        for (var i = 0; i < result3.result[0].length; i++) {
                            if (result3.result[0][i].fieldKey === 'value') {
                                found = true;
                                expect(result3.result[0][i].fieldType).to.be.equal('boolean');
                                break;
                            }
                        }
                        expect(found).to.be.true;

                        setTimeout(function () {
                            done();
                        }, 3000);
                    });
                });
            });
        }, 60000);
    });

    it('Test ' + adapterShortName + ': Read values from DB using GetHistory for aliased testValue2', function (done) {
        this.timeout(25000);

        sendTo('influxdb.0', 'getHistory', {
            id: 'influxdb.0.testValue2',
            options: {
                start:     now - 5000,
                end:       now,
                count:     50,
                aggregate: 'none'
            }
        }, function (result) {
            console.log(JSON.stringify(result.result, null, 2));
            expect(result.result.length).to.be.equal(2);

            sendTo('influxdb.0', 'getHistory', {
                id: 'influxdb.0.testValue2-alias',
                options: {
                    start:     now - 5000,
                    end:       now,
                    count:     50,
                    aggregate: 'none'
                }
            }, function (result2) {
                console.log(JSON.stringify(result2.result, null, 2));
                expect(result2.result.length).to.be.equal(2);
                for (var i = 0; i < result2.result.length; i++) {
                    expect(result2.result[i].val).to.be.equal(result.result[i].val);
                }

                done();
            });
        });
    });

    it('Test ' + adapterShortName + ': Remove Alias-ID', function (done) {
        this.timeout(5000);

        sendTo('influxdb.0', 'enableHistory', {
            id: 'influxdb.0.testValue2',
            options: {
                aliasId: ''
            }
        }, function (result) {
            expect(result.error).to.be.undefined;
            expect(result.success).to.be.true;
            // wait till adapter receives the new settings
            setTimeout(function () {
                done();
            }, 2000);
        });
    });
    it('Test ' + adapterShortName + ': Add Alias-ID again', function (done) {
        this.timeout(5000);

        sendTo('influxdb.0', 'enableHistory', {
            id: 'influxdb.0.testValue2',
            options: {
                aliasId: 'this.is.a.test-value'
            }
        }, function (result) {
            expect(result.error).to.be.undefined;
            expect(result.success).to.be.true;
            // wait till adapter receives the new settings
            setTimeout(function () {
                done();
            }, 2000);
        });
    });
    it('Test ' + adapterShortName + ': Change Alias-ID', function (done) {
        this.timeout(5000);

        sendTo('influxdb.0', 'enableHistory', {
            id: 'influxdb.0.testValue2',
            options: {
                aliasId: 'this.is.another.test-value'
            }
        }, function (result) {
            expect(result.error).to.be.undefined;
            expect(result.success).to.be.true;
            // wait till adapter receives the new settings
            setTimeout(function () {
                done();
            }, 2000);
        });
    });
    it('Test ' + adapterShortName + ': Disable Datapoint again', function (done) {
        this.timeout(5000);

        sendTo('influxdb.0', 'disableHistory', {
            id: 'influxdb.0.memRss',
        }, function (result) {
            expect(result.error).to.be.undefined;
            expect(result.success).to.be.true;
            done();
        });
    });
    it('Test ' + adapterShortName + ': Check Enabled Points after Disable', function (done) {
        this.timeout(5000);

        sendTo('influxdb.0', 'getEnabledDPs', {}, function (result) {
            console.log(JSON.stringify(result));
            expect(Object.keys(result).length).to.be.equal(4);
            done();
        });
    });
    it('Test ' + adapterShortName + ': Check that storageType is set now for memHeapUsed', function (done) {
        this.timeout(5000);

        objects.getObject('system.adapter.influxdb.0.memHeapUsed', function(err, obj) {
            expect(obj.common.custom['influxdb.0'].storageType).to.be.equal('String');
            expect(err).to.be.null;
            done();
        });
    });

    after('Test ' + adapterShortName + ' adapter: Stop js-controller', function (done) {
        this.timeout(10000);

        setup.stopController(function (normalTerminated) {
            console.log('Adapter normal terminated: ' + normalTerminated);
            setTimeout(done, 2000);
        });
    });
});
