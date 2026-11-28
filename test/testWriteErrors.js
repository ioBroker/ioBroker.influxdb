const assert = require('node:assert');
const Module = require('node:module');
const os = require('node:os');

// The write path of the adapter with a fake client: adapter-core is replaced, so no js-controller and
// no database is needed (same approach as testDelete.js).
//
// #574: InfluxDB answered with HTTP 500 "unexpected error writing points to database: timeout" for a
// minute. With "seriesBufferMax" = 0 each value was written again immediately and recursively, and was
// discarded after 10 attempts within milliseconds. With a buffer the batch was split down to single
// points, which sent even more requests to the overloaded server.

class FakeAdapter {
    constructor(options) {
        this.options = options;
        this.config = {};
        this.namespace = 'influxdb.0';
        this.logged = { warn: [], info: [], error: [] };
        this.log = {
            silly() {},
            debug() {},
            info: text => this.logged.info.push(text),
            warn: text => this.logged.warn.push(text),
            error: text => this.logged.error.push(text),
        };
    }
    on() {}
    setState() {}
    sendTo() {}
}

function loadAdapterFactory() {
    const stubs = {
        '@iobroker/adapter-core': { Adapter: FakeAdapter, getAbsoluteDefaultDataDir: () => os.tmpdir() },
        '@iobroker/plugin-docker': { DockerManagerOfOwnContainers: class {} },
    };
    const mainPath = require.resolve('../build/main.js');
    const originalLoad = Module._load;
    delete require.cache[mainPath];
    Module._load = function (request, ...rest) {
        return Object.prototype.hasOwnProperty.call(stubs, request)
            ? stubs[request]
            : originalLoad.call(this, request, ...rest);
    };
    try {
        return require(mainPath);
    } finally {
        Module._load = originalLoad;
        delete require.cache[mainPath];
    }
}

const ID = 'javascript.0.power';
const T = Date.UTC(2026, 9, 28, 19, 39, 0);

const timeoutError = () =>
    Object.assign(new Error('unexpected error writing points to database: timeout'), {
        name: 'HttpError',
        statusCode: 500,
    });
const badPointError = () =>
    Object.assign(new Error('unable to parse points: invalid field format'), { name: 'HttpError', statusCode: 400 });

describe('Write path: temporary server errors (#574)', function () {
    let createAdapter;
    const adapters = [];

    before(function () {
        createAdapter = loadAdapterFactory();
    });

    afterEach(function () {
        // the backoff timer must not keep mocha alive
        for (const adapter of adapters.splice(0)) {
            clearTimeout(adapter._writeBackoffTimer);
            adapter._writeBackoffTimer = null;
        }
    });

    /**
     * An adapter connected to a client whose write methods fail as long as `client.failWith` is set
     *
     * @param seriesBufferMax the instance setting "seriesBufferMax"
     */
    function connected(seriesBufferMax) {
        const adapter = createAdapter({});
        adapters.push(adapter);
        adapter.config = { dbversion: '2.x', seriesBufferMax, seriesBufferFlushInterval: 600, reconnectInterval: 10000 };
        adapter._connected = true;
        const client = {
            failWith: null,
            calls: { writePoint: 0, writePoints: 0, writeSeries: 0 },
            written: [],
            getHostsAvailable: () => 1,
        };
        for (const method of ['writePoint', 'writePoints']) {
            client[method] = async (id, value) => {
                client.calls[method]++;
                if (client.failWith) {
                    throw client.failWith();
                }
                client.written.push(...(Array.isArray(value) ? value : [value]));
            };
        }
        client.writeSeries = async series => {
            client.calls.writeSeries++;
            if (client.failWith) {
                throw client.failWith();
            }
            for (const id of Object.keys(series)) {
                client.written.push(...series[id]);
            }
        };
        adapter._client = client;
        adapter.reconnect = () => {
            adapter.reconnected = true;
        };
        return { adapter, client };
    }

    const push = (adapter, i) => adapter.pushValueIntoDB(ID, { val: 230 + i, ts: T + i * 1000, ack: true, q: 0 }, false);
    const buffered = adapter => (adapter._seriesBuffer[ID] || []).length;

    describe('without buffer (seriesBufferMax = 0)', function () {
        it('tries a value once and keeps it in the buffer instead of retrying it 10 times and discarding it', async function () {
            const { adapter, client } = connected(0);
            client.failWith = timeoutError;

            await push(adapter, 0);

            assert.strictEqual(client.calls.writePoint, 1, 'exactly one write request');
            assert.strictEqual(buffered(adapter), 1, 'the value waits in the buffer');
            assert.strictEqual(adapter._seriesBufferCounter, 1);
            assert.ok(!adapter._errorPoints[ID], 'a slow server is not the fault of the point');
            assert.ok(!adapter.logged.info.some(text => text.startsWith('Discard point')));
            assert.ok(!adapter.reconnected, 'no reconnect, see #551');
            assert.strictEqual(adapter._connected, true);
        });

        it('only buffers while the server was too slow a moment ago, and logs that once', async function () {
            const { adapter, client } = connected(0);
            client.failWith = timeoutError;

            for (let i = 0; i < 20; i++) {
                await push(adapter, i);
            }

            assert.strictEqual(client.calls.writePoint, 1, 'no further requests during the backoff');
            assert.strictEqual(buffered(adapter), 20);
            assert.strictEqual(adapter.logged.warn.length, 1, JSON.stringify(adapter.logged.warn));
            assert.ok(adapter._writeBackoffTimer, 'the buffer is written once the backoff is over');
        });

        it('writes everything once the server has recovered', async function () {
            const { adapter, client } = connected(0);
            client.failWith = timeoutError;
            for (let i = 0; i < 5; i++) {
                await push(adapter, i);
            }

            client.failWith = null;
            adapter._writeBackoffUntil = 0;
            await adapter.storeBufferedSeries();
            await push(adapter, 5);

            assert.strictEqual(buffered(adapter), 0);
            assert.deepStrictEqual(
                client.written.map(point => point.value),
                [230, 231, 232, 233, 234, 235],
            );
        });

        it('does not retry a rejected point recursively either', async function () {
            const { adapter, client } = connected(0);
            client.failWith = badPointError;

            await push(adapter, 0);

            assert.strictEqual(client.calls.writePoint, 1, 'the point goes back into the buffer, it is not written again at once');
            assert.strictEqual(buffered(adapter), 1);
            assert.strictEqual(adapter._errorPoints[ID], 1);
            assert.ok(!adapter.isWriteBackoff(), 'a bad point does not hold back the other datapoints');
        });
    });

    describe('with buffer', function () {
        it('keeps the batch in the buffer instead of splitting it down to single points', async function () {
            const { adapter, client } = connected(5);
            client.failWith = timeoutError;

            // the 6th value exceeds seriesBufferMax and flushes the buffer
            for (let i = 0; i < 6; i++) {
                await push(adapter, i);
            }

            assert.strictEqual(client.calls.writeSeries, 1);
            assert.strictEqual(client.calls.writePoints, 0, 'not split per ID');
            assert.strictEqual(client.calls.writePoint, 0, 'not split into single points');
            assert.strictEqual(buffered(adapter), 6);
            assert.strictEqual(adapter._seriesBufferCounter, 6);
            assert.ok(!adapter.reconnected);
        });

        it('does not flush on every new value while the server is too slow', async function () {
            const { adapter, client } = connected(5);
            client.failWith = timeoutError;

            for (let i = 0; i < 30; i++) {
                await push(adapter, i);
            }

            assert.strictEqual(client.calls.writeSeries, 1);
            assert.strictEqual(buffered(adapter), 30);
        });

        it('still searches the bad point of a rejected batch', async function () {
            const { adapter, client } = connected(5);
            client.failWith = badPointError;

            for (let i = 0; i < 6; i++) {
                await push(adapter, i);
            }

            assert.strictEqual(client.calls.writeSeries, 1);
            assert.strictEqual(client.calls.writePoints, 1, 'escalated per ID');
            assert.strictEqual(client.calls.writePoint, 6, 'and to single points');
            assert.strictEqual(buffered(adapter), 6, 'each point waits for the next flush');
        });
    });
});
