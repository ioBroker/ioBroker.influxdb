const assert = require('node:assert');
const Module = require('node:module');
const os = require('node:os');

// getHistory with a fake client: adapter-core is replaced, so no js-controller and no database is needed.
// - `id: '*'` (all states, as in the sql adapter) and a missing ID read the measurement "undefined" and answered an
//   empty result: InfluxDB keeps one measurement per state, so they are refused with an error now.
// - An aggregate without `start` computed the step as (end - undefined) / count = NaN, and the query failed with
//   "Invalid time value": like sql and history, the newest `count` values are read now.

const ID = 'javascript.0.test';

class FakeAdapter {
    constructor(options) {
        this.options = options;
        this.config = {};
        this.namespace = 'influxdb.0';
        this.log = { silly() {}, debug() {}, info() {}, warn() {}, error() {} };
    }
    on() {}
    setState() {}
    sendTo(_from, _command, message) {
        this.answers.push(message);
    }
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

describe('getHistory: ID and start', function () {
    let createAdapter;

    before(function () {
        createAdapter = loadAdapterFactory();
    });

    /** An adapter connected to a client that records every query and finds no values */
    function connected(dbversion) {
        const adapter = createAdapter({});
        adapter.answers = [];
        adapter.queries = [];
        adapter.config = { dbversion, organization: 'org', dbname: 'iobroker', round: 0 };
        adapter._connected = true;
        adapter._client = {
            getHostsAvailable: () => 1,
            query: async query => {
                adapter.queries.push(query);
                return [];
            },
            queries: async queries => {
                adapter.queries.push(...queries);
                return queries.map(() => []);
            },
        };
        return adapter;
    }

    /** getHistory answers after a timer (the buffer is stored first): wait for exactly one answer */
    async function getHistory(adapter, message) {
        await adapter.processMessage({
            command: 'getHistory',
            message,
            from: 'system.adapter.javascript.0',
            callback: {},
        });
        for (let i = 0; i < 100 && !adapter.answers.length; i++) {
            await new Promise(resolve => setTimeout(resolve, 10));
        }
        assert.strictEqual(adapter.answers.length, 1, `exactly one answer: ${JSON.stringify(adapter.answers)}`);
        return adapter.answers[0];
    }

    for (const dbversion of ['1.x', '2.x', '3.x']) {
        describe(`InfluxDB ${dbversion}`, function () {
            for (const [name, id] of [
                ['"*"', '*'],
                ['no ID', undefined],
                ['an empty ID', ''],
            ]) {
                it(`refuses ${name} with an error and queries nothing`, async function () {
                    const adapter = connected(dbversion);
                    const answer = await getHistory(adapter, { id, options: { sessionId: 7 } });
                    assert.ok(answer.error && typeof answer.error === 'string', JSON.stringify(answer));
                    assert.deepStrictEqual(answer.result, []);
                    assert.strictEqual(answer.sessionId, 7);
                    assert.deepStrictEqual(adapter.queries, []);
                });
            }

            for (const aggregate of ['average', 'max', 'total', 'percentile', 'integral']) {
                it(`answers ${aggregate} without start and reads the newest count values`, async function () {
                    const adapter = connected(dbversion);
                    const answer = await getHistory(adapter, { id: ID, options: { aggregate, count: 10 } });
                    assert.strictEqual(answer.error, undefined, JSON.stringify(answer));
                    assert.deepStrictEqual(answer.result, []);
                    assert.ok(adapter.queries.length, 'queried');
                    for (const query of adapter.queries) {
                        assert.ok(!/NaN|Invalid/.test(query), query);
                    }
                    const main = adapter.queries.find(q => q.includes('ORDER BY') || q.includes('sort('));
                    if (dbversion === '2.x') {
                        assert.ok(main.includes('sort(columns:["_time"], desc: true)'), main);
                        assert.ok(main.includes('limit(n: 10)'), main);
                        assert.ok(!main.includes('window('), main);
                    } else {
                        assert.ok(main.startsWith(`SELECT * from "${ID}"`), main);
                        assert.ok(main.includes('ORDER BY time DESC'), main);
                        assert.ok(!main.includes('GROUP BY'), main);
                        if (aggregate !== 'integral') {
                            assert.ok(main.includes('LIMIT 10'), main);
                        }
                    }
                });
            }

            it('still lets InfluxDB aggregate with a start', async function () {
                const adapter = connected(dbversion);
                const end = Date.UTC(2026, 10, 19, 12);
                const answer = await getHistory(adapter, {
                    id: ID,
                    options: { aggregate: 'average', start: end - 3_600_000, end, count: 60 },
                });
                assert.strictEqual(answer.error, undefined, JSON.stringify(answer));
                const main = adapter.queries.find(q => q.includes('ORDER BY') || q.includes('sort('));
                if (dbversion === '2.x') {
                    // the bool check finds no number in the fake client, so the values are read raw in the range
                    assert.ok(main.includes(`start: ${new Date(end - 3_600_000 - 60_000).toISOString()}`), main);
                } else {
                    assert.ok(main.includes(`SELECT mean(value) as val from "${ID}"`), main);
                    assert.ok(main.includes('GROUP BY time(60000ms)'), main);
                }
            });

            it('refuses a start or end that is no date, and takes a time stamp as text', async function () {
                let adapter = connected(dbversion);
                let answer = await getHistory(adapter, { id: ID, options: { aggregate: 'average', start: 'garbage' } });
                assert.ok(/Start date "garbage" is not a valid date/.test(answer.error), JSON.stringify(answer));
                assert.deepStrictEqual(adapter.queries, []);

                adapter = connected(dbversion);
                answer = await getHistory(adapter, { id: ID, options: { aggregate: 'average', end: 'garbage' } });
                assert.ok(/End date "garbage" is not a valid date/.test(answer.error), JSON.stringify(answer));
                assert.deepStrictEqual(adapter.queries, []);

                adapter = connected(dbversion);
                const end = Date.UTC(2026, 10, 19, 12);
                answer = await getHistory(adapter, {
                    id: ID,
                    options: { aggregate: 'onchange', start: String(end - 3_600_000), end: String(end) },
                });
                assert.strictEqual(answer.error, undefined, JSON.stringify(answer));
                const main = adapter.queries.find(q => q.includes('ORDER BY') || q.includes('sort('));
                assert.ok(main.includes(new Date(end - 3_600_000).toISOString()), main);
            });

            it('does not enable logging for an ID that is not enabled (#603)', async function () {
                const adapter = connected(dbversion);
                const pushed = [];
                adapter.pushHelper = async (id, state) => pushed.push({ id, state });
                const answer = await getHistory(adapter, {
                    id: ID,
                    options: { aggregate: 'onchange', enableDebugLogs: true },
                });
                assert.strictEqual(answer.error, undefined, JSON.stringify(answer));
                assert.ok(!Object.prototype.hasOwnProperty.call(adapter._influxDPs, ID), 'no entry in _influxDPs');

                await adapter.pushHistory(ID, { val: 1, ts: Date.now(), lc: Date.now(), ack: true, from: 'test' });
                await new Promise(resolve => setTimeout(resolve, 50));
                assert.deepStrictEqual(pushed, []);
            });
        });
    }
});
