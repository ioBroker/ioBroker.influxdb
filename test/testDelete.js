const assert = require('node:assert');
const Module = require('node:module');
const os = require('node:os');
const { parseDeleteRequests, deleteRangeProblem, toTimestamp } = require('../build/lib/deleteRequests');

// `delete` / `deleteRange` with a missing or invalid `ts` or range fell through to "delete every value of
// the datapoint": a single `{id, state: {start, end}}` or `{id, state: {}}` reached `_delete` as
// `{ts: undefined}`, which deleted everything since 2000 (1.x), since 1970 (2.x) or the whole table (3.x).

const ID = 'javascript.0.test';
const T = Date.UTC(2026, 10, 18, 12, 0, 0, 123);
const START = Date.UTC(2026, 10, 1);
const END = Date.UTC(2026, 10, 2);
const iso = ts => new Date(ts).toISOString();

/** Message shapes that must be refused with an error and must not delete anything */
const BAD_MESSAGES = {
    'single state without ts': { id: ID, state: {} },
    'single state with ts null': { id: ID, state: { ts: null } },
    'single state with a ts that is no date': { id: ID, state: { ts: 'yesterday' } },
    'single state with ts NaN': { id: ID, state: { ts: NaN } },
    'single state with an unparsable start': { id: ID, state: { start: 'garbage', end: END } },
    'single state with only end': { id: ID, state: { end: END } },
    'single entry with only end': { id: ID, end: END },
    'single entry with an unparsable start': { id: ID, start: 'garbage' },
    'single entry with an unparsable end': { id: ID, start: START, end: 'garbage' },
    'single entry with neither ts nor range': { id: ID },
    'single entry without id': { state: { ts: T } },
    'list entry without ts': [{ id: ID, state: {} }],
    'list entry with an unparsable start': [{ id: ID, start: 'garbage', end: END }],
    'list with one valid and one invalid entry': [
        { id: ID, state: { ts: T } },
        { id: ID, state: { start: 'garbage' } },
    ],
    'state list with an entry without ts': { id: ID, state: [T, {}] },
    'ts list with a value that is no date': { id: ID, ts: [T, 'garbage'] },
    'ts list with null': { id: ID, ts: [null] },
    'no object at all': 'javascript.0.test',
};

describe('delete: parsing of the message', function () {
    it('takes ts from the state', function () {
        assert.deepStrictEqual(parseDeleteRequests({ id: ID, state: { ts: T } }, 0), {
            requests: [{ id: ID, range: { mode: 'one', ts: T } }],
            errors: [],
        });
    });

    it('accepts a ts of 0', function () {
        for (const message of [
            { id: ID, state: { ts: 0 } },
            { id: ID, ts: 0 },
            { id: ID, ts: [0] },
        ]) {
            const { requests, errors } = parseDeleteRequests(message, 1);
            assert.deepStrictEqual(errors, [], JSON.stringify(message));
            assert.deepStrictEqual(requests, [{ id: ID, range: { mode: 'one', ts: 0 } }], JSON.stringify(message));
        }
    });

    it('takes a range from the state, from the entry and from a list (the bug: {id, state: {start, end}})', function () {
        const expected = { id: ID, range: { mode: 'range', start: START, end: END } };
        for (const message of [
            { id: ID, state: { start: START, end: END } },
            { id: ID, start: START, end: END },
            [{ id: ID, start: START, end: END }],
            [{ id: ID, state: { start: START, end: END } }],
            { id: ID, state: [{ start: START, end: END }] },
        ]) {
            const { requests, errors } = parseDeleteRequests(message, 1);
            assert.deepStrictEqual(errors, [], JSON.stringify(message));
            assert.deepStrictEqual(requests, [expected], JSON.stringify(message));
        }
    });

    it('lets a range without end end now, and accepts a start of 0', function () {
        assert.deepStrictEqual(parseDeleteRequests({ id: ID, start: START }, END).requests, [
            { id: ID, range: { mode: 'range', start: START, end: END } },
        ]);
        assert.deepStrictEqual(parseDeleteRequests({ id: ID, state: { start: 0, end: 0 } }, END).requests, [
            { id: ID, range: { mode: 'range', start: 0, end: 0 } },
        ]);
    });

    it('reads date strings and numeric strings', function () {
        const { requests, errors } = parseDeleteRequests(
            [{ id: ID, start: '2026-11-01T00:00:00.000Z', end: String(END) }],
            0,
        );
        assert.deepStrictEqual(errors, []);
        assert.deepStrictEqual(requests, [{ id: ID, range: { mode: 'range', start: START, end: END } }]);
    });

    it('takes several entries of one datapoint', function () {
        const { requests, errors } = parseDeleteRequests(
            { id: ID, state: [T, { ts: T + 1 }, { start: START, end: END }] },
            0,
        );
        assert.deepStrictEqual(errors, []);
        assert.deepStrictEqual(
            requests.map(r => r.range),
            [
                { mode: 'one', ts: T },
                { mode: 'one', ts: T + 1 },
                { mode: 'range', start: START, end: END },
            ],
        );
        assert.deepStrictEqual(
            parseDeleteRequests({ id: ID, ts: [T, T + 1] }, 0).requests.map(r => r.range.ts),
            [T, T + 1],
        );
    });

    it('resolves aliases', function () {
        const { requests } = parseDeleteRequests([{ id: 'alias.0.x', ts: T }], 0, id => (id === 'alias.0.x' ? ID : id));
        assert.strictEqual(requests[0].id, ID);
    });

    for (const [name, message] of Object.entries(BAD_MESSAGES)) {
        it(`refuses: ${name}`, function () {
            const { errors } = parseDeleteRequests(message, Date.now());
            assert.ok(errors.length, JSON.stringify(message));
        });
    }

    it('names what is missing', function () {
        assert.match(parseDeleteRequests({ id: ID, state: {} }, 0).errors[0], /Neither "ts" nor "start"/);
        assert.match(parseDeleteRequests({ id: ID, end: END }, 0).errors[0], /"start" is missing/);
        assert.match(parseDeleteRequests({ id: ID, state: { start: 'x' } }, 0).errors[0], /Invalid "start"/);
        assert.match(parseDeleteRequests({ id: ID, start: 1, end: 'x' }, 0).errors[0], /Invalid "end"/);
        assert.match(parseDeleteRequests({ id: ID, state: { ts: NaN } }, 0).errors[0], /Invalid "ts" NaN/);
        assert.match(parseDeleteRequests({ state: { ts: T } }, 0).errors[0], /No "id"/);
    });
});

describe('delete: time stamps and the check of _delete', function () {
    it('converts time stamps', function () {
        assert.strictEqual(toTimestamp(0), 0);
        assert.strictEqual(toTimestamp('0'), 0);
        assert.strictEqual(toTimestamp(T), T);
        assert.strictEqual(toTimestamp(String(T)), T);
        assert.strictEqual(toTimestamp(iso(T)), T);
        assert.strictEqual(toTimestamp(new Date(T)), T);
        for (const value of [undefined, null, '', ' ', 'abc', NaN, Infinity, 1e20, true, {}, [], new Date(NaN)]) {
            assert.strictEqual(toTimestamp(value), undefined, String(value));
        }
    });

    it('refuses everything but a valid ts, a valid range or an explicit "all"', function () {
        for (const range of [
            undefined,
            null,
            {},
            { ts: undefined }, // what the single {id, state: {start, end}} produced before
            { ts: T }, // the old shape without mode
            { mode: 'one' },
            { mode: 'one', ts: NaN },
            { mode: 'one', ts: '2026-11-18' },
            { mode: 'range', start: NaN, end: END },
            { mode: 'range', start: START },
            { mode: 'range', start: START, end: Infinity },
            { mode: 'everything' },
        ]) {
            assert.ok(deleteRangeProblem(range), JSON.stringify(range));
        }
        assert.strictEqual(deleteRangeProblem({ mode: 'one', ts: 0 }), null);
        assert.strictEqual(deleteRangeProblem({ mode: 'range', start: 0, end: END }), null);
        assert.strictEqual(deleteRangeProblem({ mode: 'all' }), null);
    });
});

// The message handlers of the adapter with a fake client: adapter-core is replaced, so no js-controller
// and no database is needed.

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

describe('delete: message handlers', function () {
    let createAdapter;

    before(function () {
        createAdapter = loadAdapterFactory();
    });

    /** An adapter connected to a client that records every call */
    function connected(dbversion) {
        const adapter = createAdapter({});
        adapter.answers = [];
        adapter.calls = [];
        adapter.config = { dbversion, organization: 'org', dbname: 'iobroker' };
        adapter._connected = true;
        adapter._client = {
            query: async query => {
                adapter.calls.push({ query });
                return [];
            },
            deleteData: async (start, stop, org, bucket, predicate) => {
                adapter.calls.push({ start, stop, org, bucket, predicate });
            },
            dropMeasurement: async id => {
                adapter.calls.push({ drop: id });
            },
        };
        return adapter;
    }

    async function send(adapter, command, message) {
        await adapter.processMessage({ command, message, from: 'system.adapter.javascript.0', callback: {} });
        assert.strictEqual(adapter.answers.length, 1, `exactly one answer: ${JSON.stringify(adapter.answers)}`);
        return adapter.answers[0];
    }

    for (const dbversion of ['1.x', '2.x', '3.x']) {
        describe(`InfluxDB ${dbversion}`, function () {
            for (const command of ['delete', 'deleteRange']) {
                for (const [name, message] of Object.entries(BAD_MESSAGES)) {
                    it(`${command} refuses and deletes nothing: ${name}`, async function () {
                        const adapter = connected(dbversion);
                        const answer = await send(adapter, command, message);
                        assert.strictEqual(answer.success, false, JSON.stringify(answer));
                        assert.ok(answer.error && typeof answer.error === 'string', JSON.stringify(answer));
                        assert.deepStrictEqual(adapter.calls, []);
                    });
                }
            }

            it('_delete refuses a missing or invalid ts or range itself', async function () {
                const adapter = connected(dbversion);
                for (const range of [
                    {},
                    { ts: undefined },
                    { mode: 'one', ts: NaN },
                    { mode: 'range', start: NaN, end: END },
                ]) {
                    await assert.rejects(adapter._delete(ID, range), /Nothing deleted/, JSON.stringify(range));
                }
                assert.deepStrictEqual(adapter.calls, []);
            });

            it('update without a valid ts answers an error and changes nothing', async function () {
                const adapter = connected(dbversion);
                const answer = await send(adapter, 'update', { id: ID, state: { val: 5 } });
                assert.ok(/"ts"/.test(answer.error), JSON.stringify(answer));
                assert.deepStrictEqual(adapter.calls, []);
            });

            it('deleteAll still deletes everything of the datapoint', async function () {
                const adapter = connected(dbversion);
                const before = Date.now();
                const answer = await send(adapter, 'deleteAll', { id: ID });
                assert.strictEqual(answer.success, true, JSON.stringify(answer));
                assert.strictEqual(adapter.calls.length, 1);
                const [call] = adapter.calls;
                if (dbversion === '1.x') {
                    assert.strictEqual(call.query, `DELETE FROM "${ID}" WHERE time >= '2000-01-01T00:00:00.000Z'`);
                } else if (dbversion === '2.x') {
                    assert.strictEqual(new Date(call.start).getTime(), 0);
                    assert.ok(new Date(call.stop).getTime() >= before);
                    assert.strictEqual(call.predicate, `_measurement="${ID}"`);
                } else {
                    assert.deepStrictEqual(call, { drop: ID });
                }
            });
        });
    }

    describe('InfluxDB 1.x: the DELETE statement', function () {
        it('deletes exactly one time stamp', async function () {
            const adapter = connected('1.x');
            const answer = await send(adapter, 'delete', [{ id: ID, state: { ts: T } }]);
            assert.strictEqual(answer.success, true, JSON.stringify(answer));
            assert.deepStrictEqual(adapter.calls, [{ query: `DELETE FROM "${ID}" WHERE time = '${iso(T)}'` }]);
        });

        it('deletes a ts of 0 and not everything', async function () {
            const adapter = connected('1.x');
            await send(adapter, 'delete', { id: ID, state: { ts: 0 } });
            assert.deepStrictEqual(adapter.calls, [
                { query: `DELETE FROM "${ID}" WHERE time = '1970-01-01T00:00:00.000Z'` },
            ]);
        });

        it('deletes a range given in the state of a single entry', async function () {
            const adapter = connected('1.x');
            const answer = await send(adapter, 'deleteRange', { id: ID, state: { start: START, end: END } });
            assert.strictEqual(answer.success, true, JSON.stringify(answer));
            assert.deepStrictEqual(adapter.calls, [
                { query: `DELETE FROM "${ID}" WHERE time >= '${iso(START)}' AND time <= '${iso(END)}'` },
            ]);
        });

        it('deletes a range until now if end is missing', async function () {
            const adapter = connected('1.x');
            const before = Date.now();
            await send(adapter, 'deleteRange', [{ id: ID, start: START }]);
            const match = adapter.calls[0].query.match(/^DELETE FROM ".+" WHERE time >= '(.+)' AND time <= '(.+)'$/);
            assert.ok(match, adapter.calls[0].query);
            assert.strictEqual(match[1], iso(START));
            assert.ok(new Date(match[2]).getTime() >= before);
        });

        it('deletes the entries of a list one by one, alias resolved', async function () {
            const adapter = connected('1.x');
            adapter._aliasMap['alias.0.x'] = ID;
            await send(adapter, 'delete', { id: 'alias.0.x', ts: [T, T + 1] });
            assert.deepStrictEqual(adapter.calls, [
                { query: `DELETE FROM "${ID}" WHERE time = '${iso(T)}'` },
                { query: `DELETE FROM "${ID}" WHERE time = '${iso(T + 1)}'` },
            ]);
        });
    });

    describe('InfluxDB 2.x: the time range of the delete API', function () {
        it('deletes exactly one time stamp', async function () {
            const adapter = connected('2.x');
            const answer = await send(adapter, 'delete', { id: ID, ts: T });
            assert.strictEqual(answer.success, true, JSON.stringify(answer));
            assert.deepStrictEqual(adapter.calls, [
                { start: T, stop: T, org: 'org', bucket: 'iobroker', predicate: `_measurement="${ID}"` },
            ]);
        });

        it('deletes a ts of 0 and not everything', async function () {
            const adapter = connected('2.x');
            await send(adapter, 'delete', [{ id: ID, ts: 0 }]);
            assert.strictEqual(adapter.calls.length, 1);
            assert.strictEqual(adapter.calls[0].start, 0);
            assert.strictEqual(adapter.calls[0].stop, 0);
        });

        it('deletes a range given in the state of a single entry', async function () {
            const adapter = connected('2.x');
            await send(adapter, 'deleteRange', { id: ID, state: { start: START, end: END } });
            assert.deepStrictEqual(adapter.calls, [
                { start: START, stop: END, org: 'org', bucket: 'iobroker', predicate: `_measurement="${ID}"` },
            ]);
        });

        it('deletes a range given as date strings', async function () {
            const adapter = connected('2.x');
            await send(adapter, 'deleteRange', [{ id: ID, start: iso(START), end: iso(END) }]);
            assert.strictEqual(adapter.calls[0].start, START);
            assert.strictEqual(adapter.calls[0].stop, END);
        });
    });

    describe('InfluxDB 3.x: only whole datapoints', function () {
        it('refuses a single value and a range without dropping the table', async function () {
            for (const [command, message] of [
                ['delete', { id: ID, state: { ts: T } }],
                ['deleteRange', { id: ID, state: { start: START, end: END } }],
            ]) {
                const adapter = connected('3.x');
                const answer = await send(adapter, command, message);
                assert.strictEqual(answer.success, false, JSON.stringify(answer));
                assert.match(answer.error, /cannot delete single values or a time range/);
                assert.deepStrictEqual(adapter.calls, []);
            }
        });
    });
});
