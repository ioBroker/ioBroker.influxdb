const assert = require('node:assert');
const Module = require('node:module');
const os = require('node:os');

// #594: InfluxDB 2.x crashed with "panic: runtime error: invalid memory address or nil pointer dereference"
// (HTTP 500) while the adapter counted the values of all datapoints. The statistics tab showed nothing but
// the error, and the cleanup failed as well, although deleting itself worked. The list of datapoints comes
// from the index and their status from ioBroker - both are known without reading a single value.
//
// The message handlers run with a fake client: adapter-core is replaced, so no js-controller and no
// database is needed (same approach as testDelete.js).

class FakeAdapter {
    constructor(options) {
        this.options = options;
        this.config = {};
        this.namespace = 'influxdb.0';
        this.objects = {};
        this.warnings = [];
        this.log = {
            silly() {},
            debug() {},
            info() {},
            warn: text => this.warnings.push(text),
            error() {},
        };
    }
    on() {}
    setState() {}
    sendTo(_from, _command, message) {
        this.answers.push(message);
    }
    async getForeignObjectAsync(id) {
        return this.objects[id] || null;
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

const panic = () =>
    Object.assign(
        new Error('panic: runtime error: invalid memory address or nil pointer dereference'),
        { name: 'HttpError', statusCode: 500, code: 'internal error' },
    );

describe('Statistics and cleanup when the database fails while counting (#594)', function () {
    let createAdapter;

    before(function () {
        createAdapter = loadAdapterFactory();
    });

    /**
     * An adapter with three measurements: one logged, one whose object is gone, one with logging switched off
     *
     * @param getStatistics what the client does when the values are counted
     */
    function connected(getStatistics) {
        const adapter = createAdapter({});
        adapter.answers = [];
        adapter.dropped = [];
        adapter.config = { dbversion: '2.x', dbname: 'iobroker' };
        adapter._connected = true;
        adapter.objects = {
            'javascript.0.logged': { common: { type: 'number' } },
            'javascript.0.notLogged': { common: { type: 'number' } },
        };
        adapter._influxDPs = { 'javascript.0.logged': { realId: 'javascript.0.logged' } };
        adapter._client = {
            getHostsAvailable: () => 1,
            listMeasurements: async () => ['javascript.0.deleted', 'javascript.0.logged', 'javascript.0.notLogged'],
            getStatistics,
            dropMeasurement: async id => {
                adapter.dropped.push(id);
            },
        };
        return adapter;
    }

    async function send(adapter, command, message) {
        adapter.answers = [];
        await adapter.processMessage({ command, message, from: 'system.adapter.admin.0', callback: {} });
        assert.strictEqual(adapter.answers.length, 1, `exactly one answer: ${JSON.stringify(adapter.answers)}`);
        return adapter.answers[0];
    }

    it('lists the datapoints without counts and says why', async function () {
        const adapter = connected(async () => {
            throw panic();
        });

        const answer = await send(adapter, 'getDpStatistics', {});

        assert.strictEqual(answer.success, true, JSON.stringify(answer));
        assert.ok(answer.statisticsError.includes('nil pointer dereference'), answer.statisticsError);
        assert.deepStrictEqual(
            answer.result.map(row => [row.id, row.status, row.count, row.firstTs, row.lastTs]),
            [
                ['javascript.0.deleted', 'objectMissing', null, null, null],
                ['javascript.0.logged', 'active', null, null, null],
                ['javascript.0.notLogged', 'loggingDisabled', null, null, null],
            ],
        );
        assert.strictEqual(answer.summary.datapoints, 3);
        assert.strictEqual(answer.summary.values, null);
        assert.strictEqual(adapter.warnings.length, 1);
    });

    it('previews and cleans up anyway', async function () {
        const adapter = connected(async () => {
            throw panic();
        });

        const preview = await send(adapter, 'cleanupOrphaned', {});
        assert.strictEqual(preview.success, true, JSON.stringify(preview));
        assert.strictEqual(preview.dryRun, true);
        assert.strictEqual(preview.datapoints, 1);
        assert.strictEqual(preview.values, null);
        assert.deepStrictEqual(
            preview.items.map(item => item.id),
            ['javascript.0.deleted'],
        );
        assert.ok(preview.statisticsError);
        assert.deepStrictEqual(adapter.dropped, [], 'a preview deletes nothing');

        const done = await send(adapter, 'cleanupOrphaned', {
            scope: { objectMissing: true, ids: ['javascript.0.deleted'] },
            confirm: true,
        });
        assert.strictEqual(done.success, true, JSON.stringify(done));
        assert.deepStrictEqual(adapter.dropped, ['javascript.0.deleted']);
        assert.deepStrictEqual(done.deleted, { datapoints: 1, values: null });
        assert.deepStrictEqual(done.failed, []);
    });

    it('still answers with an error if the database is not reachable at all', async function () {
        // nothing to show then: a partial list would hide that the database is down
        const adapter = connected(async () => {
            throw Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:8086'), { code: 'ECONNREFUSED' });
        });

        const answer = await send(adapter, 'getDpStatistics', {});
        assert.ok(answer.error && answer.error.includes('ECONNREFUSED'), JSON.stringify(answer));

        const preview = await send(adapter, 'cleanupOrphaned', {});
        assert.ok(preview.error, JSON.stringify(preview));
        assert.deepStrictEqual(adapter.dropped, []);
    });

    it('keeps the counts when they could be read', async function () {
        const adapter = connected(async () => ({
            'javascript.0.deleted': { count: 7, firstTs: 1000, lastTs: 2000, cardinality: 1 },
            'javascript.0.logged': { count: 3, firstTs: 1500, lastTs: 3000, cardinality: 1 },
        }));

        const answer = await send(adapter, 'getDpStatistics', {});
        assert.strictEqual(answer.statisticsError, undefined);
        assert.deepStrictEqual(
            answer.result.map(row => [row.id, row.count]),
            [
                ['javascript.0.deleted', 7],
                ['javascript.0.logged', 3],
                ['javascript.0.notLogged', 0],
            ],
        );
        assert.strictEqual(answer.summary.values, 10);

        const preview = await send(adapter, 'cleanupOrphaned', {});
        assert.strictEqual(preview.values, 7);
        assert.strictEqual(preview.statisticsError, undefined);
    });
});
