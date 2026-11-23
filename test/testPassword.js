const assert = require('node:assert');
const Module = require('node:module');

// Regression guard for ioBroker/ioBroker.influxdb#601: the stored password is plain text and must reach
// InfluxDB 1.x unchanged, only the "Test connection" button of Admin sends it URI-encoded.

const log = { debug: () => {}, info: () => {}, warn: () => {}, error: () => {}, silly: () => {} };

const coreId = require.resolve('@iobroker/adapter-core');
const mainId = require.resolve('../build/main');
const db1xModule = require('../build/lib/DatabaseInfluxDB1x');

/** "Test connection" as Admin sends it (see _testConnection in admin/jsonConfig.json) */
function testMessage(password) {
    return {
        from: 'system.adapter.admin.0',
        command: 'test',
        message: { config: { dbversion: '1.x', host: 'localhost', port: '8086', password } },
    };
}

describe('Test password handling of InfluxDB 1.x (#601)', function () {
    /** The cache entry of adapter-core before this suite, if another test loaded it */
    let originalCoreEntry;
    let originalDb1x;
    let createAdapter;
    /** Settings handed to the InfluxDB 1.x client */
    let received;
    /** The answer of the fake client to getDatabaseNames() */
    let databaseNames;

    before(function () {
        // the first load of build/main (and its InfluxDB clients) takes longer than the default 2 s on a cold cache
        this.timeout(30000);
        // The real adapter-core must not be loaded: without a js-controller installation (as in CI, where it only
        // exists in tmp/) it ends the whole process with exit code 10. A fake module with only what main.ts
        // imports and connect()/testConnection() use takes its place in the require cache.
        originalCoreEntry = require.cache[coreId];
        class Adapter {
            constructor() {
                this.log = log;
                this.config = {};
            }
            sendTo(from, command, message) {
                this.answer = message;
            }
        }
        const fakeCore = new Module(coreId);
        fakeCore.filename = coreId;
        fakeCore.loaded = true;
        fakeCore.exports = { Adapter, getAbsoluteDefaultDataDir: () => '' };
        require.cache[coreId] = fakeCore;

        originalDb1x = db1xModule.default;
        db1xModule.default = class {
            constructor(options, settings) {
                received = settings;
            }
            getDatabaseNames() {
                return databaseNames();
            }
        };

        delete require.cache[mainId];
        createAdapter = require(mainId);
    });

    after(function () {
        if (originalCoreEntry) {
            require.cache[coreId] = originalCoreEntry;
        } else {
            delete require.cache[coreId];
        }
        db1xModule.default = originalDb1x;
        delete require.cache[mainId];
    });

    beforeEach(function () {
        received = null;
        databaseNames = () => Promise.resolve([]);
    });

    for (const password of ['Ab1%xy', 'Ab1%20x', 'Pässwörtß1']) {
        it(`passes the stored password "${password}" unchanged on start`, function () {
            // the client is created synchronously, the rest of connect() is not of interest here
            databaseNames = () => new Promise(() => {});
            const adapter = createAdapter();
            adapter.config = { dbversion: '1.x', host: 'localhost', port: 8086, dbname: 'iobroker', password };
            void adapter.connect();
            assert.ok(received, 'no InfluxDB 1.x client was created');
            assert.strictEqual(received.password, password);
        });

        it(`decodes the password "${password}" sent by "Test connection" once`, async function () {
            const adapter = createAdapter();
            await adapter.testConnection(testMessage(encodeURIComponent(password)));
            assert.deepStrictEqual(adapter.answer, { error: null });
            assert.strictEqual(received.password, password);
        });
    }

    it('keeps a not encoded password of "Test connection" as it is', async function () {
        const adapter = createAdapter();
        await adapter.testConnection(testMessage('Ab1%xy'));
        assert.strictEqual(received.password, 'Ab1%xy');
    });
});
