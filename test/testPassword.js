const assert = require('node:assert');

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
    let originalCore;
    let originalDb1x;
    let createAdapter;
    /** Settings handed to the InfluxDB 1.x client */
    let received;
    /** The answer of the fake client to getDatabaseNames() */
    let databaseNames;

    before(function () {
        require(coreId);
        originalCore = require.cache[coreId].exports;
        // The adapter runs without js-controller here: only what connect() and testConnection() use
        class Adapter {
            constructor() {
                this.log = log;
                this.config = {};
            }
            sendTo(from, command, message) {
                this.answer = message;
            }
        }
        require.cache[coreId].exports = { ...originalCore, Adapter };

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
        require.cache[coreId].exports = originalCore;
        db1xModule.default = originalDb1x;
        delete require.cache[mainId];
    });

    beforeEach(function () {
        received = null;
        databaseNames = () => Promise.resolve([]);
    });

    for (const password of ['Ab1%xy', 'Ab1%20x']) {
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
