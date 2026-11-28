const assert = require('node:assert');
const DatabaseInfluxDB2x = require('../build/lib/DatabaseInfluxDB2x').default;

const log = { debug: () => {}, info: () => {}, warn: () => {}, error: () => {}, silly: () => {} };

/**
 * A 2.x driver whose org and bucket API are replaced by stubs, so no server is needed
 *
 * @param bucketNames all buckets of the organization, in the order the server lists them
 * @param serverBehaviour how the fake server pages: 'honest' honors limit and offset, 'noOffset'
 *                        ignores the offset, 'firstPageOnly' always answers with the first 20 buckets
 * @param database the configured bucket
 */
function createDriver(bucketNames, serverBehaviour, database) {
    const driver = new DatabaseInfluxDB2x(
        { log, host: '127.0.0.1', port: 8086, protocol: 'http', database, requestTimeout: 1000 },
        { path: '', token: 'token', organization: 'org', validateSSL: false },
    );
    const buckets = bucketNames.map((name, i) => ({ name, id: `id${i}` }));
    const requests = [];

    driver.orgsApi = { getOrgs: async () => ({ orgs: [{ id: 'orgId', name: 'org' }] }) };
    driver.bucketsApi = {
        getBuckets: async request => {
            requests.push(request);
            if (request.name) {
                // like InfluxDB 2.x: an unknown name is answered with 404, not with an empty list
                const found = buckets.filter(bucket => bucket.name === request.name);
                if (!found.length) {
                    throw Object.assign(new Error(`bucket "${request.name}" not found`), {
                        name: 'HttpError',
                        statusCode: 404,
                    });
                }
                return { buckets: found };
            }
            if (serverBehaviour === 'firstPageOnly') {
                return { buckets: buckets.slice(0, 20) };
            }
            const limit = request.limit ?? 20;
            const offset = serverBehaviour === 'noOffset' ? 0 : request.offset || 0;
            return { buckets: buckets.slice(offset, offset + limit) };
        },
    };
    return { driver, requests };
}

const names = count => Array.from({ length: count }, (_, i) => `bucket${String(i).padStart(3, '0')}`);

describe('Test bucket list of InfluxDB 2.x (#605)', function () {
    it('finds a bucket that is not among the first 20', async function () {
        const all = [...names(21), 'vbucket'];
        const { driver } = createDriver(all, 'honest', 'vbucket');
        const found = await driver.getDatabaseNames();
        assert.strictEqual(found.length, 22);
        assert.ok(found.includes('vbucket'));
        assert.strictEqual(driver.bucketIds.vbucket, 'id21');
    });

    it('reads all pages of a large organization', async function () {
        const all = [...names(250), 'zbucket'];
        const { driver, requests } = createDriver(all, 'honest', 'zbucket');
        const found = await driver.getDatabaseNames();
        assert.strictEqual(found.length, 251);
        assert.deepStrictEqual(
            requests.map(request => request.offset),
            [0, 100, 200],
        );
        assert.ok(requests.every(request => request.limit === 100));
    });

    it('stops when the server ignores the offset', async function () {
        const all = [...names(150), 'zbucket'];
        const { driver, requests } = createDriver(all, 'noOffset', 'zbucket');
        const found = await driver.getDatabaseNames();
        // the second page brought nothing new; the configured bucket is then asked for by name
        assert.ok(found.includes('zbucket'));
        assert.strictEqual(requests.filter(request => !request.name).length, 2);
        assert.strictEqual(requests.filter(request => request.name === 'zbucket').length, 1);
    });

    it('asks for the configured bucket by name if the list does not contain it', async function () {
        const all = [...names(30), 'vbucket'];
        const { driver, requests } = createDriver(all, 'firstPageOnly', 'vbucket');
        const found = await driver.getDatabaseNames();
        assert.ok(found.includes('vbucket'));
        assert.strictEqual(driver.bucketIds.vbucket, 'id30');
        assert.ok(requests.some(request => request.name === 'vbucket'));
    });

    it('does not report a bucket that really does not exist, so it can be created', async function () {
        // the server answers the lookup by name with 404 - that must not make the connect fail
        const { driver, requests } = createDriver(names(5), 'honest', 'missing');
        const found = await driver.getDatabaseNames();
        assert.strictEqual(found.length, 5);
        assert.ok(!found.includes('missing'));
        assert.ok(requests.some(request => request.name === 'missing'));
    });

    it('still fails on other errors of the lookup by name', async function () {
        const { driver } = createDriver(names(5), 'honest', 'missing');
        const original = driver.bucketsApi.getBuckets;
        driver.bucketsApi.getBuckets = async request => {
            if (request.name) {
                throw Object.assign(new Error('unauthorized access'), { name: 'HttpError', statusCode: 401 });
            }
            return original(request);
        };
        await assert.rejects(() => driver.getDatabaseNames(), /unauthorized/);
    });
});
