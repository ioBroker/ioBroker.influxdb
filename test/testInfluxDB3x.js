const assert = require('node:assert');
const http = require('node:http');
const {
    toLineProtocol,
    splitStatements,
    parseInfluxTime,
    translateWriteError,
    isDeletedTable,
} = require('../build/lib/influx3Helpers');
const { isConnectionError, formatError } = require('../build/lib/errors');
const DatabaseInfluxDB3x = require('../build/lib/DatabaseInfluxDB3x').default;

const value = (overrides = {}) => ({
    value: 1.5,
    time: 1760000000123,
    from: 'system.adapter.x.0',
    q: 0,
    ack: true,
    ...overrides,
});

describe('InfluxDB 3.x: line protocol', function () {
    it('writes q, ack and from as fields by default', function () {
        assert.strictEqual(
            toLineProtocol('hm-rpc.0.dev.TEMP', value(), false),
            'hm-rpc.0.dev.TEMP value=1.5,q=0,ack=true,from="system.adapter.x.0" 1760000000123',
        );
    });

    it('writes q, ack and from as sorted tags with useTags', function () {
        assert.strictEqual(
            toLineProtocol('a', value(), true),
            'a,ack=true,from=system.adapter.x.0,q=0 value=1.5 1760000000123',
        );
    });

    it('adds custom tags, escaped, and drops empty ones', function () {
        const line = toLineProtocol('a', value({ tags: { room: 'living room', empty: '', 'a,b': 'c=d' } }), false);
        assert.ok(line.startsWith('a,a\\,b=c\\=d,room=living\\ room value='), line);
        assert.ok(!line.includes('empty='), line);
    });

    it('escapes measurement names and string fields', function () {
        const line = toLineProtocol('my id,with space', value({ value: 'say "hi"\\now' }), false);
        assert.ok(line.startsWith('my\\ id\\,with\\ space value="say \\"hi\\"\\\\now",'), line);
    });

    it('writes booleans and rounds the timestamp to whole milliseconds', function () {
        const line = toLineProtocol('a', value({ value: false, time: 1760000000123.4 }), false);
        assert.ok(line.startsWith('a value=false,'), line);
        assert.ok(line.endsWith(' 1760000000123'), line);
    });

    it('refuses a non-finite number instead of sending a broken line', function () {
        assert.throws(() => toLineProtocol('a', value({ value: NaN }), false), /non-finite/);
    });

    it('keeps the timestamp in field mode (regression: the point must not get the server time)', function () {
        assert.ok(toLineProtocol('a', value(), false).endsWith(' 1760000000123'));
    });
});

describe('InfluxDB 3.x: statements', function () {
    it('splits statements and ignores empty ones', function () {
        assert.deepStrictEqual(splitStatements('SELECT 1; SELECT 2;; '), ['SELECT 1', 'SELECT 2']);
    });

    it('does not split inside quoted identifiers or strings', function () {
        assert.deepStrictEqual(splitStatements(`SELECT value FROM "a;b" WHERE x = 'c;d'; SELECT 2`), [
            `SELECT value FROM "a;b" WHERE x = 'c;d'`,
            'SELECT 2',
        ]);
    });

    it('honours escaped quotes', function () {
        assert.deepStrictEqual(splitStatements('SELECT value FROM "a\\";b"; SELECT 2'), [
            'SELECT value FROM "a\\";b"',
            'SELECT 2',
        ]);
    });
});

describe('InfluxDB 3.x: results and errors', function () {
    it('reads timestamps without zone as UTC', function () {
        assert.strictEqual(parseInfluxTime('2026-10-21T07:10:58.316').toISOString(), '2026-10-21T07:10:58.316Z');
        assert.strictEqual(parseInfluxTime('2026-10-21T07:10:58Z').toISOString(), '2026-10-21T07:10:58.000Z');
        assert.strictEqual(parseInfluxTime('2026-10-21T09:10:58+02:00').toISOString(), '2026-10-21T07:10:58.000Z');
    });

    it('words a type conflict like InfluxDB 1.x, which the write path recognizes', function () {
        const text = translateWriteError(
            "invalid column type for column 'value', expected iox::column_type::field::float, got iox::column_type::field::string",
            'probe.num value="str',
        );
        assert.ok(text.includes('field type conflict'), text);
        assert.ok(text.includes('is type string, already exists as type float'), text);
        assert.ok(text.includes('"probe.num"'), text);

        assert.ok(
            translateWriteError(
                "invalid column type for column 'value', expected iox::column_type::field::float, got iox::column_type::field::boolean",
            ).includes('is type bool, already exists as type float'),
        );
    });

    it('recognizes the leftover of a deleted table', function () {
        assert.ok(isDeletedTable('hm-rpc.0.dev.TEMP-20261021T072447'));
        assert.ok(!isDeletedTable('hm-rpc.0.dev.TEMP'));
        assert.ok(!isDeletedTable('sensor-2026'));
    });

    it('leaves other write errors alone', function () {
        assert.strictEqual(translateWriteError('something else'), 'something else');
    });
});

describe('InfluxDB 3.x: driver against a mock server', function () {
    let server;
    let port;
    /** @type {{ method: string, url: string, headers: object, body: string }[]} */
    let requests;
    let respond;

    const createDb = (overrides = {}) =>
        new DatabaseInfluxDB3x(
            {
                log: { debug() {}, info() {}, warn() {}, error() {} },
                host: '127.0.0.1',
                port,
                protocol: 'http',
                database: 'iobroker',
                requestTimeout: 2000,
            },
            { path: '', token: 'secret', validateSSL: true, useTags: false, ...overrides },
        );

    before(function (done) {
        server = http.createServer((req, res) => {
            const chunks = [];
            req.on('data', chunk => chunks.push(chunk));
            req.on('end', () => {
                const entry = {
                    method: req.method,
                    url: req.url,
                    headers: req.headers,
                    body: Buffer.concat(chunks).toString(),
                };
                requests.push(entry);
                const answer = respond(entry);
                res.writeHead(answer.status, { 'Content-Type': 'application/json' });
                res.end(answer.body ?? '');
            });
        });
        server.listen(0, '127.0.0.1', () => {
            port = server.address().port;
            done();
        });
    });

    after(function (done) {
        server.closeAllConnections?.();
        server.close(done);
    });

    beforeEach(function () {
        requests = [];
        respond = () => ({ status: 204 });
    });

    it('writes line protocol with token and millisecond precision', async function () {
        await createDb().writeSeries({ a: [value(), value({ time: 1760000000124 })], b: [value({ value: 'x' })] });

        assert.strictEqual(requests.length, 1);
        const [request] = requests;
        assert.strictEqual(request.method, 'POST');
        assert.ok(request.url.startsWith('/api/v3/write_lp?db=iobroker&precision=millisecond'), request.url);
        assert.strictEqual(request.headers.authorization, 'Bearer secret');
        assert.strictEqual(request.body.split('\n').length, 3);
    });

    it('waits for the WAL for a single value if syncWrites is set', async function () {
        const db = createDb({ syncWrites: true });
        await db.writePoint('a', value());
        assert.ok(!requests[0].url.includes('no_sync'), requests[0].url);
    });

    it('waits for the WAL only when writing a batch, not for a single value', async function () {
        const db = createDb();
        await db.writePoints('a', [value()]);
        await db.writeSeries({ a: [value()] });
        await db.writePoint('a', value());
        assert.ok(!requests[0].url.includes('no_sync'), requests[0].url);
        assert.ok(!requests[1].url.includes('no_sync'), requests[1].url);
        assert.ok(requests[2].url.includes('no_sync=true'), requests[2].url);
    });

    it('does not send an Authorization header without a token', async function () {
        await createDb({ token: '' }).writePoint('a', value());
        assert.strictEqual(requests[0].headers.authorization, undefined);
    });

    it('turns a rejected line into an error the write path understands', async function () {
        respond = () => ({
            status: 400,
            body: JSON.stringify({
                error: 'partial write of line protocol occurred',
                data: [
                    {
                        error_message:
                            "invalid column type for column 'value', expected iox::column_type::field::float, got iox::column_type::field::string",
                        line_number: 1,
                        original_line: 'probe.num value="str',
                    },
                ],
            }),
        });
        await assert.rejects(createDb().writePoint('probe.num', value({ value: 'str' })), error => {
            const text = formatError(error);
            assert.ok(text.includes('partial write'), text);
            assert.ok(text.includes('field type conflict'), text);
            assert.ok(text.includes('is type string, already exists as type float'), text);
            assert.strictEqual(error.statusCode, 400);
            assert.ok(!isConnectionError(error));
            return true;
        });
    });

    it('classifies an unreachable server as a connection error and marks the host unavailable', async function () {
        const db = createDb();
        const closed = http.createServer();
        await new Promise(resolve => closed.listen(0, '127.0.0.1', resolve));
        const closedPort = closed.address().port;
        await new Promise(resolve => closed.close(resolve));

        const unreachable = new DatabaseInfluxDB3x(
            {
                log: { debug() {}, info() {}, warn() {}, error() {} },
                host: '127.0.0.1',
                port: closedPort,
                protocol: 'http',
                database: 'iobroker',
                requestTimeout: 2000,
            },
            { path: '', token: 't', validateSSL: true },
        );
        await assert.rejects(unreachable.writePoint('a', value()), error => isConnectionError(error));
        assert.strictEqual(unreachable.getHostsAvailable(), 0);
        assert.strictEqual(db.getHostsAvailable(), 1);
    });

    it('classifies a timeout as a connection error', async function () {
        respond = () => ({ status: 200 });
        const slow = http.createServer(() => {
            // never answers
        });
        await new Promise(resolve => slow.listen(0, '127.0.0.1', resolve));
        const db = new DatabaseInfluxDB3x(
            {
                log: { debug() {}, info() {}, warn() {}, error() {} },
                host: '127.0.0.1',
                port: slow.address().port,
                protocol: 'http',
                database: 'iobroker',
                requestTimeout: 100,
            },
            { path: '', token: 't', validateSSL: true },
        );
        try {
            await assert.rejects(db.ping(), error => isConnectionError(error));
        } finally {
            slow.closeAllConnections?.();
            await new Promise(resolve => slow.close(resolve));
        }
    });

    it('runs several statements one after the other and returns one array per statement', async function () {
        respond = request => {
            const { q } = JSON.parse(request.body);
            if (q.startsWith('SELECT value from "a;b" WHERE')) {
                return {
                    status: 200,
                    body: JSON.stringify([{ 'iox::measurement': 'a;b', time: '2026-10-21T07:00:00', value: 1.5 }]),
                };
            }
            return { status: 200, body: '' };
        };
        const rows = await createDb().query(
            `SELECT value from "a;b" WHERE time <= 'x' LIMIT 1; SELECT value from "a;b" LIMIT 1`,
        );

        assert.strictEqual(requests.length, 2);
        assert.strictEqual(requests[0].url, '/api/v3/query_influxql');
        assert.strictEqual(JSON.parse(requests[0].body).db, 'iobroker');
        assert.strictEqual(rows.length, 2);
        assert.strictEqual(rows[0][0].value, 1.5);
        assert.ok(rows[0][0].time instanceof Date);
        assert.strictEqual(rows[0][0].time.toISOString(), '2026-10-21T07:00:00.000Z');
        assert.strictEqual(rows[0][0]['iox::measurement'], undefined);
        assert.deepStrictEqual(rows[1], []);
    });

    it('returns the plain rows for a single statement', async function () {
        respond = () => ({ status: 200, body: JSON.stringify([{ 'iox::measurement': 'a', name: 'a' }]) });
        assert.deepStrictEqual(await createDb().query('SHOW MEASUREMENTS'), [{ name: 'a' }]);
    });

    it('detects where the metadata is stored', async function () {
        const answers = {
            'SHOW TAG KEYS': [],
            'SHOW FIELD KEYS': [{ fieldKey: 'value' }, { fieldKey: 'ack' }],
        };
        respond = request => ({ status: 200, body: JSON.stringify(answers[JSON.parse(request.body).q]) });
        assert.strictEqual(await createDb().getMetaDataStorageType(), 'fields');

        answers['SHOW TAG KEYS'] = [{ tagKey: 'from' }];
        assert.strictEqual(await createDb().getMetaDataStorageType(), 'tags');

        answers['SHOW FIELD KEYS'] = [{ fieldKey: 'value' }];
        answers['SHOW TAG KEYS'] = [{ tagKey: 'room' }];
        assert.strictEqual(await createDb().getMetaDataStorageType(), 'none');
    });

    it('treats a database that does not exist yet as empty', async function () {
        respond = () => ({ status: 404, body: JSON.stringify({ error: 'query error: database not found: iobroker' }) });
        assert.strictEqual(await createDb().getMetaDataStorageType(), 'none');
    });

    it('lists databases and falls back to the configured one if the token may not list them', async function () {
        respond = () => ({
            status: 200,
            body: JSON.stringify([{ 'iox::database': '_internal' }, { 'iox::database': 'iobroker' }]),
        });
        assert.deepStrictEqual(await createDb().getDatabaseNames(), ['_internal', 'iobroker']);

        respond = () => ({ status: 403, body: 'forbidden' });
        assert.deepStrictEqual(await createDb().getDatabaseNames(), ['iobroker']);

        respond = () => ({ status: 401, body: JSON.stringify({ error: 'the request was not authenticated' }) });
        await assert.rejects(createDb().getDatabaseNames(), /not authenticated/);
    });

    it('creates a database and accepts that it exists already', async function () {
        const db = createDb();
        await db.createDatabase('iobroker');
        assert.strictEqual(requests[0].method, 'POST');
        assert.strictEqual(requests[0].url, '/api/v3/configure/database');
        assert.deepStrictEqual(JSON.parse(requests[0].body), { db: 'iobroker' });

        respond = () => ({ status: 409, body: 'attempted to create a resource that already exists' });
        await db.createDatabase('iobroker');
    });

    it('drops a measurement as a table and ignores a missing one', async function () {
        const db = createDb();
        await db.dropMeasurement('a b;c');
        assert.strictEqual(requests[0].method, 'DELETE');
        assert.strictEqual(requests[0].url, '/api/v3/configure/table?db=iobroker&table=a%20b%3Bc&hard_delete_at=now');

        respond = () => ({ status: 404, body: 'table not found' });
        await db.dropMeasurement('missing');
    });

    it('cannot delete a time range', async function () {
        await assert.rejects(createDb().deleteData(0, 1, '', 'iobroker', ''), /cannot delete/);
    });

    it('reports a healthy server, and a server that only wants a better token, as online', async function () {
        respond = () => ({ status: 200, body: 'OK' });
        assert.deepStrictEqual(await createDb().ping(), [{ online: true }]);
        respond = () => ({ status: 401, body: '{}' });
        assert.deepStrictEqual(await createDb().ping(), [{ online: true }]);
        respond = () => ({ status: 503, body: '' });
        await assert.rejects(createDb().ping(), error => isConnectionError(error));
    });

    it('lists the measurements without the leftovers of deleted tables', async function () {
        respond = () => ({
            status: 200,
            body: JSON.stringify([{ table_name: 'a' }, { table_name: 'a-20261021T072447' }, { table_name: 'b' }]),
        });
        assert.deepStrictEqual(await createDb().listMeasurements(), ['a', 'b']);
    });

    it('builds the statistics with SQL, one statement per batch of measurements', async function () {
        respond = request => {
            const { q } = JSON.parse(request.body);
            if (q.includes('information_schema.tables')) {
                return {
                    status: 200,
                    body: JSON.stringify([{ table_name: 'a' }, { table_name: 'it\'s "b"' }, { table_name: 'empty' }]),
                };
            }
            return {
                status: 200,
                body: JSON.stringify([
                    { m: 'a', c: 3, f: '2026-10-21T07:00:00', l: '2026-10-21T08:00:00.500' },
                    { m: 'it\'s "b"', c: 1, f: '2026-10-21T07:00:00', l: '2026-10-21T07:00:00' },
                    { m: 'empty', c: 0 },
                ]),
            };
        };
        const statistics = await createDb().getStatistics(0, Date.UTC(2262, 3, 11));

        assert.strictEqual(requests.length, 2);
        assert.ok(requests[0].url === '/api/v3/query_sql' && requests[1].url === '/api/v3/query_sql');
        const sql = JSON.parse(requests[1].body).q;
        assert.ok(sql.includes('FROM "it\'s ""b"""'), sql);
        assert.ok(sql.includes("SELECT 'it''s \"b\"'"), sql);
        assert.strictEqual(sql.split(' UNION ALL ').length, 3, sql);
        assert.deepStrictEqual(statistics.a, {
            count: 3,
            firstTs: Date.UTC(2026, 9, 21, 7),
            lastTs: Date.UTC(2026, 9, 21, 8, 0, 0, 500),
            cardinality: null,
        });
        assert.deepStrictEqual(statistics.empty, { count: 0, firstTs: null, lastTs: null, cardinality: null });
    });

    it('has no statistics for a database that does not exist yet', async function () {
        respond = () => ({ status: 404, body: JSON.stringify({ error: 'database not found' }) });
        assert.deepStrictEqual(await createDb().getStatistics(0, 1), {});
    });
});
