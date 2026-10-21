const assert = require('node:assert');
const { formatError, isConnectionError, UnstorableValueError } = require('../build/lib/errors');

describe('Test formatError', function () {
    it('unwraps an AggregateError', function () {
        // this is what Node's happy-eyeballs connect produces when InfluxDB is switched off:
        // an AggregateError with an empty message, so String(err) is just "AggregateError"
        const first = Object.assign(new Error('connect ECONNREFUSED ::1:8086'), { code: 'ECONNREFUSED' });
        const second = Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:8086'), { code: 'ECONNREFUSED' });
        const err = new AggregateError([first, second]);

        assert.strictEqual(err.toString(), 'AggregateError', 'precondition: toString() says nothing');

        const text = formatError(err);
        assert.ok(text.includes('connect ECONNREFUSED ::1:8086'), text);
        assert.ok(text.includes('connect ECONNREFUSED 127.0.0.1:8086'), text);
        // the class name says nothing about the cause and must not clutter the log line
        assert.ok(!text.includes('AggregateError'), text);
        assert.ok(!text.includes('\n'), 'must stay on one line');
    });

    it('reports identical sub-errors only once', function () {
        const err = new AggregateError([
            new Error('connect ECONNREFUSED 127.0.0.1:8086'),
            new Error('connect ECONNREFUSED 127.0.0.1:8086'),
        ]);
        const text = formatError(err);
        assert.strictEqual(text.indexOf('ECONNREFUSED'), text.lastIndexOf('ECONNREFUSED'), text);
    });

    it('limits the number of nested errors', function () {
        const err = new AggregateError(new Array(9).fill(0).map((_, i) => new Error(`address ${i} failed`)));
        const text = formatError(err);
        assert.ok(text.includes('address 4 failed'), text);
        assert.ok(!text.includes('address 5 failed'), text);
        assert.ok(text.includes('and 4 more'), text);
    });

    it('keeps a normal error readable and adds the code', function () {
        const err = Object.assign(new Error('Access denied'), { code: 'unauthorized', statusCode: 401 });
        assert.strictEqual(formatError(err), 'Access denied (code: unauthorized) (statusCode: 401)');
    });

    it('does not repeat a code that is already in the message', function () {
        const err = Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:8086'), { code: 'ECONNREFUSED' });
        assert.strictEqual(formatError(err), 'connect ECONNREFUSED 127.0.0.1:8086');
    });

    it('keeps a name that tells something', function () {
        // the influx 1.x driver names its errors, and "ServiceNotAvailableError: ..." is worth showing
        const err = Object.assign(new Error('No host available'), { name: 'ServiceNotAvailableError' });
        assert.strictEqual(formatError(err), 'ServiceNotAvailableError: No host available');
    });

    it('unwraps the cause', function () {
        const err = new Error('Cannot write points', { cause: new Error('getaddrinfo ENOTFOUND influx-host') });
        const text = formatError(err);
        assert.ok(text.includes('Cannot write points'), text);
        assert.ok(text.includes('getaddrinfo ENOTFOUND influx-host'), text);
    });

    it('survives strings, null and plain objects', function () {
        assert.strictEqual(formatError('No connection to DB'), 'No connection to DB');
        assert.strictEqual(formatError(null), 'Unknown error');
        assert.strictEqual(formatError(undefined), 'Unknown error');
        assert.strictEqual(formatError({ severity: 'FATAL' }), '{"severity":"FATAL"}');
        const circular = {};
        circular.self = circular;
        assert.ok(formatError(circular).length > 0);
    });
});

describe('Test isConnectionError', function () {
    it('detects the AggregateError of a refused connect', function () {
        const err = new AggregateError([
            Object.assign(new Error('connect ECONNREFUSED ::1:8086'), { code: 'ECONNREFUSED' }),
            Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:8086'), { code: 'ECONNREFUSED' }),
        ]);
        // Node sets the code on the aggregate too, but not in every version - both ways must work
        assert.ok(isConnectionError(err), 'sub-error code must be enough');
        assert.ok(isConnectionError(Object.assign(new AggregateError([]), { code: 'ECONNREFUSED' })));
    });

    it('detects timeouts and unresolvable hosts', function () {
        assert.ok(isConnectionError(Object.assign(new Error('read ETIMEDOUT'), { code: 'ETIMEDOUT' })));
        assert.ok(isConnectionError(new Error('Request timed out')));
        assert.ok(isConnectionError('timeout'));
        assert.ok(isConnectionError(Object.assign(new Error('getaddrinfo EAI_AGAIN influx'), { code: 'EAI_AGAIN' })));
        assert.ok(
            isConnectionError(Object.assign(new Error('No host available'), { name: 'ServiceNotAvailableError' })),
        );
        assert.ok(isConnectionError(Object.assign(new Error('Bad Gateway'), { statusCode: 502 })));
        assert.ok(isConnectionError(Object.assign(new Error('Service Unavailable'), { statusCode: 503 })));
        assert.ok(isConnectionError(Object.assign(new Error('timeout'), { name: 'HttpError', statusCode: 504 })));
        assert.ok(isConnectionError({ code: 'ETIMEDOUT' }));
    });

    it('does not classify a rejected point as connection error', function () {
        // these must keep the "find the conflicting point" escalation of the write path
        assert.ok(!isConnectionError(new Error('partial write: field type conflict: input field "value" on ...')));
        assert.ok(!isConnectionError(Object.assign(new Error('unauthorized access'), { statusCode: 401 })));
        assert.ok(!isConnectionError(new Error('database not found')));
        assert.ok(
            !isConnectionError(
                Object.assign(new Error('unexpected error writing points to database: timeout'), {
                    name: 'HttpError',
                    statusCode: 500,
                }),
            ),
            'an answer of the server is no connection error, even if the message says "timeout"',
        );
        assert.ok(!isConnectionError(Object.assign(new Error('timeout'), { res: { statusCode: 400 } })));
        assert.ok(!isConnectionError(null));
        assert.ok(!isConnectionError(undefined));
        assert.ok(!isConnectionError(new UnstorableValueError('null', 'Skipping null value for x')));
    });
});
