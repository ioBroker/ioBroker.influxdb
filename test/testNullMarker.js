const assert = require('node:assert');
const { NULL_MARKER_FIELD } = require('../build/lib/Database');
const DatabaseInfluxDB1x = require('../build/lib/DatabaseInfluxDB1x').default;
const DatabaseInfluxDB2x = require('../build/lib/DatabaseInfluxDB2x').default;

const log = { debug: () => {}, info: () => {}, warn: () => {}, error: () => {}, silly: () => {} };
const options = { log, host: 'localhost', port: 8086, protocol: 'http', database: 'iobroker', requestTimeout: 1000 };

function client2x(useTags) {
    return new DatabaseInfluxDB2x(options, {
        path: '',
        token: 'token',
        organization: 'org',
        validateSSL: true,
        useTags,
    });
}

const gap = { value: null, time: 1700000000000, from: 'system.adapter.influxdb.0', q: 0, ack: true };

describe('Test null values written as gap', function () {
    it('InfluxDB 1.x: writes the marker instead of the value field', function () {
        // toPoint is private in TypeScript only
        const point = DatabaseInfluxDB1x.toPoint('test.0.value', gap);

        assert.strictEqual(point.fields.value, undefined);
        assert.strictEqual(point.fields[NULL_MARKER_FIELD], 1);
        assert.strictEqual(point.fields.q, 0);
        assert.strictEqual(point.fields.ack, true);
    });

    it('InfluxDB 1.x: a real value has no marker', function () {
        const point = DatabaseInfluxDB1x.toPoint('test.0.value', { ...gap, value: 5 });

        assert.strictEqual(point.fields.value, 5);
        assert.strictEqual(point.fields[NULL_MARKER_FIELD], undefined);
    });

    it('InfluxDB 2.x with fields: writes the marker instead of the value field', function () {
        const line = client2x(false).stateValueToPoint('test.0.value', gap).toLineProtocol();

        assert.ok(line, 'the point must be valid');
        assert.ok(line.includes(`${NULL_MARKER_FIELD}=1`), line);
        assert.ok(!/[ ,]value=/.test(line), line);
        assert.ok(line.includes('ack=T'), line);
    });

    it('InfluxDB 2.x with tags: the marker is the only field', function () {
        const line = client2x(true).stateValueToPoint('test.0.value', gap).toLineProtocol();

        // without any field the client would drop the point (toLineProtocol() returns undefined)
        assert.ok(line, 'the point must be valid');
        assert.ok(line.includes(` ${NULL_MARKER_FIELD}=1 `), line);
        assert.ok(line.includes('ack=true'), line);
    });

    it('InfluxDB 2.x: a real value has no marker', function () {
        const line = client2x(false)
            .stateValueToPoint('test.0.value', { ...gap, value: 5 })
            .toLineProtocol();

        assert.ok(line.includes('value=5'), line);
        assert.ok(!line.includes(NULL_MARKER_FIELD), line);
    });
});
