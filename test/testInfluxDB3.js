const assert = require('node:assert');
const { normalizeTypeConflict } = require('../build/lib/errors');
const DatabaseInfluxDB3x = require('../build/lib/DatabaseInfluxDB3x').default;

describe('Test InfluxDB 3 helpers', function () {
    it('reports a type conflict of InfluxDB 3 in the words of 1.x', function () {
        // as answered by InfluxDB 3 Core 3.12 on /write
        const text =
            'A 400 Bad Request error occurred: {"error":"line protocol parsing error","data":{"error_message":"invalid column type for column \'value\', expected iox::column_type::field::float, got iox::column_type::field::string","line_number":1}}';
        const normalized = normalizeTypeConflict(text);

        assert.ok(normalized.includes('field type conflict'), normalized);
        // the handling of type conflicts in main.ts looks for exactly this
        assert.ok(normalized.includes('is type string, already exists as type float'), normalized);
        assert.ok(normalized.startsWith(text), 'the original error must stay visible');
    });

    it('maps boolean to the wording of 1.x', function () {
        const normalized = normalizeTypeConflict(
            "invalid column type for column 'value', expected iox::column_type::field::float, got iox::column_type::field::boolean",
        );
        assert.ok(normalized.includes('is type boolean, already exists as type float'), normalized);
    });

    it('keeps other errors unchanged', function () {
        const text = 'A 401 Unauthorized error occurred: {"error": "the request was not authenticated"}';
        assert.strictEqual(normalizeTypeConflict(text), text);
    });

    it('recognizes deleted tables that are still listed', function () {
        assert.ok(DatabaseInfluxDB3x.isDeletedTable('javascript.0.temperature-20261021T131555'));
        assert.ok(!DatabaseInfluxDB3x.isDeletedTable('javascript.0.temperature'));
        assert.ok(!DatabaseInfluxDB3x.isDeletedTable('hm-rpc.0.ABC0012345.1.STATE'));
    });
});
