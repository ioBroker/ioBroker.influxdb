const assert = require('node:assert');
const { normalizeCustomTags, getInvalidTagNameReason } = require('../build/lib/customTags');

describe('Test custom tags', function () {
    it('converts the rows of the admin table into tags', function () {
        const { tags, invalid } = normalizeCustomTags([
            { name: 'room', value: 'kitchen' },
            { name: 'device', value: 'heat pump' },
        ]);
        assert.deepStrictEqual(tags, { room: 'kitchen', device: 'heat pump' });
        assert.deepStrictEqual(invalid, []);
    });

    it('means "no tags" for everything that is not an array', function () {
        for (const rows of [undefined, null, '', 'room=kitchen', {}, { room: 'kitchen' }, 5]) {
            assert.deepStrictEqual(normalizeCustomTags(rows), { tags: {}, invalid: [] }, JSON.stringify(rows));
        }
    });

    it('silently skips empty rows, as the admin adds them with "+"', function () {
        const { tags, invalid } = normalizeCustomTags([
            { name: '', value: '' },
            {},
            { name: '  ', value: ' ' },
            null,
            'room',
            { name: 'room', value: 'kitchen' },
        ]);
        assert.deepStrictEqual(tags, { room: 'kitchen' });
        assert.deepStrictEqual(invalid, []);
    });

    it('trims names and values and accepts numbers and booleans', function () {
        const { tags } = normalizeCustomTags([
            { name: ' room ', value: ' kitchen ' },
            { name: 'floor', value: 1 },
            { name: 'outdoor', value: false },
        ]);
        assert.deepStrictEqual(tags, { room: 'kitchen', floor: '1', outdoor: 'false' });
    });

    it('reports rows with a missing name or value', function () {
        const { tags, invalid } = normalizeCustomTags([
            { name: 'room', value: '' },
            { name: '', value: 'kitchen' },
            { name: 'device', value: { nested: true } },
        ]);
        assert.deepStrictEqual(tags, {});
        assert.strictEqual(invalid.length, 3, JSON.stringify(invalid));
    });

    it('reports reserved names', function () {
        const rows = ['value', 'q', 'ack', 'from', 'time', 'Value', 'ACK', '_measurement', '_field', '_x'].map(
            name => ({ name, value: 'x' }),
        );
        const { tags, invalid } = normalizeCustomTags(rows);
        assert.deepStrictEqual(tags, {});
        assert.strictEqual(invalid.length, rows.length, JSON.stringify(invalid));
    });

    it('lets the last row win for duplicate names', function () {
        const { tags } = normalizeCustomTags([
            { name: 'room', value: 'kitchen' },
            { name: 'room', value: 'bedroom' },
        ]);
        assert.deepStrictEqual(tags, { room: 'bedroom' });
    });

    it('explains why a name is invalid', function () {
        assert.strictEqual(getInvalidTagNameReason('room'), null);
        assert.strictEqual(getInvalidTagNameReason('room_1'), null);
        assert.ok(getInvalidTagNameReason(''));
        assert.ok(getInvalidTagNameReason('from'));
        assert.ok(getInvalidTagNameReason('_start'));
    });
});
