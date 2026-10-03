const assert = require('node:assert');
const {
    classifyDatapoint,
    summarize,
    selectForCleanup,
    cardinalityFromSeriesKeys,
} = require('../build/lib/statistics');

/**
 * Build a statistics row, so a test only has to name what it cares about
 *
 * @param id the datapoint ID
 * @param overrides the attributes this test is about
 */
function stat(id, overrides) {
    return {
        id,
        type: 'Number',
        count: 0,
        firstTs: null,
        lastTs: null,
        cardinality: 1,
        status: 'active',
        ...overrides,
    };
}

describe('Test statistics', function () {
    describe('classifyDatapoint', function () {
        it('separates a deleted state from one with logging switched off', function () {
            assert.strictEqual(classifyDatapoint(true, true), 'active');
            assert.strictEqual(classifyDatapoint(true, false), 'loggingDisabled');
            assert.strictEqual(classifyDatapoint(false, false), 'objectMissing');
        });

        it('reports a state that no longer exists as missing, even while it is still logged', function () {
            // the object can disappear while the adapter still holds its custom config
            assert.strictEqual(classifyDatapoint(false, true), 'objectMissing');
        });
    });

    describe('summarize', function () {
        it('counts datapoints and values per status', function () {
            const summary = summarize([
                stat('a', { count: 10 }),
                stat('b', { count: 5, status: 'loggingDisabled' }),
                stat('c', { count: 2, status: 'objectMissing' }),
                stat('d', { count: 3, status: 'objectMissing' }),
            ]);

            assert.strictEqual(summary.datapoints, 4);
            assert.strictEqual(summary.values, 20);
            assert.deepStrictEqual(summary.byStatus.active, { datapoints: 1, values: 10 });
            assert.deepStrictEqual(summary.byStatus.loggingDisabled, { datapoints: 1, values: 5 });
            assert.deepStrictEqual(summary.byStatus.objectMissing, { datapoints: 2, values: 5 });
        });

        it('adds up the cardinality', function () {
            assert.strictEqual(summarize([stat('a', { cardinality: 2 }), stat('b', { cardinality: 3 })]).cardinality, 5);
        });

        it('reports no total cardinality as soon as one datapoint has none', function () {
            // a partial sum presented as a total would understate the real footprint
            assert.strictEqual(
                summarize([stat('a', { cardinality: 2 }), stat('b', { cardinality: null })]).cardinality,
                null,
            );
        });

        it('survives an empty database', function () {
            const summary = summarize([]);
            assert.strictEqual(summary.datapoints, 0);
            assert.strictEqual(summary.values, 0);
            assert.strictEqual(summary.cardinality, 0);
            assert.deepStrictEqual(summary.byStatus.active, { datapoints: 0, values: 0 });
        });
    });

    describe('selectForCleanup', function () {
        const stats = [
            stat('active', { status: 'active' }),
            stat('disabled', { status: 'loggingDisabled' }),
            stat('missing', { status: 'objectMissing' }),
        ];

        it('removes only deleted states without a scope', function () {
            assert.deepStrictEqual(
                selectForCleanup(stats).map(s => s.id),
                ['missing'],
            );
            assert.deepStrictEqual(
                selectForCleanup(stats, {}).map(s => s.id),
                ['missing'],
            );
        });

        it('takes datapoints with logging switched off only when asked to', function () {
            assert.deepStrictEqual(
                selectForCleanup(stats, { loggingDisabled: true }).map(s => s.id),
                ['disabled', 'missing'],
            );
        });

        it('can be limited to datapoints with logging switched off', function () {
            assert.deepStrictEqual(
                selectForCleanup(stats, { objectMissing: false, loggingDisabled: true }).map(s => s.id),
                ['disabled'],
            );
        });

        it('never selects a datapoint that is being logged', function () {
            const all = selectForCleanup(stats, { objectMissing: true, loggingDisabled: true });
            assert.ok(!all.find(s => s.status === 'active'));
        });
    });

    describe('cardinalityFromSeriesKeys', function () {
        it('counts the series of every measurement', function () {
            assert.deepStrictEqual(
                cardinalityFromSeriesKeys([
                    'influxdb.0.temperature',
                    'influxdb.0.humidity,room=kitchen',
                    'influxdb.0.humidity,room=bath',
                ]),
                { 'influxdb.0.temperature': 1, 'influxdb.0.humidity': 2 },
            );
        });

        it('does not split a measurement at an escaped comma', function () {
            // naive splitting would attribute this series to the measurement "a\"
            assert.deepStrictEqual(cardinalityFromSeriesKeys(['a\\,b,room=kitchen']), { 'a,b': 1 });
        });

        it('unescapes the measurement, so it matches the name SHOW MEASUREMENTS reports', function () {
            assert.deepStrictEqual(cardinalityFromSeriesKeys(['a\\ b']), { 'a b': 1 });
        });

        it('ignores empty keys and an empty answer', function () {
            assert.deepStrictEqual(cardinalityFromSeriesKeys([]), {});
            assert.deepStrictEqual(cardinalityFromSeriesKeys(['', 'a']), { a: 1 });
        });
    });
});
