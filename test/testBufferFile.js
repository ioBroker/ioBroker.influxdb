const assert = require('node:assert');
const { normalizeBufferedPoint, normalizeSeriesBuffer } = require('../build/lib/bufferFile');

describe('Test reading the buffer file influxdata.json', function () {
    it('unwraps a point of version 4 (#517)', function () {
        // as version 4.0.2 wrote it: every point wrapped in an array, the time as date
        const file = JSON.parse(
            JSON.stringify({
                seriesBufferCounter: 2,
                seriesBuffer: {
                    'alias.0.Strom.Panels.Panel_Osten1': [
                        [
                            {
                                value: 5,
                                time: new Date('2026-08-17T17:48:59.960Z'),
                                from: 'system.adapter.ankersolix2.0',
                                q: 0,
                                ack: true,
                            },
                        ],
                    ],
                    'system.host.MSNUC-IOB01.mem': [
                        [
                            {
                                value: 43,
                                time: new Date('2026-08-17T17:48:11.219Z'),
                                from: 'system.host.MSNUC-IOB01',
                                q: 0,
                                ack: true,
                            },
                        ],
                    ],
                },
            }),
        );

        const { series, count, dropped } = normalizeSeriesBuffer(file.seriesBuffer);

        assert.strictEqual(count, 2);
        assert.strictEqual(dropped, 0);
        assert.deepStrictEqual(series['alias.0.Strom.Panels.Panel_Osten1'], [
            {
                value: 5,
                time: Date.parse('2026-08-17T17:48:59.960Z'),
                from: 'system.adapter.ankersolix2.0',
                q: 0,
                ack: true,
            },
        ]);
        // without the fix q was undefined, because the array was taken for the point
        assert.strictEqual(series['system.host.MSNUC-IOB01.mem'][0].q, 0);
    });

    it('keeps a point of version 5 as it is', function () {
        const point = {
            value: 'on',
            time: 1792575657104,
            from: 'system.adapter.x.0',
            q: 0,
            ack: false,
            tags: { room: 'kitchen' },
        };
        const { series, count } = normalizeSeriesBuffer(JSON.parse(JSON.stringify({ 'x.0.y': [point] })));

        assert.strictEqual(count, 1);
        assert.deepStrictEqual(series['x.0.y'], [point]);
    });

    it('drops entries that are no usable point', function () {
        const { series, count, dropped } = normalizeSeriesBuffer({
            'a.0.b': [
                { value: 1, time: 1792575657104, from: '', q: 0, ack: true },
                { value: null, time: 1792575657104 }, // InfluxDB can not store null
                { value: 2, time: null }, // would be 1970
                { value: 3, time: 'not a date' },
                { value: 4 }, // no time
                null,
                'garbage',
                [],
            ],
            'c.0.d': 'no list',
        });

        assert.strictEqual(count, 1);
        assert.strictEqual(dropped, 8);
        assert.deepStrictEqual(Object.keys(series), ['a.0.b']);
    });

    it('normalizes q, ack, from and tags', function () {
        const point = normalizeBufferedPoint({
            value: true,
            time: '2026-08-17T17:48:59.960Z',
            q: '0x01',
            ack: 'true',
            tags: { room: 'kitchen', broken: 5 },
        });

        assert.deepStrictEqual(point, {
            value: true,
            time: Date.parse('2026-08-17T17:48:59.960Z'),
            from: '',
            q: 1,
            ack: true,
            tags: { room: 'kitchen' },
        });
    });

    it('accepts an empty or missing buffer', function () {
        assert.deepStrictEqual(normalizeSeriesBuffer(undefined), { series: {}, count: 0, dropped: 0 });
        assert.deepStrictEqual(normalizeSeriesBuffer({}), { series: {}, count: 0, dropped: 0 });
    });
});
