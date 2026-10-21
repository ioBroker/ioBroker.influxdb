import type { ValuesForInflux } from './Database';

/** Result of reading the buffered points of the file `influxdata.json` */
export type LoadedSeriesBuffer = {
    /** the points in the form the write path expects, per measurement */
    series: { [id: string]: ValuesForInflux[] };
    /** number of points in `series` */
    count: number;
    /** number of entries that could not be used and were dropped */
    dropped: number;
};

/**
 * Bring one buffered point into the form of this version.
 *
 * The file is read by whatever version starts next, so it may come from an older one. Version 4 stored
 * every point wrapped in an array (`[point]`) and its time as a date, version 5 stores the point itself
 * with the time in milliseconds. Taken over as it is, the wrapped point of version 4 has no `q`, so every
 * write of it fails ("invalid float value for field 'q': undefined") - and it is put back into the buffer
 * after every failure, also into the file on the next exit.
 *
 * @param entry one entry of a measurement in the file
 * @returns the point, or null if the entry is no usable point
 */
export function normalizeBufferedPoint(entry: unknown): ValuesForInflux | null {
    // version 4: `[point]`
    const point = (Array.isArray(entry) ? entry[0] : entry) as Partial<Record<keyof ValuesForInflux, unknown>>;
    if (!point || typeof point !== 'object' || Array.isArray(point)) {
        return null;
    }

    const value = point.value;
    if (typeof value !== 'string' && typeof value !== 'boolean' && (typeof value !== 'number' || !isFinite(value))) {
        return null;
    }

    // version 4 stored a date, which is a string in JSON; version 5 milliseconds.
    // `new Date(null)` would be 1970, so a missing time is checked first
    if (point.time === null || point.time === undefined) {
        return null;
    }
    const time = typeof point.time === 'number' ? point.time : new Date(point.time as string).getTime();
    if (!isFinite(time)) {
        return null;
    }

    const normalized: ValuesForInflux = {
        value,
        time: Math.round(time),
        from: typeof point.from === 'string' ? point.from : '',
        q: Number(point.q) || 0,
        ack: point.ack === true || point.ack === 'true',
    };

    if (point.tags && typeof point.tags === 'object' && !Array.isArray(point.tags)) {
        const tags: { [name: string]: string } = {};
        for (const [name, tagValue] of Object.entries(point.tags as Record<string, unknown>)) {
            if (typeof tagValue === 'string') {
                tags[name] = tagValue;
            }
        }
        if (Object.keys(tags).length) {
            normalized.tags = tags;
        }
    }
    return normalized;
}

/**
 * Bring the buffered points of the file `influxdata.json` into the form of this version.
 *
 * The counter is taken from the points, not from the file: it would be wrong for dropped entries.
 *
 * @param seriesBuffer `seriesBuffer` of the file
 */
export function normalizeSeriesBuffer(seriesBuffer: unknown): LoadedSeriesBuffer {
    const result: LoadedSeriesBuffer = { series: {}, count: 0, dropped: 0 };
    if (!seriesBuffer || typeof seriesBuffer !== 'object' || Array.isArray(seriesBuffer)) {
        return result;
    }
    for (const [id, entries] of Object.entries(seriesBuffer as Record<string, unknown>)) {
        if (!Array.isArray(entries)) {
            result.dropped++;
            continue;
        }
        const points: ValuesForInflux[] = [];
        for (const entry of entries) {
            const point = normalizeBufferedPoint(entry);
            if (point) {
                points.push(point);
            } else {
                result.dropped++;
            }
        }
        if (points.length) {
            result.series[id] = points;
            result.count += points.length;
        }
    }
    return result;
}
