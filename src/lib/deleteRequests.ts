/**
 * Parsing and validation of the time stamps of `delete`, `deleteRange` and `update`.
 *
 * `ts`, `start` and `end` were taken as they came, and whatever was missing, 0 or not a date fell
 * through to "delete every value of the datapoint": a single `{id, state: {start, end}}` or
 * `{id, state: {}}` reached `_delete` as `{ts: undefined}`, an unparsable `start` became `NaN`. That
 * deleted everything since 2000 on InfluxDB 1.x, everything since 1970 on 2.x and the whole table on
 * 3.x. Every entry is checked here, and `_delete` refuses an invalid range itself as well.
 */

/** What `_delete` removes of one datapoint. Everything (`all`) is only removed if asked for explicitly */
export type DeleteRange = { mode: 'one'; ts: number } | { mode: 'range'; start: number; end: number } | { mode: 'all' };

/** One entry of `delete` / `deleteRange` after validation */
export interface DeleteRequest {
    /** ID of the datapoint, aliases already resolved */
    id: string;
    range: DeleteRange;
}

/** Largest distance from 1970 in ms a `Date` can hold */
const MAX_DATE_MS = 8.64e15;

function isTimestamp(value: unknown): value is number {
    return typeof value === 'number' && Number.isFinite(value) && Math.abs(value) <= MAX_DATE_MS;
}

/** A value for an error text: `NaN` and `undefined` are not readable as JSON */
function show(value: unknown): string {
    return typeof value === 'number' || value === undefined ? String(value) : JSON.stringify(value);
}

/**
 * Time stamp in ms from a number, a numeric string, a date string or a `Date`.
 *
 * @param value the value of `ts`, `start` or `end` from the message
 * @returns the time stamp, or `undefined` if the value is none. 0 is a valid time stamp.
 */
export function toTimestamp(value: unknown): number | undefined {
    let ts: number;
    if (typeof value === 'number') {
        ts = value;
    } else if (typeof value === 'string' && value.trim()) {
        const text = value.trim();
        // "1589458809352" is a number: `new Date()` would read it as a date and return NaN
        ts = /^-?\d+(\.\d+)?$/.test(text) ? Number(text) : new Date(text).getTime();
    } else if (value instanceof Date) {
        ts = value.getTime();
    } else {
        return undefined;
    }
    return isTimestamp(ts) ? ts : undefined;
}

/**
 * Why a range must not be deleted, or `null` if it is valid.
 *
 * The second line of defence in `_delete`: whatever a caller passes, a missing or invalid time stamp
 * must never turn into deleting every value.
 *
 * @param range what the caller wants to delete
 */
export function deleteRangeProblem(range: DeleteRange | null | undefined): string | null {
    if (!range || typeof range !== 'object') {
        return 'neither "ts" nor a range given';
    }
    if (range.mode === 'one') {
        return isTimestamp(range.ts) ? null : `invalid "ts" ${show(range.ts)}`;
    }
    if (range.mode === 'range') {
        if (!isTimestamp(range.start)) {
            return `invalid "start" ${show(range.start)}`;
        }
        return isTimestamp(range.end) ? null : `invalid "end" ${show(range.end)}`;
    }
    if (range.mode === 'all') {
        return null;
    }
    return `unknown delete mode ${show((range as { mode?: unknown }).mode)}`;
}

/**
 * Turn the message of `delete` or `deleteRange` into a list of requests.
 *
 * Both commands accept the same shapes: `{id, ts}`, `{id, start, end?}`, `{id, state: {ts}}`,
 * `{id, state: {start, end?}}`, each also as a list, plus `{id, state: [...]}` and `{id, ts: [...]}`
 * for several entries of one datapoint (a number in these lists is a `ts`). `ts` deletes the values
 * with exactly this time stamp, `start` / `end` the values in `[start, end]`; `end` defaults to now,
 * `end` without `start` is refused like in the history and sql adapters. Time stamps are ms or date
 * strings, 0 is valid. An entry without ID, without `ts` or `start`, or with a value that is no time
 * stamp is reported in `errors` and not returned in `requests`.
 *
 * @param message `msg.message` as delivered
 * @param now the time stamp `end` defaults to
 * @param resolveId maps an alias to the ID of the datapoint
 */
export function parseDeleteRequests(
    message: unknown,
    now: number,
    resolveId: (id: string) => string = id => id,
): { requests: DeleteRequest[]; errors: string[] } {
    const requests: DeleteRequest[] = [];
    const errors: string[] = [];

    const one = (id: unknown, entry: unknown): void => {
        if (typeof id !== 'string' || !id) {
            errors.push(`No "id" given for ${show(entry)}`);
            return;
        }
        // a plain value is a time stamp (`{id, ts: [1, 2]}`), an object carries ts or start/end
        const spec: Record<string, unknown> =
            entry && typeof entry === 'object' ? (entry as Record<string, unknown>) : { ts: entry };
        if (spec.ts !== undefined && spec.ts !== null) {
            const ts = toTimestamp(spec.ts);
            if (ts === undefined) {
                errors.push(`Invalid "ts" ${show(spec.ts)} for ${id}`);
            } else {
                requests.push({ id: resolveId(id), range: { mode: 'one', ts } });
            }
            return;
        }
        if (spec.start !== undefined && spec.start !== null) {
            const start = toTimestamp(spec.start);
            const end = spec.end === undefined || spec.end === null ? now : toTimestamp(spec.end);
            if (start === undefined) {
                errors.push(`Invalid "start" ${show(spec.start)} for ${id}`);
            } else if (end === undefined) {
                errors.push(`Invalid "end" ${show(spec.end)} for ${id}`);
            } else {
                requests.push({ id: resolveId(id), range: { mode: 'range', start, end } });
            }
            return;
        }
        if (spec.end !== undefined && spec.end !== null) {
            errors.push(`"start" is missing for ${id}: a range needs "start", "end" is optional`);
            return;
        }
        errors.push(`Neither "ts" nor "start" given for ${id}: ${show(entry)}`);
    };

    /** `{id, ts}` / `{id, start, end}` carry the time themselves, otherwise it is in `state` */
    const timeOf = (item: Record<string, unknown>): unknown =>
        (item.ts === undefined || item.ts === null) &&
        (item.start === undefined || item.start === null) &&
        item.state &&
        typeof item.state === 'object'
            ? item.state
            : item;

    if (Array.isArray(message)) {
        for (const item of message) {
            if (!item || typeof item !== 'object') {
                errors.push(`Invalid entry ${show(item)}`);
            } else {
                one(item.id, timeOf(item));
            }
        }
    } else if (message && typeof message === 'object') {
        const msg = message as Record<string, unknown>;
        if (Array.isArray(msg.state)) {
            msg.state.forEach(entry => one(msg.id, entry));
        } else if (Array.isArray(msg.ts)) {
            msg.ts.forEach(ts => one(msg.id, ts));
        } else {
            one(msg.id, timeOf(msg));
        }
    } else {
        errors.push(`Invalid call: ${show(message)}`);
    }

    return { requests, errors };
}
