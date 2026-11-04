/**
 * Pure helpers of the InfluxDB 3.x driver.
 *
 * Kept apart from `DatabaseInfluxDB3x.ts` so they can be unit tested without a server. InfluxDB 3 is
 * reached over plain HTTP (line protocol for writing, InfluxQL for reading), which means the driver has
 * to do two things the 1.x/2.x client libraries do for it: serialize points and split statements.
 */

import type { ValuesForInflux } from './Database';

/** Escape a measurement name for the line protocol */
function escapeMeasurement(name: string): string {
    return name.replace(/\n/g, ' ').replace(/\\/g, '\\\\').replace(/,/g, '\\,').replace(/ /g, '\\ ');
}

/** Escape a tag key, tag value or field key for the line protocol */
function escapeKey(name: string): string {
    return name
        .replace(/\n/g, ' ')
        .replace(/\\/g, '\\\\')
        .replace(/,/g, '\\,')
        .replace(/=/g, '\\=')
        .replace(/ /g, '\\ ');
}

/** Escape a string field value (without the surrounding quotes) */
function escapeStringValue(value: string): string {
    return value.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
}

/**
 * Serialize one ioBroker value as a line of the InfluxDB line protocol.
 *
 * `q`, `ack` and `from` follow the same rule as in the 2.x driver: tags if `useTags` is set, fields
 * otherwise. Custom tags are always tags. Empty tag values are left out, the line protocol cannot
 * express them.
 *
 * @param measurement the ID of the datapoint
 * @param value the value to store
 * @param useTags write `q`, `ack` and `from` as tags
 * @returns the line, without a trailing newline
 */
export function toLineProtocol(measurement: string, value: ValuesForInflux, useTags: boolean): string {
    const tags: { [name: string]: string } = {};
    if (useTags) {
        tags.q = String(value.q);
        tags.ack = String(value.ack);
        tags.from = value.from;
    }
    if (value.tags) {
        Object.assign(tags, value.tags);
    }

    // sorted by key, which is what the InfluxDB documentation recommends for write performance
    const tagText = Object.keys(tags)
        .sort()
        .filter(name => tags[name] !== undefined && tags[name] !== null && String(tags[name]) !== '')
        .map(name => `${escapeKey(name)}=${escapeKey(String(tags[name]))}`)
        .join(',');

    const fields: string[] = [];
    switch (typeof value.value) {
        case 'boolean':
            fields.push(`value=${value.value}`);
            break;
        case 'number':
            if (!isFinite(value.value)) {
                throw new Error(`Cannot write a non-finite number: ${value.value}`);
            }
            fields.push(`value=${value.value}`);
            break;
        default:
            fields.push(`value="${escapeStringValue(String(value.value))}"`);
            break;
    }
    if (!useTags) {
        fields.push(`q=${Number(value.q) || 0}`);
        fields.push(`ack=${!!value.ack}`);
        fields.push(`from="${escapeStringValue(value.from || '')}"`);
    }

    return `${escapeMeasurement(measurement)}${tagText ? `,${tagText}` : ''} ${fields.join(',')} ${Math.round(value.time)}`;
}

/**
 * Split a text with several InfluxQL statements at the `;` between them.
 *
 * The server accepts exactly one statement per request, but `getHistory` sends up to three at once.
 * A `;` inside a quoted identifier or string is part of it (an ioBroker ID can contain anything), so
 * quotes have to be tracked - a plain `split(';')` would cut `SELECT * FROM "a;b"` in two.
 *
 * @param text one or more statements
 * @returns the non-empty statements
 */
export function splitStatements(text: string): string[] {
    const statements: string[] = [];
    let current = '';
    let quote: '"' | "'" | null = null;

    for (let i = 0; i < text.length; i++) {
        const char = text[i];
        if (quote) {
            current += char;
            if (char === '\\' && i + 1 < text.length) {
                current += text[++i];
            } else if (char === quote) {
                quote = null;
            }
        } else if (char === '"' || char === "'") {
            quote = char;
            current += char;
        } else if (char === ';') {
            if (current.trim()) {
                statements.push(current.trim());
            }
            current = '';
        } else {
            current += char;
        }
    }
    if (current.trim()) {
        statements.push(current.trim());
    }
    return statements;
}

/**
 * Is this the leftover of a deleted table?
 *
 * InfluxDB 3 does not remove a deleted table from the catalog: it renames it to `<name>-YYYYMMDDTHHMMSS` and keeps
 * listing it. There is no flag to tell it apart, only the suffix.
 *
 * @param table name of a table as the server lists it
 */
export function isDeletedTable(table: string): boolean {
    return /-\d{8}T\d{6}$/.test(table);
}

/**
 * Turn a timestamp of a query result into a Date.
 *
 * InfluxDB 3 answers with UTC timestamps that carry no zone marker (`2026-10-21T07:10:58.316`), which
 * `new Date()` would read as local time.
 *
 * @param time the `time` column of a result row
 * @returns the point in time
 */
export function parseInfluxTime(time: string | number | Date): Date {
    if (typeof time === 'string' && !/(Z|[+-]\d{2}:?\d{2})$/i.test(time)) {
        return new Date(`${time}Z`);
    }
    return new Date(time);
}

/**
 * Express a rejected write the way InfluxDB 1.x did.
 *
 * The write path of the adapter recognizes a type conflict by the 1.x wording (`field type conflict`,
 * `is type string, already exists as type float`) and uses it to convert the value and pin the
 * datapoint's storage type. InfluxDB 3 says the same thing differently - `invalid column type for column
 * 'value', expected iox::column_type::field::float, got iox::column_type::field::string` - so the
 * driver translates it instead of teaching the adapter a second dialect.
 *
 * @param errorMessage `error_message` of one rejected line
 * @param line `original_line` of the rejected line, used to find the measurement
 * @returns the 1.x style text, or the message unchanged if it is no type conflict
 */
export function translateWriteError(errorMessage: string, line?: string): string {
    const match = /invalid column type for column '([^']+)', expected (?:\S*::)?(\w+), got (?:\S*::)?(\w+)/.exec(
        errorMessage,
    );
    if (!match) {
        return errorMessage;
    }
    const typeName = (type: string): string => (type === 'boolean' ? 'bool' : type);
    const measurement = line ? /^((?:[^,\s\\]|\\.)*)/.exec(line)?.[1].replace(/\\(.)/g, '$1') : '';
    return (
        `field type conflict: input field "${match[1]}" on measurement "${measurement || ''}" ` +
        `is type ${typeName(match[3])}, already exists as type ${typeName(match[2])}`
    );
}
