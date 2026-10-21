// Only the first few sub-errors of an AggregateError are rendered - a DNS name with many A/AAAA
// records would otherwise produce a log line of arbitrary length.
const MAX_NESTED_ERRORS = 5;
const MAX_DEPTH = 3;

/**
 * Error names that say nothing about the cause. `Error` is the default of every `new Error()`, and
 * `AggregateError` is just the box Node puts the real connect errors into - neither belongs in a log
 * line. A driver-specific name like `HttpError` or `ServiceNotAvailableError` is kept, it does tell something.
 */
const GENERIC_ERROR_NAMES = ['Error', 'AggregateError'];

/** Fields the InfluxDB drivers put on their errors that are worth showing when the message alone says nothing */
const DETAIL_FIELDS = ['code', 'errno', 'syscall', 'address', 'port', 'statusCode'] as const;

/** Socket/DNS level failures: the database is not reachable, retrying the same point makes no sense */
const CONNECTION_ERROR_CODES = [
    'EAI_AGAIN',
    'ECONNABORTED',
    'ECONNREFUSED',
    'ECONNRESET',
    'EHOSTDOWN',
    'EHOSTUNREACH',
    'ENETDOWN',
    'ENETUNREACH',
    'ENOTFOUND',
    'EPIPE',
    'EPROTO',
    'ESOCKETTIMEDOUT',
    'ETIMEDOUT',
    'ERR_SOCKET_CONNECTION_TIMEOUT',
];

/** Error classes that only ever mean "no usable host" - the driver's own and ours */
const CONNECTION_ERROR_NAMES = ['ServiceNotAvailableError', 'HostUnavailableError'];

/** HTTP status codes of a gateway/proxy in front of InfluxDB that mean the server itself is down */
const CONNECTION_ERROR_STATUS_CODES = [502, 503, 504];

/** Wordings the drivers use for the same situation when they do not set a code */
const CONNECTION_ERROR_PATTERN =
    /(timeout|timed out|socket hang up|no host available|service not available|network error|connection (closed|refused|lost|terminated))/i;

/**
 * A value that InfluxDB is not able to store at all (`null`, `NaN`/`Infinity`, or a non-numeric value
 * for a datapoint pinned to `Number`).
 *
 * Such a value is not an operational problem - some adapters simply deliver them all the time - so the
 * caller logs it once per datapoint instead of on every state change, while `storeState` still answers
 * with a real error instead of a silent `success: true`.
 */
export class UnstorableValueError extends Error {
    /** Which of the three checks rejected the value - used as part of the "logged already" key */
    public readonly kind: 'null' | 'nonFinite' | 'type';

    constructor(kind: 'null' | 'nonFinite' | 'type', message: string) {
        super(message);
        this.name = 'UnstorableValueError';
        this.kind = kind;
    }
}

/**
 * The request was not even attempted, because the host is known to be unreachable.
 *
 * Thrown instead of a plain `Error`, so the caller can throttle it like any other connection error
 * (`isConnectionError()` knows this class) rather than repeating it on every buffered value.
 */
export class HostUnavailableError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'HostUnavailableError';
    }
}

/**
 * Render an error as one readable line.
 *
 * `String(err)` is not good enough for connection errors: Node's happy-eyeballs connect (and with it
 * every driver that opens a TCP socket) rejects with an `AggregateError` whose own `message` is empty,
 * so `toString()` degrades to a bare "AggregateError" and the actual reason - "connect ECONNREFUSED
 * 127.0.0.1:8086" for each address that was tried - stays hidden in `err.errors`. Unwrap that array,
 * unwrap `err.cause`, and append the driver's error code when the message does not contain it already.
 *
 * @param err whatever was caught or handed to a callback
 * @param depth recursion depth, used internally for nested errors
 * @returns a non-empty, single-line description
 */
export function formatError(err: unknown, depth = 0): string {
    if (err === null || err === undefined) {
        return 'Unknown error';
    }
    if (typeof err === 'string') {
        return err || 'Unknown error';
    }
    if (typeof err === 'number' || typeof err === 'boolean' || typeof err === 'bigint') {
        return String(err);
    }
    if (typeof err !== 'object') {
        // symbol or function - nothing sensible to print
        return Object.prototype.toString.call(err);
    }

    const error = err as Record<string, any>;
    const message = typeof error.message === 'string' ? oneLine(error.message) : '';
    const name = typeof error.name === 'string' && error.name ? error.name : '';

    let text = '';
    if (name && !GENERIC_ERROR_NAMES.includes(name)) {
        text = message ? `${name}: ${message}` : name;
    } else {
        text = message;
    }

    // AggregateError: the reason is in `errors`, not in `message`
    if (Array.isArray(error.errors) && error.errors.length && depth < MAX_DEPTH) {
        const details: string[] = [];
        for (const nested of error.errors.slice(0, MAX_NESTED_ERRORS)) {
            const nestedText = describeNested(nested, depth + 1);
            // both stacks of a happy-eyeballs connect fail the same way more often than not
            if (nestedText && !details.includes(nestedText)) {
                details.push(nestedText);
            }
        }
        if (error.errors.length > MAX_NESTED_ERRORS) {
            details.push(`and ${error.errors.length - MAX_NESTED_ERRORS} more`);
        }
        if (details.length) {
            text = text ? `${text}: ${details.join('; ')}` : details.join('; ');
        }
    }

    for (const field of DETAIL_FIELDS) {
        const value = error[field];
        if ((typeof value === 'string' && value) || typeof value === 'number') {
            if (!text.includes(String(value))) {
                text = text ? `${text} (${field}: ${value})` : `${field}: ${value}`;
            }
        }
    }

    if (error.cause !== undefined && error.cause !== null && depth < MAX_DEPTH) {
        const cause = formatError(error.cause, depth + 1);
        if (cause && !text.includes(cause)) {
            text = text ? `${text}; caused by ${cause}` : cause;
        }
    }

    if (!text) {
        // nothing but an empty shell - show what it actually holds
        try {
            const json = JSON.stringify(error);
            if (json && json !== '{}') {
                return json;
            }
        } catch {
            // circular or non-serializable - fall through
        }
        return name || Object.prototype.toString.call(error);
    }

    return text;
}

/**
 * Is this error "the database is not reachable" and not "this point is bad"?
 *
 * The write path escalates a failed batch down to single points to find the one InfluxDB rejects. That
 * is exactly the wrong reaction to an unreachable server: every point fails, every point is logged, and
 * a switched-off database fills the log. The distinction cannot be made on `message` alone, because
 * Node reports a failed connect as an `AggregateError` with an empty message and the real code either
 * on the aggregate or on its sub-errors.
 *
 * @param err whatever was caught
 * @param depth recursion depth, used internally for nested errors
 * @returns true if the error means the host is (currently) not usable
 */
export function isConnectionError(err: unknown, depth = 0): boolean {
    if (typeof err === 'string') {
        return CONNECTION_ERROR_PATTERN.test(err) || CONNECTION_ERROR_CODES.some(code => err.includes(code));
    }
    if (!err || typeof err !== 'object') {
        return false;
    }

    const error = err as Record<string, any>;

    if (typeof error.code === 'string' && CONNECTION_ERROR_CODES.includes(error.code)) {
        return true;
    }
    if (typeof error.name === 'string' && CONNECTION_ERROR_NAMES.includes(error.name)) {
        return true;
    }
    if (typeof error.statusCode === 'number' && CONNECTION_ERROR_STATUS_CODES.includes(error.statusCode)) {
        return true;
    }
    // a client error (4xx) is the answer of a reachable server; its message may quote user data such as the
    // ID of a datapoint ("...timeout"), which must not be mistaken for a network problem
    const isClientError = typeof error.statusCode === 'number' && error.statusCode >= 400 && error.statusCode < 500;
    if (!isClientError && typeof error.message === 'string' && CONNECTION_ERROR_PATTERN.test(error.message)) {
        return true;
    }

    if (depth >= MAX_DEPTH) {
        return false;
    }

    if (Array.isArray(error.errors) && error.errors.some((nested: unknown) => isConnectionError(nested, depth + 1))) {
        return true;
    }

    return error.cause !== undefined && error.cause !== null && isConnectionError(error.cause, depth + 1);
}

/**
 * Squeeze all whitespace into single spaces, so a multi-line driver message stays one log line
 *
 * @param text the text to normalize
 * @returns the text without line breaks
 */
function oneLine(text: string): string {
    return text.replace(/\s+/g, ' ').trim();
}

/**
 * Describe one sub-error of an AggregateError: the message alone if there is one, because it usually
 * already carries the code ("connect ECONNREFUSED 127.0.0.1:8086").
 *
 * @param err the nested error
 * @param depth current recursion depth
 * @returns a short description of the nested error
 */
function describeNested(err: unknown, depth: number): string {
    if (err && typeof err === 'object') {
        const nested = err as Record<string, any>;
        const message = typeof nested.message === 'string' ? oneLine(nested.message) : '';
        if (message) {
            const code = typeof nested.code === 'string' ? nested.code : '';
            return code && !message.includes(code) ? `${message} (${code})` : message;
        }
    }
    return formatError(err, depth);
}
