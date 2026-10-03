import { formatError, isConnectionError } from './errors';
import type { MeasurementStatistics } from './statistics';

/**
 * How long a host counts as unusable after a connection error.
 *
 * The 1.x driver pool takes a host out of rotation the same way, and without an expiry the adapter
 * would keep buffering forever, because only a successful request could ever bring the host back -
 * and no request is sent while the host counts as unavailable. Roughly the reconnect interval.
 */
const HOST_UNAVAILABLE_TIME = 10_000;

/** Oldest timestamp the adapter looks at. It never writes a point before the epoch */
export const MIN_INFLUX_TIME = 0;
/** Newest timestamp a Flux range may stop at. InfluxDB cannot store anything after it */
export const MAX_INFLUX_TIME = Date.UTC(2262, 3, 11);

export type ValuesForInflux = {
    value: string | number | boolean;
    time: number;
    from: string;
    q: number;
    ack: boolean;
    /** Custom tags of the datapoint (name -> value), see `customTags` in the datapoint settings */
    tags?: { [name: string]: string };
};

/**
 * Escape an InfluxQL identifier (e.g. a measurement or database name) that is placed inside
 * double quotes in a query, to prevent InfluxQL injection via the ioBroker state id.
 */
export function escapeInfluxQLIdentifier(id: string | undefined): string {
    return String(id).replace(/\\/g, '\\\\').replace(/"/g, '\\"');
}

/**
 * Escape a value that is placed inside a Flux double-quoted string literal, to prevent Flux
 * injection (incl. Flux string interpolation via ${...}) via the ioBroker state id or db name.
 */
export function escapeFluxString(value: string | undefined): string {
    return String(value).replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\$\{/g, '\\${');
}

export abstract class Database {
    protected log: ioBroker.Logger;
    protected readonly host: string;
    protected readonly port: number | string;
    protected readonly protocol: 'http' | 'https';
    protected readonly database: string;
    protected readonly requestTimeout: number;
    /** 0 if the host is usable, otherwise the time of the connection error that took it out of rotation */
    private hostUnavailableSince = 0;

    protected constructor(options: {
        log: ioBroker.Logger;
        host: string;
        port: number | string;
        protocol: 'http' | 'https';
        database: string;
        requestTimeout: number;
    }) {
        this.log = options.log;
        this.host = options.host;
        this.port = options.port;
        this.protocol = options.protocol;
        this.database = options.database;
        this.requestTimeout = options.requestTimeout;
    }
    /**
     * Is the InfluxDB host currently usable?
     *
     * The adapter uses this to decide whether it may write at all or has to buffer: writing point by
     * point against a server that is not reachable produces one error (and one log line) per point.
     * A host that failed with a connection error is taken out of rotation for a short while and is
     * then tried again - exactly what the connection pool of the 1.x driver does internally.
     *
     * @returns 1 while the host may be used, 0 while it is known to be unreachable
     */
    getHostsAvailable(): number {
        if (this.hostUnavailableSince && Date.now() - this.hostUnavailableSince < HOST_UNAVAILABLE_TIME) {
            return 0;
        }
        // the backoff is over: give the host another try
        this.hostUnavailableSince = 0;
        return 1;
    }

    /** Report that the host answered, so it counts as usable again */
    protected markHostAvailable(): void {
        this.hostUnavailableSince = 0;
    }

    /** Report that the host is not reachable, so the adapter buffers instead of writing point by point */
    protected markHostUnavailable(): void {
        this.hostUnavailableSince = Date.now();
    }

    /**
     * Run a request and remember whether the host answered.
     *
     * Only connection errors change the state: a rejected point ("field type conflict", "unauthorized")
     * says nothing about the reachability of the server and must not stop the adapter from writing.
     *
     * @param action the request to execute
     * @returns whatever the request returned
     */
    protected async trackConnection<T>(action: () => Promise<T>): Promise<T> {
        try {
            const result = await action();
            this.markHostAvailable();
            return result;
        } catch (error) {
            if (isConnectionError(error)) {
                this.markHostUnavailable();
            }
            throw error;
        }
    }

    abstract connect(): void;
    abstract getDatabaseNames(): Promise<string[]>;
    abstract createDatabase(dbname: string): Promise<void>;
    abstract dropDatabase(dbname: string): Promise<void>;
    abstract ping(): Promise<{ online: boolean }[]>;
    abstract applyRetentionPolicyToDB(dbName: string, retention: number): Promise<void>;
    abstract getMetaDataStorageType(): Promise<'tags' | 'fields' | 'none'>;
    abstract getRetentionPolicyForDB(dbName: string): Promise<{ name: string | null; time: number | undefined } | null>;

    // write many series with many points
    abstract writeSeries(series: { [id: string]: ValuesForInflux[] }): Promise<void>;
    // write one series with many points
    abstract writePoints(seriesId: string, pointsToSend: ValuesForInflux[]): Promise<void>;
    // write one point to one series
    abstract writePoint(seriesId: string, value: ValuesForInflux): Promise<void>;
    abstract deleteData(
        start: Date | number,
        stop: Date | number,
        org: string,
        dbName: string,
        query: string,
    ): Promise<void>;

    abstract query<T>(query: string): Promise<Array<T & { time: Date }>>;

    /**
     * Number of values, oldest/newest timestamp and series count of every measurement.
     *
     * InfluxDB exposes no byte size per measurement - neither 1.x (`SHOW STATS` and `SHOW SHARDS`
     * only know the engine and the shards) nor 2.x (disk size is monitored per bucket). Series
     * cardinality is reported instead: it is both available and meaningful, because it drives the
     * memory footprint of the index.
     *
     * One pass over the whole database, not one query per measurement: an installation that has
     * been logging for years holds thousands of IDs. Measurements without a value in the range are
     * not reported - the caller fills them in as "empty", so a datapoint whose logging stopped
     * before the range still shows up in the statistics.
     *
     * @param start beginning of the examined range (ms)
     * @param stop end of the examined range (ms)
     */
    abstract getStatistics(start: number, stop: number): Promise<MeasurementStatistics>;

    /**
     * Remove a measurement with everything it contains.
     *
     * Used by `cleanupOrphaned` for datapoints nobody logs any more. Unlike deleting a time range
     * this also drops the series from the index, which is the point of the cleanup.
     *
     * @param measurement the measurement to remove
     */
    abstract dropMeasurement(measurement: string): Promise<void>;

    async queries<T>(queries: string[]): Promise<Array<T & { time: Date }>[] | null> {
        const collectedRows: Array<T & { time: Date }>[] = [];
        let success = false;
        const errors: string[] = [];
        for (const query of queries) {
            try {
                const rows = await this.query(query);
                success = true;
                collectedRows.push(rows as Array<T & { time: Date }>);
            } catch (error) {
                this.log.warn(`Error in query "${query}": ${formatError(error)}`);
                errors.push(error);
                collectedRows.push([]);
            }
        }

        if (errors.length) {
            throw new Error(`${errors.length} Error happened while processing ${queries.length} queries`);
        }
        return success ? collectedRows : null;
    }

    calculateShardGroupDuration(retentionTime: number): number {
        // in seconds
        // Shard Group Duration according to official Influx recommendations
        if (!retentionTime) {
            // infinite
            return 604800; // 7 days
        }
        if (retentionTime < 172800) {
            // < 2 days
            return 3600; // 1 hour
        }
        if (retentionTime >= 172800 && retentionTime <= 15811200) {
            // >= 2 days, <= 6 months (~182 days)
            return 86400; // 1 day
        }
        // > 6 months
        return 604800; // 7 days
    }
}
