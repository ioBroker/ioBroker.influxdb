export type ValuesForInflux = {
    value: string | number | boolean;
    time: number;
    from: string;
    q: number;
    ack: boolean;
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
    abstract getHostsAvailable(): number;
    abstract deleteData(
        start: Date | number,
        stop: Date | number,
        org: string,
        dbName: string,
        query: string,
    ): Promise<void>;

    abstract query<T>(query: string): Promise<Array<T & { time: Date }>>;

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
                this.log.warn(`Error in query "${query}": ${error}`);
                errors.push(error);
                collectedRows.push([] as any);
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
