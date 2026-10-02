// The types of the `getHistory` contract and of the aggregation are shared with the history and sql
// adapters, so they live in `@iobroker/aggregate` and are only re-exported here.
import type { CustomTagRow, CustomTags } from './lib/customTags';

export type {
    AggregateMethod,
    DataEntry,
    GetHistoryOptions,
    GetHistoryOptionsExtended,
    GetStatistics,
    InternalHistoryOptions,
    IobDataEntry,
    ProcessingEntry,
    SmartDate,
    TimeInterval,
} from '@iobroker/aggregate';

/** Type a value is stored as. It is fixed by InfluxDB with the first written point of a measurement */
export type StorageType = 'Number' | 'String' | 'Boolean';

/** Options of the `getRawEntries` message, already sanitized by the adapter */
export interface RawEntriesOptions {
    /** oldest timestamp to return (inclusive) */
    start?: number;
    /** newest timestamp to return (inclusive) */
    end?: number;
    /** maximal number of returned entries */
    limit: number;
    /** number of entries to skip */
    offset: number;
    /** sort order by timestamp */
    sort: 'asc' | 'desc';
}

export interface InfluxDBAdapterConfig {
    debounce: number | string;
    retention: number | string;
    dbname: string;
    host: string;
    protocol: 'http' | 'https';
    path: string;
    port: number | string;
    user: string;
    password: string;
    token: string;
    organization: string;
    round: number | string | null;
    seriesBufferMax: number | string;
    seriesBufferFlushInterval: number | string;
    changesRelogInterval: number | string;
    changesMinDelta: number;
    reconnectInterval: number | string;
    pingInterval: number | string;
    requestTimeout: number | string;
    validateSSL: boolean;
    dbversion: '1.x' | '2.x';
    usetags: boolean;
    pingserver: boolean;
    blockTime: number | string;
    debounceTime: number | string;
    disableSkippedValueLogging: boolean;
    enableLogging: boolean;
    customRetentionDuration: number | string;
    relogLastValueOnStart: boolean | 'true' | 'false';
    enableDebugLogs: boolean;
    limit: number | string;

    dockerInflux?: {
        enabled?: boolean;
        bind?: string;
        stopIfInstanceStopped?: boolean;
        port?: number | string;
        autoImageUpdate?: boolean;
    };
    dockerGrafana?: {
        enabled?: boolean;
        bind?: string;
        stopIfInstanceStopped?: boolean;
        port?: number | string;
        autoImageUpdate?: boolean;
        adminSecurityPassword?: string;
        serverRootUrl?: string;
        plugins?: string[];
        usersAllowSignUp?: boolean;
    };
}

export interface InfluxDbCustomConfig {
    debounceTime: number | string;
    blockTime: number | string;
    changesOnly: boolean | 'true' | 'false';
    changesRelogInterval: number | string;
    changesMinDelta: number | string;
    ignoreBelowNumber: number | string | null | undefined;
    ignoreAboveNumber: number | string | null;
    ignoreZero: boolean | 'true' | 'false';
    disableSkippedValueLogging: boolean | 'true' | 'false' | '';
    storageType: '' | StorageType | false;
    aliasId: string;
    round: number | string | null;
    enableDebugLogs: boolean | 'true' | 'false' | '';
    debounce: number | string;
    ignoreBelowZero: boolean | 'true' | 'false';
    /** Additional tags written with every point of this datapoint, as rows of the admin table */
    customTags?: CustomTagRow[] | null;
}

export interface InfluxDbCustomConfigTyped {
    enabled: boolean;
    debounceTime: number;
    blockTime: number;
    changesOnly: boolean;
    changesRelogInterval: number;
    changesMinDelta: number;
    ignoreBelowNumber: number | null;
    ignoreAboveNumber: number | null;
    ignoreZero: boolean;
    disableSkippedValueLogging: boolean;
    storageType: StorageType | false;
    aliasId: string;
    round: number;
    enableDebugLogs: boolean;
    debounce: number;
    ignoreBelowZero: boolean;
    /** Additional tags written with every point of this datapoint (validated, name -> value) */
    customTags: CustomTags;
}
