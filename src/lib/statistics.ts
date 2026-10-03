/**
 * Pure helpers behind the `getDpStatistics` and `cleanupOrphaned` messages.
 *
 * Kept out of `main.ts` so they can be unit tested: importing `main.ts` pulls in
 * `@iobroker/adapter-core`, which needs a js-controller installation that this repository does
 * not depend on. Shared in shape with the same file of ioBroker.sql, so the two statistics tabs
 * stay comparable - only the storage specific columns differ.
 */

import type { StorageType } from '../types';

/**
 * Why a datapoint is or is not a candidate for cleanup.
 *
 * `objectMissing` and `loggingDisabled` are deliberately kept apart. A state that no longer exists
 * in ioBroker cannot produce new values and nobody can chart it, so removing its history is safe.
 * A state that still exists but has logging switched off is a different matter: the history is
 * still reachable and may well be wanted, which is why cleanup must not treat the two as one.
 */
export type DatapointStatus = 'active' | 'loggingDisabled' | 'objectMissing';

/** What a driver reports for one measurement */
export type MeasurementStatistic = {
    /** Number of stored values in the examined time range */
    count: number;
    /** Timestamp of the oldest value in the range, or null when there is none */
    firstTs: number | null;
    /** Timestamp of the newest value in the range, or null when there is none */
    lastTs: number | null;
    /** Number of series the measurement is split into, or null when it is unknown */
    cardinality: number | null;
};

/** What a driver reports for the whole database, keyed by measurement name */
export type MeasurementStatistics = { [measurement: string]: MeasurementStatistic };

export type DatapointStat = {
    /** The ioBroker ID the measurement is named after (the Alias-ID if one is configured) */
    id: string;
    /** Storage type, or null when it cannot be determined any more */
    type: StorageType | null;
    /** Number of stored values */
    count: number;
    /** Timestamp of the oldest value, or null when there is none */
    firstTs: number | null;
    /** Timestamp of the newest value, or null when there is none */
    lastTs: number | null;
    /**
     * Number of series this measurement is split into, or null when the database did not report it.
     *
     * This is the InfluxDB counterpart of the size column of ioBroker.sql. InfluxDB has no per
     * measurement byte size at all (see `getStatistics` of the drivers), but the series count is
     * what actually matters for its memory footprint - and custom tags are the usual way to blow
     * it up by accident, so it belongs in front of the user.
     */
    cardinality: number | null;
    status: DatapointStatus;
};

/**
 * Decide how a datapoint should be classified.
 *
 * @param objectExists whether the ioBroker object for this ID still exists
 * @param loggingEnabled whether this instance currently logs the ID
 */
export function classifyDatapoint(objectExists: boolean, loggingEnabled: boolean): DatapointStatus {
    if (!objectExists) {
        return 'objectMissing';
    }
    return loggingEnabled ? 'active' : 'loggingDisabled';
}

export type StatisticsSummary = {
    datapoints: number;
    /** Number of stored values over all datapoints */
    values: number;
    /** null when the cardinality of at least one datapoint is unknown, so the sum would mislead */
    cardinality: number | null;
    byStatus: Record<DatapointStatus, { datapoints: number; values: number }>;
};

/**
 * Totals for the statistics table.
 *
 * `cardinality` is null as soon as one datapoint has none: a partial sum presented as a total
 * would understate the real footprint.
 *
 * @param stats the per-datapoint statistics
 */
export function summarize(stats: DatapointStat[]): StatisticsSummary {
    const byStatus: Record<DatapointStatus, { datapoints: number; values: number }> = {
        active: { datapoints: 0, values: 0 },
        loggingDisabled: { datapoints: 0, values: 0 },
        objectMissing: { datapoints: 0, values: 0 },
    };

    let values = 0;
    let cardinality = 0;
    let cardinalityKnown = true;

    for (const stat of stats) {
        values += stat.count;
        byStatus[stat.status].datapoints++;
        byStatus[stat.status].values += stat.count;

        if (stat.cardinality === null) {
            cardinalityKnown = false;
        } else {
            cardinality += stat.cardinality;
        }
    }

    return {
        datapoints: stats.length,
        values,
        cardinality: cardinalityKnown ? cardinality : null,
        byStatus,
    };
}

/** Which statuses a cleanup run should remove */
export type CleanupScope = {
    /** States that no longer exist in ioBroker. Safe, and the default. */
    objectMissing?: boolean;
    /** States that still exist but are not logged. Their history may still be wanted. */
    loggingDisabled?: boolean;
};

/**
 * Pick the datapoints a cleanup run with this scope would remove.
 *
 * An empty scope selects `objectMissing` only. Defaulting to the safe half means a caller that
 * forgets to pass a scope deletes the data nobody can reach any more, not data someone may be
 * keeping on purpose. `active` datapoints are never selectable.
 *
 * @param stats the per-datapoint statistics
 * @param scope which statuses to include
 */
export function selectForCleanup(stats: DatapointStat[], scope?: CleanupScope): DatapointStat[] {
    const includeMissing = scope?.objectMissing !== false;
    const includeDisabled = scope?.loggingDisabled === true;

    return stats.filter(
        stat =>
            (stat.status === 'objectMissing' && includeMissing) ||
            (stat.status === 'loggingDisabled' && includeDisabled),
    );
}

/**
 * Count the series of a measurement from the series keys InfluxDB 1.x reports.
 *
 * `SHOW SERIES` answers with one key per series, in the line protocol shape
 * `measurement,tag=value,tag=value`. The measurement itself may contain escaped commas, so the
 * key is split at the first comma that is not escaped - splitting naively would attribute the
 * series of `a\,b` to the measurement `a\`.
 *
 * @param seriesKeys the `key` column of `SHOW SERIES`
 */
export function cardinalityFromSeriesKeys(seriesKeys: string[]): { [measurement: string]: number } {
    const result: { [measurement: string]: number } = {};

    for (const key of seriesKeys) {
        if (!key) {
            continue;
        }
        let measurement = key;
        for (let i = 0; i < key.length; i++) {
            if (key[i] === '\\') {
                i++; // the next character is escaped, so it can not end the measurement
            } else if (key[i] === ',') {
                measurement = key.substring(0, i);
                break;
            }
        }
        measurement = measurement.replace(/\\(.)/g, '$1');
        result[measurement] = (result[measurement] || 0) + 1;
    }

    return result;
}
