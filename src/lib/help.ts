/**
 * The answer of the message `help`: what the instance answers, written for an AI agent (e.g. ioBroker.pi-agent, or
 * the MCP server with `send_to`) and for script authors. Keep it in sync with `processMessage` in main.ts.
 */

/**
 * Every command `processMessage` answers; `write` = it changes data, settings or the instance; `standard` = it follows
 * the shared catalog of sendTo commands (HELP.md of ioBroker.assistant), the same in history, sql, influxdb and recorder
 */
export const COMMANDS: { command: string; write: boolean; standard?: string }[] = [
    { command: 'help', write: false },
    { command: 'getHistory', write: false, standard: 'history.getHistory' },
    { command: 'aggregate', write: false, standard: 'history.aggregate' },
    { command: 'lastChange', write: false, standard: 'history.lastChange' },
    { command: 'getEnabledDPs', write: false },
    { command: 'getDatapoints', write: false },
    { command: 'getRawEntries', write: false },
    { command: 'getDpStatistics', write: false },
    { command: 'getRetention', write: false },
    { command: 'getConflictingPoints', write: false },
    { command: 'features', write: false },
    { command: 'flushBuffer', write: false },
    { command: 'test', write: false },
    { command: 'query', write: true },
    { command: 'enableHistory', write: true },
    { command: 'disableHistory', write: true },
    { command: 'storeState', write: true },
    { command: 'update', write: true },
    { command: 'delete', write: true },
    { command: 'deleteRange', write: true },
    { command: 'deleteAll', write: true },
    { command: 'cleanupOrphaned', write: true },
    { command: 'resetConflictingPoints', write: true },
    { command: 'destroy', write: true },
    { command: 'stopInstance', write: true },
];

export const HELP = `ioBroker.influxdb: stores the history of the states that are enabled for it (per state in the object settings)
in InfluxDB 1.x, 2.x or 3.x (native.dbversion of system.adapter.influxdb.0). An agent can read the course of a
state (the stored values or one value per interval), list the logged states and the stored data, run a reading
query and, only on the user's request, enable or disable the logging and store, change or delete values.

Times (start, end, ts): milliseconds since 1970 or a date text such as "2026-11-19T08:00" (local time of the host).
Times in the answers are milliseconds since 1970.

Layout (for a query):
- Database (1.x, 3.x) or bucket (2.x, organization native.organization): native.dbname, default "iobroker".
- One measurement per state, named after its ID (or its setting aliasId). Field "value": float, boolean or string
  (objects as JSON); the first point fixes the type (setting storageType of the state). q, ack and from are
  fields; on 2.x and 3.x with native.usetags they are tags (strings). The custom tags of a state are tags.
- InfluxQL (1.x and 3.x): SELECT value FROM "hm-rpc.0.ABC.1.TEMPERATURE" WHERE time > now() - 1h; the ID in
  double quotes, times as now() - 1d or '2026-11-19T08:00:00Z'.
- Flux (2.x): from(bucket: "iobroker") |> range(start: -1h) |> filter(fn: (r) => r._measurement ==
  "hm-rpc.0.ABC.1.TEMPERATURE" and r._field == "value"); the value is in _value.

Commands (sendTo influxdb.0): getHistory, aggregate and lastChange are standard commands of the shared catalog of
ioBroker (family history): the same names, parameters and answers in history, sql, influxdb and recorder.
- getHistory {id, options: {start?, end?, aggregate?, count?, step?, ...}}: the course of one state:
  {result: [{ts, val, ack?, from?, q?, id?}], step, sessionId}, on an error {result: [], error}. options is
  required (may be {}). id: one state; "*" (all states) is refused with an error.
  aggregate: average (default), min, max, minmax (min and max of each interval), total, count, percentile
  {percentile 0-100, default 50}, quantile {quantile 0-1, default 0.5}, integral {integralUnit in s, default 60,
  3600 gives Wh from W; integralInterpolation none (default) | linear}: one value per interval, mostly computed
  by InfluxDB; none or onchange: the stored values (with their ack, q and from).
  start: without it the newest count values; with an aggregate and without step, the adapter builds the
  intervals from them (as sql and history do).
  end: default now + 5000 s. count: the number of intervals (default 500); with none/onchange the number of
  values (default limit on 1.x and 3.x, 500 on 2.x). step: the interval in ms instead of count.
  intervalBorders: [ms, ...] ascending borders of the intervals (e.g. local days), replaces start, end and step.
  limit: at most this many values or intervals (default count, else the setting limit, else 2000).
  returnNewestEntries: true = the newest values instead of the oldest when count cuts. addId: true adds the id.
  round: digits after the decimal point (default the setting of the instance). removeBorderValues: true drops
  the values before start and after end that are added for charts. sessionId: returned as it is. ignoreNull,
  ack, from and q are not read: a null is replaced by the value before.
- aggregate {id, start, end?, fn, value?}: one number for a period: {value, ts?, unit?, count}. fn: min, max (ts =
  when), avg (the mean of the stored values), sum, count (with value: how often the state became it, e.g. true),
  delta (how much a counter rose, resets included), first, last, integral (value × hours: W gives Wh), duration
  (with value: how many seconds the state had it). end: default now. count: the stored values in the period, at
  most 50000 (else reason TOO_MANY_VALUES). Errors: {error, reason} (reason BAD_REQUEST for a wrong parameter).
  Computed from the values of getHistory (aggregate none).
- lastChange {id, value?, before?}: when the state last changed (to value): {ts, val}; before: only the values
  before this time. A value logged again does not count as a change. Reason NOT_FOUND if it never had the value;
  only the newest 10000 stored values are searched: limited true = it had it already then.
- getEnabledDPs {}: the states this instance logs: {[id]: their settings (enabled, changesOnly, debounceTime,
  blockTime, changesMinDelta, changesRelogInterval, ignoreZero, ignoreBelowNumber, ignoreAboveNumber, round,
  storageType, aliasId, customTags, ...)}.
- getDatapoints {}: every measurement of the database, also the ones no longer logged: {success, result: [{id,
  type: Number | String | Boolean | null}]}.
- getRawEntries {id, start?, end?, limit? (1-5000, default 100), offset? (default 0), sort? desc (default) | asc,
  noTotal?}: the points exactly as stored: {id, total, limit, offset, sort, result: [{ts, val, ack, q, from}]};
  noTotal: true skips the counting (total is missing then).
- getDpStatistics {start?, end?}: per measurement the number of values (a full scan, slow on a big database):
  {success, result: [{id, type, count, firstTs, lastTs, cardinality (series), status}], summary: {datapoints,
  values, cardinality, byStatus}}. status: active, loggingDisabled (the state exists, not logged), objectMissing.
- getRetention {}: how long the database keeps values: {result: {name, time (s, 0 = forever)}} or {error}.
- getConflictingPoints {}: the states whose values InfluxDB refused for another type; they are written directly
  since: {conflictingPoints: {[id]: 1}}.
- features {}: {supportedFeatures: [update, delete, deleteRange, deleteAll, storeState, getDatapoints,
  getRawEntries, getDpStatistics, cleanupOrphaned]}; 3.x without delete and deleteRange (and update with usetags).
- flushBuffer {id?}: writes the buffered values of the state (of all without id) into the database now, as
  getHistory does itself: {error: null} or {error}.
- query "statement" (the message is the string itself): runs it as it is, InfluxQL on 1.x and 3.x, Flux on 2.x;
  several statements separated by ";": {result: [[rows of the 1st statement], ...], ts} or {result: [], error}.
  time becomes ts (ms); InfluxQL rows keep the field as "value". It runs ANY statement (also DELETE, DROP or a
  Flux to()); use it only for reading.
- enableHistory {id, options?}: logs the state from now on; options = its settings as in getEnabledDPs (the
  defaults of the instance otherwise): {success} or {error}.
- disableHistory {id}: stops logging the state, the stored values stay: {success} or {error}.
- storeState {id, state: {ts, val, ack?, q?, from?} or a list of them, rules?} or [{id, state, rules?}]: writes
  values (through the buffer), also of a state that is not logged; ts is required, null is refused, a number as
  text becomes a number; rules: true applies the logging settings (only for a logged state): {success,
  successCount} or {error, errors, successCount}.
- update {id, state: {ts, val?, ack?, q?, from?}} or [{id, state}] or {id, state: [...]}: changes the stored point
  with exactly this ts (not on 3.x with usetags): {success} or {success: false, error}.
- delete {id, ts} or {id, ts: [ts, ...]} or [{id, ts}]: deletes the points with exactly these ts (not on 3.x):
  {success} or {success: false, error}.
- deleteRange {id, start, end?} or [{id, start, end?}] or {id, state: [{start, end?}]}: deletes the points with
  start <= ts <= end (end default now; not on 3.x): {success} or {success: false, error}.
  ts, start and end: ms or a date string, 0 is valid. If one entry has no valid ts or start, nothing is deleted
  and error says what is missing.
- deleteAll {id} or [{id}]: deletes every value of these states (3.x: drops the measurement): {success} or
  {success: false, error}.
- cleanupOrphaned {scope?: {objectMissing? (default true), loggingDisabled? (default false), ids?}, confirm?}: the
  measurements of states that are not logged any more. Without confirm: true only the preview {dryRun: true,
  datapoints, values, items}; with confirm: true it drops them and adds {deleted, failed}.
- resetConflictingPoints {}: empties the list of getConflictingPoints: {reset: true, conflictingPoints}.
- destroy {noRestart?}: drops the whole database (bucket), then restarts the instance (noRestart: true creates it
  empty again instead): {error: null} or {error}. Every stored value is lost.
- test {config}, stopInstance: for the settings and js-controller, not for agents.

How to talk to the user:
- Find the state first (search the objects); name states by their name, give times in local time and values
  with their unit.
- A state has a history only if it is or was logged (getEnabledDPs, getDatapoints). For a longer period request
  one value per interval (e.g. aggregate average with start and count 24 to 200) instead of every stored value.
  For one number of a period (the highest value, how often, how long, how much) use the command aggregate, for
  "when was it last ..." lastChange. Prefer these and getHistory; write a query only for what they cannot answer,
  and read dbversion and dbname first.
- Send only reading statements with query (InfluxQL SELECT or SHOW, Flux without to() or another output). Any
  other statement only on the user's explicit request.
- query (other than reading), delete, deleteRange, deleteAll, update, storeState, cleanupOrphaned, destroy,
  resetConflictingPoints, enableHistory, disableHistory and stopInstance change the database, the settings or
  the instance: never without the user's explicit request. Deleted values cannot be restored: name the state
  and the range and ask for confirmation first; show the preview of cleanupOrphaned (without confirm) before
  deleting; for destroy say that the whole history is lost.`;
