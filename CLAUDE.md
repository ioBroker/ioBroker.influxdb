# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

ioBroker adapter (`iobroker.influxdb`, type `storage`) that logs ioBroker states into InfluxDB 1.x or 2.x and serves them back via the ioBroker `getHistory` message API. Source is TypeScript in `src/`, compiled to `build/` (`main` = `build/main.js`); `build/` is gitignored and must exist before the adapter or the tests can run.

## Commands

```bash
npm run build          # tsc -p tsconfig.build.json -> build/
npx tsc -p tsconfig.json   # type check only (noEmit; no npm script exists for this)
npx eslint .           # lint (no npm script; config in eslint.config.mjs, ignores test/**/*.js and build/)
npm test               # mocha --exit  -> runs test/*.js (non-recursive; test/lib/* are helpers)
npx mocha test/testAdapter.js --exit   # single suite
npm run translate      # @iobroker/adapter-dev translate-adapter for admin/i18n/*
npm run release-patch  # @alcalzone/release-script (runs npm run build before commit)
```

### Running the tests

`test/testPackageFiles.js` is the only offline suite. The four `testAdapter*.js` suites are full integration tests: `test/lib/setup.js` installs js-controller into `tmp/` from npm (first run takes minutes, timeouts are 600 s), starts a real adapter instance, and talks to a **real InfluxDB server** — they fail without one.

Which DB the suites target is decided by env vars:

| Env | Effect |
| --- | --- |
| _(none)_ | InfluxDB 1.x on `localhost:8086` |
| `INFLUX_DB1_HOST` | InfluxDB 1.x on that host |
| `INFLUXDB2=true` | InfluxDB 2.x; plus `INFLUXDB2_TOKEN` (default `test-token`), `INFLUXDB2_ORG` (default `test-org`), optional `AUTHTOKEN` (JSON from `influx auth create`) |

The four suites differ only in instance config: `testAdapter.js` (default), `testAdapterBuffer.js` (`seriesBufferMax = 5`), `testAdapterTags.js` (`usetags = true`), `testAdapterExisting.js` (pre-existing DB). They share the assertion body in `test/lib/testcases.js` — fix a test expectation there, not four times. `test/lib/testCompare.ts` is stale (imports a removed `src/lib/dockerManager.types`) and is not executed.

CI (`.github/workflows/test-and-release.yml`) runs package-file checks, then the suites against InfluxDB 1.8 and 2.0 on Node 20/22/24; lint and type-check steps are currently commented out there.

## Architecture

### Layers

- `src/main.ts` (~3.5k lines) — the whole adapter: `class InfluxDBAdapter extends Adapter`. Exports a factory when `require`d (ioBroker compact mode), self-starts otherwise.
- `src/lib/Database.ts` — abstract client contract (`connect`, `writeSeries`/`writePoints`/`writePoint`, `query`, retention, `getMetaDataStorageType`, …).
- `src/lib/DatabaseInfluxDB1x.ts` — InfluxQL over the `influx` package (user/password auth).
- `src/lib/DatabaseInfluxDB2x.ts` — Flux over `@influxdata/influxdb-client` (+ `-apis`), token/org auth, buckets instead of databases.
- `src/lib/aggregate.ts` — pure client-side aggregation/beautify/response code shared in shape with the ioBroker `history` and `sql` adapters. Keep its exported signatures aligned with those adapters.

`connect()` picks the implementation from `config.dbversion`; almost every version-specific branch elsewhere keys off the same field.

### Write path

`stateChange` → alias remap via `_aliasMap` → `pushHistory()` → `pushHelper()` → `pushValueIntoDB()` → buffer or direct write.

`pushHistory()` holds all per-datapoint filtering, driven by the custom config normalized in `normalizeStateConfig()`: `debounceTime` (delay for a stable value), `blockTime` (dead time after a write), `changesOnly` + `changesMinDelta` + `changesRelogInterval` (re-log timer per id), `ignoreZero`/`ignoreBelowNumber`/`ignoreAboveNumber`, `storageType` coercion, and the "skipped value" mechanism that re-inserts the last suppressed value so charts stay correct (`disableSkippedValueLogging` turns it off).

Buffering: points land in `_seriesBuffer[measurementId][]` and are flushed when `seriesBufferMax` is exceeded or every `seriesBufferFlushInterval` seconds. `seriesBufferMax = 0` means direct writes. On shutdown `writeFileBufferToDisk()` persists the buffer plus `_conflictingPoints` to `<iobroker-data>/influxdata.json` (`influxdata_<instance>.json` for instance ≠ 0) and `main()` re-reads and deletes it on the next start — changing that file format breaks upgrade continuity.

Failure handling is a deliberate escalation ladder, each level narrowing down the offending point: `writeAllSeriesAtOnce` → `writeAllSeriesPerID` → `writePointsForID` → `writeOnePointForID`. Host-unavailable/timeout errors push everything back into the buffer and trigger `reconnect()`. A `field type conflict` makes `writeOnePointForID` guess the pinned type (bool↔float↔string), retry once, write the detected `storageType` back into the object's custom config via `extendForeignObject`, and mark the id in `_conflictingPoints` so it is written directly (unbuffered) from then on. Writes are batched at 15 000 points because InfluxDB rejects bodies over ~2 MB.

Subscription strategy: under 20 configured datapoints the adapter subscribes to each `realId`; at 20+ it flips `_subscribeAll` and subscribes to `*` — new-object handling in the `objectChange` callback must keep both branches consistent.

### Read path

`getHistory` dispatches to `getHistoryV1` (InfluxQL: `SELECT mean(value)… GROUP BY time(<step>ms) fill(previous)`) or `getHistoryV2` (Flux: `range |> filter |> window |> mean/max/quantile/integral…`). Both try to push aggregation into the DB and set `options.preAggregated = true`; when the aggregate is unsupported there (`none`, `onchange`, `minmax`, linear `integral`, non-numeric measurements) they clear the flag and let `src/lib/aggregate.ts` do it in Node. Both also fetch one extra point before/after the range for charting unless `removeBorderValues` is set.

### Message API (`processMessage`)

This is the adapter's public surface, consumed by javascript/admin/flot/history clients: `getHistory`, `storeState`, `update`, `delete` / `deleteRange` / `deleteAll`, `query` (1.x InfluxQL) vs `multiQuery` (2.x Flux, statements separated by `;`), `test`, `destroy`, `getRetention`, `flushBuffer`, `enableHistory` / `disableHistory` / `getEnabledDPs`, `getConflictingPoints` / `resetConflictingPoints`, `features`, `stopInstance`. Adding a command means updating both `processMessage` and the `features` reply.

### Metadata storage: tags vs fields (2.x)

`usetags` decides whether `q`/`ack`/`from` are written as Influx tags or fields. A bucket can only hold one scheme, so `checkMetaDataStorageType()` inspects the bucket on connect and **stops the adapter** on mismatch. The flag also changes query shape (`pivot(...)` for fields vs `duplicate(column: "_value", as: "value")` for tags) — touch both places together.

### Docker mode

`io-package.json` declares the `@iobroker/plugin-docker` plugin with `docker-compose.yaml` (the `iob*` labels are plugin directives, and `${config.x.y:-default}` placeholders are resolved from instance native config). When `dockerInflux.enabled`, `main()` forces `dbversion = '2.x'`, `protocol = http`, org `iobroker` and a hardcoded default token; `prepareDockerConfigGrafana()` generates `build/grafana-provisioning/datasources/datasource.yml` before signalling `instanceIsReady()`. Because that file is written relative to `__dirname`, it only exists after a build.

## Conventions

- Admin config is JSON-driven: `admin/jsonConfig.json` (instance tabs) and `admin/jsonCustom.json` (per-state settings). Any new option must be added in four places: the JSON config, `io-package.json` `native` (or the custom defaults), the `InfluxDBAdapterConfig` / `InfluxDbCustomConfig` types in `src/types.d.ts`, and the normalization in `main()` / `normalizeStateConfig()` — admin delivers numbers and booleans as strings, which is why `parseBool`/`parseNumber` exist.
- Translations in `admin/i18n/*.json` are maintained through Weblate; regenerate with `npm run translate` rather than hand-editing every language.
- `io-package.json` `common.version` must track `package.json` `version`; the release script keeps them in sync and prepends the changelog under `### **WORK IN PROGRESS**` in `README.md`.
- InfluxDB rejects `null` and non-finite values, and fixes a measurement's data type on first write — new write code must keep the existing guards in `pushValueIntoDB`/`pushHelper`.
