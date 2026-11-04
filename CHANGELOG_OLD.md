# Older changes
## 5.1.6 (2026-10-13)
* (@GermanBluefox) The "Statistics" tab works now with InfluxDB 2.0

## 5.1.5 (2026-10-13)
* (@GermanBluefox) Added missing translations. Compact mode no longer can exit the whole host process. The Grafana admin password is protected now

## 5.1.4 (2026-10-13)
* (@GermanBluefox) The "Statistics" tab works now also with InfluxDB 2.0. A datapoint without storage type that received a numeric string is shown there as number

## 5.1.3 (2026-10-13)
* (@GermanBluefox) The error of a failed save or delete in the data browser is no longer hidden by the reload of the table

## 5.1.2 (2026-10-13)
* (@franzenhof) A value received during `blockTime` is now logged when the block time is over. Before, it was dropped and a datapoint that then stayed unchanged kept its outdated value in the database (repeated by `changesRelogInterval`) until the next change
* (@GermanBluefox) A value excluded by `ignoreZero`/`ignoreBelowNumber`/`ignoreAboveNumber` no longer replaces the value waiting for the end of `blockTime`, and is not written on unload. A waiting value is also written when the datapoint is reconfigured instead of being lost. Note: as the waiting value keeps its original timestamp, two points in the database can now be less than `blockTime` apart

## 5.1.1 (2026-10-13)
* (@GermanBluefox) Grafana in Docker is shown as link on the intro page and in the instance list of admin. The instance restarts once when the link is added or removed
* (@GermanBluefox) The settings show the Grafana login (`admin` / admin security password) and link to the documentation
* (@GermanBluefox) Described how to use InfluxDB and Grafana in Docker: login, data source, first dashboard, resetting a forgotten password

## 5.1.0 (2026-10-03)
* (@jb-io) Added custom tags per datapoint, written with every value to InfluxDB 1.x and 2.x (#32)
* (@GermanBluefox) `enableHistory` also accepts the custom tags in the form `getEnabledDPs` reports them, so a configuration read from there and written back keeps its tags
* (@GermanBluefox) Fixed the error of a failed message (`getRetention`, `test`, `destroy`, `getDatapoints`, `getRawEntries`, ...) being sent as an empty object instead of its text
* (@GermanBluefox) `io-package.json` matches the current js-controller schema again (`subscribe` removed, `docs.en` added)
* (@GermanBluefox) The admin frontend is installed before it is linted in the CI, so its lint no longer fails on unresolved types
* (@GermanBluefox) Added a **Statistics** tab: values, oldest/newest value, series and status per datapoint, plus a cleanup for the data of states that were deleted in ioBroker (`getDpStatistics` and `cleanupOrphaned`)

**Note:** a buffer written by this version (`influxdata.json`, only present if the adapter was stopped with unwritten values) can not be read by version 5.0.5 and older if custom tags are used. Downgrading discards those buffered values.

## 5.0.5 (2026-10-01)
* (@GermanBluefox) Corrected boolean aggregation

## 5.0.4 (2026-08-28)
* (@GermanBluefox) Fixed Grafana not finding the InfluxDB running next to it in Docker: the provisioned data source pointed at `iob_influxdb_<instance>`, while the container was named `iob_influxdb_<instance>_flux_data` because the compose file gave it a name of its own. Inside the shared network only the container name resolves, so the data source could not connect. The influx service uses the default name of the instance now - the name the data source and `testConnection()` both expect
* (@GermanBluefox) Fixed the port of that data source: it used the port published on the host, although Grafana reaches InfluxDB inside the docker network, where the container port 8086 applies. The data source broke as soon as the port was changed in the settings
* (@GermanBluefox) Fixed the Grafana container never being started when Grafana is enabled and InfluxDB is not: the plugin waits for the readiness signal of the adapter before it starts any container of the instance, and that signal was only sent when both were switched on. That state is reachable by switching InfluxDB off afterwards - the Grafana checkbox is hidden then, but its stored value stays
* (@GermanBluefox) The "automatic image update" setting of Grafana had no effect: the compose file never passed it on to the plugin
* (@GermanBluefox) **Existing installations with InfluxDB in Docker have to remove the old container once**, because it is renamed by the first fix: `docker rm -f iob_influxdb_<instance>_flux_data`. It still holds the published port, so the correctly named container cannot start next to it. The data is not affected - it lives in the volumes, which keep their names

## 5.0.3 (2026-08-27)
* (@GermanBluefox) Errors are logged with more detail: error code, `cause` and a driver-specific error name (`HttpError`, `ServiceNotAvailableError`) are shown now, and a nested error without a message no longer degrades to `{}` (same implementation as in the SQL adapter)
* (@GermanBluefox) A switched-off or unreachable InfluxDB no longer floods the log (and syslog): a connection error is now recognized by its error code - Node reports a failed TCP connect as an `AggregateError` with an empty message, which no check could match before - so the points are buffered and a reconnect is scheduled instead of retrying every single point. The repeated error is logged once and afterwards only once an hour
* (@GermanBluefox) `getHostsAvailable()` reports the real state again: it returned a hardcoded `1` since the TypeScript port, so every "host not available" check in the adapter was dead code. After a connection error the host is now taken out of rotation for a short backoff and values are buffered instead of being written - and logged - point by point
* (@GermanBluefox) A failing buffer flush inside the interval timer no longer produces an unhandled promise rejection, which terminates the adapter process on current Node.js versions
* (@GermanBluefox) `storeState` answers with an error again if a value cannot be stored (`null`, `NaN`, or a non-numeric value for a `Number` datapoint) instead of reporting `success: true` and silently discarding it; on the state-change path such a value is still logged only once per datapoint
* (@GermanBluefox) The warning about an `undefined` state value is logged only once per datapoint as well

## 5.0.2 (2026-08-26)
* (@GermanBluefox) Added the data browser to the configuration, so the stored values can be viewed, edited and deleted.
* (@GermanBluefox) The aggregation is used now from `@iobroker/aggregate` and is shared with the history and SQL adapters.
* (@joltcoke) Fixed average and total returning null for every interval that contains a null value: parseFloat(null) is NaN and poisoned the sum of the whole interval (thanks to @joltcoke, ioBroker/ioBroker.sql#526). As the result was NaN and not null, ignoreNull could not act on it either
* (@joltcoke) Fixed min returning a wrong value if the interval contains a null, minmax losing the minimum if the interval starts with a null, and percentile/quantile counting a null as 0

## 5.0.1 (2026-08-15)
* (@GermanBluefox) Completely refactored the code to TypeScript and ES6
* (@GermanBluefox) Added possibility to start docker containers directly from the adapter
* (mcm1957) Adapter requires admin >= 7.7.2 now
* (arteck) Fixed the connection handling for InfluxDB 1.x: the health check (ping) and the automatic reconnect were never started
* (arteck) Fixed the loss of buffered values if the writing was running while new values arrived or if the write failed
* (arteck) Values are no longer written twice if they are written directly (buffer size 0 or conflicting points)
* (arteck) State IDs and database names are now escaped in the queries
* (arteck) The password/token is no longer written into the log by the connection test
* (arteck) The settings "request timeout" and "validate SSL" are now used for InfluxDB 1.x too
* (arteck) Fixed the cache file name if more than one instance runs in the compact mode
* (arteck) Fixed the aggregation for `percentile: 100`/`quantile: 1` and the last value of `integralTotal`
* (bluefox) Fixed empty charts for the aggregation `onchange` ("raw" in e-charts): it was run through the interval aggregation and returned only `null` values
* (@GermanBluefox) Minimal node.js version is 22

## 4.0.3 (2024-05-16)
* (bluefox) Some packages were updated

## 4.0.2 (2024-01-03)
* (bluefox) Corrected JSON config
* (Marc-Berg) Corrected filter function in booleanTypeCheckQuery
* (Marc-Berg) Allowed the self-signed certificates using "test connection" button
* (Marc-Berg) The deletion of data from DB was implemented for V2
* (bluefox) Added support for `count` aggregate type on getHistory
* (bluefox) minimal supported node.js version is 16 now

## 3.2.0 (2022-09-19)
* (Apollon77) Adjust cache file to be different per instance when having multiple instances

## 3.1.8 (2022-08-13)
* (Apollon77) Fix crash cases reported by Sentry

## 3.1.7 (2022-06-27)
* (Apollon77) Allows removing a configuration value for "round" in config again

## 3.1.6 (2022-06-27)
* (Apollon77) When not count is provided for aggregate "none" or "onchange" then the limit (default 2000) is used as count to define the number of data to return.

## 3.1.5 (2022-06-12)
* (Apollon77) Make sure the debug log is active, according to the settings

## 3.1.4 (2022-06-08)
* (Apollon77) Performance optimizations for GetHistory calls, especially for "minmax" aggregate method
* (Apollon77) Prevent the crash case reported by Sentry

## 3.1.3 (2022-06-01)
* (Apollon77) Corrected issue when testing connection to InfluxDBv1 with weird passwords

## 3.1.2 (2022-05-31)
* (Apollon77) Workaround an admin issue when testing connection to InfluxDB

## 3.1.0 (2022-05-27)
* (Apollon77) Data are not converted to number if they are other data types on `getHistory` to respect the saved data formats as defined in the datapoint settings for storage.
* (Apollon77) Fix retention change to lower checkbox in UI
* (Apollon77) Allow storeState again to write to InfluxDB for "unknown state ids" - "rules" usage is not supported in for this and storeState would be silently discarded in this case!
* (Apollon77) Fix several crash cases reported by Sentry
* (Apollon77) Make sure disabling "Log changes only" also really do not log the changes anymore
* (Apollon77) Allow storeState and GetHistory also to be called for "unknown ids"

## 3.0.2 (2022-05-12)
* (Apollon77) handles an empty Path for InfluxDB 2.0 correctly in all cases

## 3.0.1 (2022-05-11)
* (Apollon77) BREAKING: Configuration is only working in the new Admin 5 UI!
* (Apollon77) Did bigger adjustments to the recording logic and added a lot of new Features. Please refer to Changelog and Forum post for details.

## 3.0.0 (2022-05-11)
* (Apollon77) Breaking: Configuration is only working in the new Admin 5 UI!
* (Apollon77) Breaking! Did bigger adjustments to the recording logic. Debounce is refined and blockTime is added to differentiate between the two checks
* (Apollon77) Breaking! GetHistory requests now need to deliver the ts in milliseconds! Make sure to use up to date scripts and Charting UIs
* (Apollon77) New setting added to disable the "logging of additional values for charting optimization" - then only the expected data are logged
* (Apollon77) Added new Debug flag to enable/disable debug logging on datapoint level (default is false) to optimize performance
* (Apollon77) Added flag returnNewestEntries for GetHistory to determine which records to return when more entries as "count" are existing for aggregate "none"
* (Apollon77) Flush the buffer for the datapoint which is queried by GetHistory before the query is done, so that all data are in the database
* (Apollon77) make sure id is always returned on GetHistory when addId is set
* (Apollon77) Added aggregate method "percentile" to calculate the percentile (0..100) of the values (requires options.percentile with the percentile level, defaults to 50 if not provided). Basically same as Quantile just different levels are used. Calculation will be done by InfluxDB
* (Apollon77) Added aggregate method "quantile" to calculate the quantile (0..1) of the values (requires options.quantile with the quantile level, defaults to 0.5 if not provided). Basically same as Percentile just different levels are used. Calculation will be done by InfluxDB
* (Apollon77) Added (experimental) method "integral" to calculate the integral of the values. Requires options.integralUnit with the time duration of the integral in seconds, defaults to 60s if not provided. Optionally a linear interpolation can be done by setting options.integralInterpolation to "linear". Calculation will be done byInfluxDB (except for InfluxDB 1 and interpolation "linear", this is done by adapter on raw data)
* (Apollon77) When request contains flag removeBorderValues: true, the result then cut the additional pre and post border values out of the results
* (Apollon77) Enhance the former "Ignore below 0" feature and now allow specifying to ignore below or above specified values. The old setting is converted to the new one
* (Apollon77) Allow to specify custom retention duration in days
* (Apollon77) Added a new message flushBuffer to flush complete buffers or for a given id
* (Apollon77) Allow to specify an additional path for the InfluxDB URL, when Reverse proxies are used or such
* (Apollon77) Make sure that min change delta allows numbers entered with comma (german notation) in all cases
* (Apollon77) Added support to specify how to round numbers on query per datapoint
* (Apollon77) Allow to specify if the last value of a state is logged on startup

## 2.6.3 (2022-03-07)
* (Apollon77) Fix potential crash cases (Sentry IOBROKER-INFLUXDB-4Q)

## 2.6.2 (2022-03-03)
* (Apollon77) Correctly handle db names with hyphens for InfluxDB 1
* (Apollon77) Fix getHistory for non-boolean fields
* (Apollon77) Adjust shard group duration to influxdb proposals

## 2.6.1 (2022-02-28)
* (Excodibur) Address issue with updating retention policy for influxDB 1

## 2.6.0 (2022-02-24)
* (Excodibur) Make request timeout configurable - defaults to 30s
* (Apollon77) Added an option to also accept self-signed ssl certificates for InfluxDB 2.x

## 2.5.2 (2022-02-22)
* (bluefox) Marked interpolated data with `i=true`

## 2.5.0 (2022-02-14)
* (bluefox) Added new messages: 'update', 'delete', 'deleteRange', 'deleteAll' (only for influxdb v1)
* (bluefox) Corrected aggregation of data

## 2.4.0 (2021-12-19)
* (Excodibur) Added expert settings to deactivate health checks for Influx 2 db (There are no health checks for Influx 1)
* (Excodibur) Hide settings not relevant when "log changes only" is not used
* (Apollon77) Allow all number values for debounce again
* (Apollon77) InfluxDB also does not like Infinite values, so ignore them too

## 2.3.0 (2021-12-14)
* (bluefox) Support only `js.controller` >= 3.3.x
* (bluefox) Used system/custom view for collecting the objects
* (bluefox) Implemented option to ignore zero- or/and below zero- values

## 2.2.0 (2021-08-25)
* (Excodibur) Added an option to store metadata (q, ack, from) as tags instead of fields for Influx 2.x - see README!
* (Excodibur) Failure to update/set retention policy will now cause warning instead of error/restart, to support more restrictive DB setups
* (Excodibur/Apollon77) Bug fixes and adjustments

## 2.1.1 (2021-08-13)
* IMPORTANT: You need to re-enter the password after installing this version!
* IMPORTANT: The adapter now requires Admin 5.1.15+ and js-controller 3.3+! For other admin or js-controller versions please use the former v1.9.5 of thi adapter.
* (Excodibur) Added InfluxDB 2.0 support
* (Excodibur) Adjust Retention handling on Database level to work for InfluxDB 1.x and 2.x
* (Excodibur) Removed retention options on datapoint level because never worked and also not supported really by InfluxDB anymore

## 1.9.5 (2021-04-19)
* (bluefox) Added the support of Admin5

## 1.9.4 (2021-01-17)
* (Apollon77) Optimize stop handling

## 1.9.3 (2020-11-07)
* (Apollon77) Crash case prevented (Sentry IOBROKER-INFLUXDB-T, IOBROKER-INFLUXDB-Y)

## 1.9.2 (2020-08-06)
* (Apollon77) crash prevented (Sentry IOBROKER-INFLUXDB-G)

## 1.9.1 (2020-07-22)
* (Apollon77) crash prevented (Sentry IOBROKER-INFLUXDB-E)

## 1.9.0 (2020-07-21)
* (Apollon77) Optimize handling in case of write errors and host unavailabilities

## 1.8.8 (2020-07-18)
* (Apollon77) Some errors prevented
* (Apollon77) Set timeouts for influxdb connections to make sure connectioens do not run forever

## 1.8.7 (2020-05-14)
* (bluefox) added command to read supported features

## 1.8.6 (2020-05-11)
* (Apollon77) make sure disabling of data points while starting adapter do not crash adapter (Sentry IOBROKER-INFLUXDB-7)
* (Apollon77) Make sure all start values are processed correctly
* (Apollon77) More checks to make sure to not crash when states are disabled while data are processed (Sentry IOBROKER-INFLUXDB-8)

## 1.8.5 (2020-05-08)
* (bluefox) set default history if not yet set

## 1.8.4 (2020-05-02)
* (Apollon77) make sure disabling of data points do not crash adapter (Sentry IOBROKER-INFLUXDB-4)

## 1.8.3 (2020-04-29)
* (Apollon77) Fix pot crash case when deleting objects while saving values (Sentry)

## 1.8.2 (2020-04-19)
* __Requires js-controller >= 2.0.0__
* (Apollon77) removed usage of `adapter.objects`
* (Apollon77) check if objects have changed and ignore unchanged
* (Apollon77) Added Sentry for Error Reporting with js-controller 3.0
* (Apollon77) Make sure value undefined is ignored

## 1.4.2 (2017-03-02)
* (Apollon77) Added option to define storage datatype per datapoint inclusing converting the value if needed

## 1.3.4 (2017-02-22)
* (Apollon77) Small fix for older configurations

## 1.3.3 (2017-02-08)
* (Apollon77) Enhance handling of DB outages with mass writes afterwards
* (Apollon77) Small fix for older configurations

## 1.3.2
* (Apollon77) Enhance Min-Delta logic for data points from type mixed

## 1.3.1 (2017-01-16)
* (bluefox) Fix handling of float values in Adapter config and Datapoint config.

## 1.3.0 (2016-12-02)
* (Apollon77) Added messages enableHistory/disableHistory
* (Apollon77) Added support to log changes only if value differs a minimum value for numbers

## 1.2.1 (2016-11)
* (Apollon77) small enhancements and fixes

## 1.2.0 (2016-11-05)
* (Apollon77) support re-logging also for states that are not updated often (timed relog using relog-Interval)
* (Apollon77) try to solve easy type conflicts and convert float <--> boolean if needed
* (Apollon77) enhance `getHistory` to retrieve also boundary values better by selecting also points outside of the given time boundaries

## 1.1.1 (2016-11-03)
* (Apollon77) small final change on custom dialog

## 1.1.0 (2016-10-29)
* (Apollon77) optimizations and enhancements for high-traffic situations
* (Apollon77) Bugfix if InfluxDB is unavailable
* (Apollon77) Added functions getConflictingPoints and resetConflictingPoints
* (Apollon77) Added option to re-log unchanged values to make it easier for visualization

## 1.0.1 (2016-10-18)
* (Apollon77) changed storing values to asynchronous way
* (Apollon77) Added support for `minmax` aggregate type on getHistory

## 1.0.0 (2016-10-10)
* (Apollon77) bulk write into DB
* (Apollon77) buffer values if no connection to DB
* (bluefox) connection indication

## 0.5.3 (2016-09-30)
* (Apollon77) enhance and correct documentation
* (Apollon77) implement "step" calculation for aggregation if only "count" is set

## 0.5.2 (2016-09-25)
* (Apollon77) change custom queries. Fix delete DB

## 0.5.1 (2016-09-20)
* (Apollon77) Step is given as Milliseconds and not Seconds!

## 0.5.0 (2016-08-30)
* (bluefox) Compatible only with new admin

## 0.4.0 (2016-08-27)
* (bluefox) change name of object from "history" to "custom"

## 0.3.1 (2016-06-07)
* (bluefox) better error handling

## 0.3.1 (2016-06-05)
* (bluefox) no support of influxDB 0.8 (please update)

## 0.3.0 (2016-05-18)
* (bluefox) make name of DB configurable

## 0.2.0 (2016-04-30)
* (bluefox) support of milliseconds instead of seconds

## 0.1.2 (2015-12-19)
* (bluefox) make onchange work

## 0.1.1 (2015-12-19)
* (bluefox) retention policy for 0.9.x

## 0.1.0 (2015-12-19)
* (bluefox) supported InfluxDB version 0.9.x and 0.8.x

## 0.0.2 (2015-12-14)
* (bluefox) change supported InfluxDB version to 0.9.x

## 0.0.1 (2015-12-12)
* (bluefox) initial commit
