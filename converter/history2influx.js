/* jshint -W097 */// jshint strict:false
/*jslint node: true */
"use strict";

//noinspection JSUnresolvedFunction
var utils  = require(__dirname + '/lib/utils'); // Get common adapter utils
var influxInstance = "influxdb.0";

adapter.sendTo(influxInstance, "storeState", {
    state: {'result': rows, 'ts': new Date().getTime()},
    id:  ""
}, msg.callback);
