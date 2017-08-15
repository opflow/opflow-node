'use strict';

var lodash = require('lodash');
var util = require('util');
var debugx = require('debug')('opflow:worker');
var Runner = require('./runner');

var Worker = function(params) {
  debugx.enabled && debugx(' + constructor begin ...');

  params = lodash.defaults({mode: 'worker'}, params);
  Runner.call(this, params);

  debugx.enabled && debugx(' - constructor end!');
}

util.inherits(Worker, Runner);

module.exports = Worker;
