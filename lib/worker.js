'use strict';

var lodash = require('lodash');
var util = require('util');
var debugx = require('debug')('opflow:worker');
var Bridge = require('./bridge');

var Worker = function(params) {
  debugx.enabled && debugx(' + constructor begin ...');

  params = lodash.defaults({mode: 'worker'}, params);
  Bridge.call(this, params);

  debugx.enabled && debugx(' - constructor end!');
}

util.inherits(Worker, Bridge);

module.exports = Worker;
