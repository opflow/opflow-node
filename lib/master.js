'use strict';

var lodash = require('lodash');
var util = require('util');
var debugx = require('debug')('opflow:master');
var Runner = require('./runner');

var Master = function(params) {
  debugx.enabled && debugx(' + constructor begin ...');

  params = lodash.defaults({mode: 'master'}, params);
  Runner.call(this, params);

  debugx.enabled && debugx(' - constructor end!');
}

util.inherits(Master, Runner);

module.exports = Master;
