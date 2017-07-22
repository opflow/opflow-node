'use strict';

var lodash = require('lodash');
var util = require('util');
var debugx = require('debug')('opflow:master');
var Bridge = require('./bridge');

var Master = function(params) {
  debugx.enabled && debugx(' + constructor begin ...');

  params = lodash.defaults({mode: 'master'}, params);
  Bridge.call(this, params);

  debugx.enabled && debugx(' - constructor end!');
}

util.inherits(Master, Bridge);

module.exports = Master;
