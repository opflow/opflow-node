'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var debugx = require('debug')('opflow:task');
var misc = require('./util');

var Task = function(args) {
  events.EventEmitter.call(this);

  debugx.enabled && debugx(' + constructor begin ...');

  args = args || {};
  var _routineId = args.routineId || args.routineID;
  var _requestId = args.requestId || args.requestID;
  var self = this;

  Object.defineProperties(this, {
    'routineId': {
      get: function() { return _routineId = _routineId || misc.getUUID() },
      set: function(val) {}
    },
    'requestId': {
      get: function() { return _requestId = _requestId || misc.getUUID() },
      set: function(val) {}
    }
  });

  debugx.enabled && debugx(' - constructor end!');
}

util.inherits(Task, events.EventEmitter);

module.exports = Task;
