'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var debugx = require('debug')('opflow:worker');
var Engine = require('./engine');

var Worker = function(params) {
  debugx.enabled && debugx(' + constructor begin ...');

  params = lodash.defaults({mode: 'worker'}, params);
  var self = this;
  self.logger = self.logger || params.logger;

  var store = { tasks: {} }
  var engine = new Engine(params);
  
  this.process = function(callback) {
    return engine.readyConsumer().then(function() {
      return engine.consume(function(message, info, done, notifier) {
        var headers = info && info.properties && info.properties.headers || {};
        callback(message, headers, done, notifier);
      });
    });
  };

  this.ready = engine.ready.bind(engine);

  this.purge = engine.purge.bind(engine);

  this.destroy = engine.destroy.bind(engine);

  debugx.enabled && debugx(' - constructor end!');
}

util.inherits(Worker, events.EventEmitter);

module.exports = Worker;
