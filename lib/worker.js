'use strict';


var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var debugx = require('debug')('opflow:worker');
var Engine = require('./engine');

var Handler = function(params) {
  events.EventEmitter.call(this);

  debugx.enabled && debugx(' + constructor begin ...');

  var self = this;
  self.logger = self.logger || params.logger;

  var engine = new Engine(params);

  this.process = function(callback) {
    return engine.readyConsumer().then(function() {
      return engine.consume(function(message, info, done, notifier) {
        callback(message, done, notifier);
      });
    });
  }

  this.destroy = function() {
    return engine.destroy();
  }

  debugx.enabled && debugx(' - constructor end!');
}

util.inherits(Handler, events.EventEmitter);

module.exports = Handler;
