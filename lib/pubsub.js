'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var uuidV4 = require('uuid/v4');
var debugx = require('debug')('opflow:pubsub');
var Engine = require('./engine');

var Handler = function(params) {
  events.EventEmitter.call(this);

  debugx.enabled && debugx(' + constructor begin ...');

  params = params || {};
  var self = this;
  self.logger = self.logger || params.logger;

  var engine = new Engine(params);

  self.publish = function(data, channel) {
    return engine.produce(data, {}, { routingKey: channel });
  }

  self.subscribe = function(callback) {
    return engine.readyConsumer().then(function() {
      return engine.consume(function(message, info, done) {
        callback(message, info);
        done();
      });
    });
  }

  this.ready = engine.ready.bind(engine);

  this.purge = engine.purge.bind(engine);

  this.destroy = engine.destroy.bind(engine);

  debugx.enabled && debugx(' - constructor end!');
}

util.inherits(Handler, events.EventEmitter);

module.exports = Handler;
