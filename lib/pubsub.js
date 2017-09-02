'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var debugx = require('debug')('opflow:pubsub');
var Engine = require('./engine');
var Executor = require('./executor');
var misc = require('./util');

var Handler = function(params) {
  events.EventEmitter.call(this);

  debugx.enabled && debugx(' + constructor begin ...');

  params = params || {};
  var self = this;
  self.logger = self.logger || params.logger;

  params = lodash.defaults({mode: 'pubsub'}, params);
  var engine = new Engine(params);
  var executor = new Executor({ engine: engine });
  var listener = null;
  var consumerRefs = [];

  var subscriberName = null;
  if (lodash.isString(params.subscriberName)) {
    subscriberName = params.subscriberName;
    executor.assertQueue({
      queueName: subscriberName
    });
  }

  var recyclebinName = null;
  if (lodash.isString(params.recyclebinName)) {
    recyclebinName = params.recyclebinName;
    executor.assertQueue({
      queueName: recyclebinName
    });
  }

  var redeliveredLimit = 0;
  if (lodash.isNumber(params.redeliveredLimit)) {
    redeliveredLimit = params.redeliveredLimit;
    if (redeliveredLimit < 0) redeliveredLimit = 0;
  }

  this.ready = engine.ready.bind(engine);

  self.publish = function(body, headers, routingKey) {
    headers = headers || {};
    headers['requestId'] = headers['requestId'] || misc.getUUID();
    var properties = { headers: headers }
    var override = { routingKey: routingKey }
    return engine.produce(body, properties, override);
  }

  self.subscribe = function(callback) {
    listener = listener || callback;
    if (listener == null) {
      return Promise.reject({
        message: 'listener should not be null'
      });
    } else if (listener != callback) {
      return Promise.reject({
        message: 'Pubsub only supports single listener'
      });
    }
    var options = {
      noAck: true,
      queueName: subscriberName
    }
    var consumerRef = engine.consume(function(msg, sandbox, done) {
      var headers = msg && msg.properties && msg.properties.headers;
      var requestId = misc.getRequestId(headers);
      debugx.enabled && debugx('Request[%s] - subscribe() receive a message', requestId);
      callback(msg.content, headers, function(err, result) {
        if (err) {
          debugx.enabled && debugx('consume() - processing failed, requeue the message');
          var strDone;
          var props = lodash.clone(msg.properties);
          props.headers = props.headers || {};
          props.headers['redeliveredCount'] = (props.headers['redeliveredCount'] || 0) + 1;
          if (props.headers['redeliveredCount'] <= redeliveredLimit) {
            debugx.enabled && debugx('consume() - enqueueConsumer message');
            strDone = executor.sendToQueue(msg.content, props, options, sandbox);
          } else {
            debugx.enabled && debugx('consume() - enqueueRecycler message');
            if (recyclebinName) {
              strDone = executor.sendToQueue(msg.content, props, {
                queueName: recyclebinName
              }, sandbox);
            } else {
              strDone = Promise.resolve();
            }
          }
          if (strDone) {
            strDone.finally(function() { done(err, result) });
          }
        } else {
          done(null, result);
        }
      });
    }, options);

    consumerRefs.push(consumerRef);

    return consumerRef;
  }

  this.destroy = function() {
    return Promise.mapSeries(consumerRefs, function(consumerInfo) {
      return engine.cancelConsumer(consumerInfo);
    }).then(function() {
      return engine.destroy();
    });
  }

  Object.defineProperty(this, 'executor', {
    get: function() { return executor; },
    set: function(value) {}
  });

  Object.defineProperty(this, 'subscriberName', {
    get: function() { return subscriberName; },
    set: function(value) {}
  });

  Object.defineProperty(this, 'recyclebinName', {
    get: function() { return recyclebinName; },
    set: function(value) {}
  });

  debugx.enabled && debugx(' - constructor end!');
}

util.inherits(Handler, events.EventEmitter);

module.exports = Handler;
