'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var Engine = require('./engine');
var Executor = require('./executor');
var misc = require('./util');
var LogAdapter = require('./logadapter');
var L = LogAdapter.getLogger({ scope: 'opflow:pubsub' });

var Handler = function(params) {
  L.isEnabledFor('verbose') && L.log('verbose', ' + constructor begin ...');

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

  this.ready = function() {
    return engine.ready();
  }

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
    return engine.consume(function(msg, sandbox, done) {
      var headers = msg && msg.properties && msg.properties.headers;
      var requestId = misc.getRequestId(headers);
      L.isEnabledFor('verbose') && L.log('verbose', 'Request[%s] - subscribe() receive a message', requestId);
      callback(msg.content, headers, function(err, result) {
        if (err) {
          L.isEnabledFor('verbose') && L.log('verbose', 'consume() - processing failed, requeue the message');
          var strDone;
          var props = lodash.clone(msg.properties);
          props.headers = props.headers || {};
          props.headers['redeliveredCount'] = (props.headers['redeliveredCount'] || 0) + 1;
          if (props.headers['redeliveredCount'] <= redeliveredLimit) {
            L.isEnabledFor('verbose') && L.log('verbose', 'consume() - enqueueConsumer message');
            strDone = executor.sendToQueue(msg.content, props, options, sandbox);
          } else {
            L.isEnabledFor('verbose') && L.log('verbose', 'consume() - enqueueRecycler message');
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
    }, options).then(function(consumerRef) {
      consumerRefs.push(consumerRef);
      return consumerRef;
    });
  }

  this.close = function() {
    return Promise.mapSeries(consumerRefs, function(consumerRef) {
      return engine.cancelConsumer(consumerRef);
    }).then(function() {
      listener = null;
      while(consumerRefs.length > 0) consumerRefs.pop();
      return engine.close();
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

  L.isEnabledFor('verbose') && L.log('verbose', ' - constructor end!');
}

module.exports = Handler;
