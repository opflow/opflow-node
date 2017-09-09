'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var Engine = require('./engine');
var Executor = require('./executor');
var misc = require('./util');
var LogAdapter = require('./logadapter');
var L = LogAdapter.getLogger({ scope: 'opflow:pubsub' });

var Handler = function(params) {
  events.EventEmitter.call(this);

  params = lodash.defaults({mode: 'pubsub'}, params, { engineId: misc.getUUID() });
  var self = this;

  L.isEnabledFor('info') && L.log('info', {
    message: 'new PubsubHandler()',
    pubsubHandlerId: params.engineId,
    instanceId: misc.instanceId });

  var engine = new Engine(params);
  var executor = new Executor({ engine: engine });
  var listener = null;
  var consumerRefs = [];

  var subscriberName = null;
  if (lodash.isString(params.subscriberName)) {
    subscriberName = params.subscriberName;
  }

  var recyclebinName = null;
  if (lodash.isString(params.recyclebinName)) {
    recyclebinName = params.recyclebinName;
  }

  var redeliveredLimit = 0;
  if (lodash.isNumber(params.redeliveredLimit)) {
    redeliveredLimit = params.redeliveredLimit;
    if (redeliveredLimit < 0) redeliveredLimit = 0;
  }

  this.ready = function() {
    var asyncFuns = [];
    asyncFuns.push(engine.ready());
    if (subscriberName) {
      asyncFuns.push(executor.assertQueue({
        queueName: subscriberName
      }));
    }
    if (recyclebinName) {
      asyncFuns.push(executor.assertQueue({
        queueName: recyclebinName
      }));
    }
    return Promise.all(asyncFuns);
  }

  self.publish = function(body, headers, routingKey) {
    headers = headers || {};
    headers['requestId'] = headers['requestId'] || misc.getUUID();
    var properties = { headers: headers }
    var override = { routingKey: routingKey }
    L.isEnabledFor('info') && L.log('info', {
      message: 'PubsubHandler.publish',
      pubsubHandlerId: params.engineId,
      requestId: headers['requestId'],
      routingKey: routingKey });
    return engine.produce(body, properties, override);
  }

  self.subscribe = function(callback) {
    listener = listener || callback;
    if (listener == null) {
      var error = {
        message: 'Pubsub listener should not be null',
        pubsubHandlerId: params.engineId
      }
      L.isEnabledFor('debug') && L.log('debug', error);
      return Promise.reject(error);
    } else if (listener != callback) {
      var error = {
        message: 'Pubsub only supports single listener',
        pubsubHandlerId: params.engineId
      }
      L.isEnabledFor('debug') && L.log('debug', error);
      return Promise.reject(error);
    }
    var options = {
      noAck: true,
      queueName: subscriberName
    }
    return engine.consume(function(msg, sandbox, done) {
      var headers = msg && msg.properties && msg.properties.headers;
      var requestId = misc.getRequestId(headers);
      L.isEnabledFor('info') && L.log('info', {
        message: 'Subscriber receive a message',
        pubsubHandlerId: params.engineId,
        requestId: requestId });
      callback(msg.content, headers, function(err, result) {
        if (err) {
          L.isEnabledFor('info') && L.log('info', {
            message: 'consume() - processing failed, requeue the message',
            requestId: requestId });
          var strDone;
          var props = lodash.clone(msg.properties);
          props.headers = props.headers || {};
          props.headers['redeliveredCount'] = (props.headers['redeliveredCount'] || 0) + 1;
          if (props.headers['redeliveredCount'] <= redeliveredLimit) {
            L.isEnabledFor('debug') && L.log('debug', {
              message: 'subscribe() - requeue to Subscriber',
              requestId: requestId,
              redeliveredCount: props.headers['redeliveredCount'],
              redeliveredLimit: redeliveredLimit });
            strDone = executor.sendToQueue(msg.content, props, options, sandbox);
          } else {
            if (recyclebinName) {
              L.isEnabledFor('debug') && L.log('debug', {
                message: 'subscribe() - enqueue to Recyclebin',
                requestId: requestId,
                redeliveredCount: props.headers['redeliveredCount'],
                redeliveredLimit: redeliveredLimit });
              strDone = executor.sendToQueue(msg.content, props, {
                queueName: recyclebinName
              }, sandbox);
            } else {
              L.isEnabledFor('debug') && L.log('debug', {
                message: 'subscribe() - throw message away',
                requestId: requestId,
                redeliveredCount: props.headers['redeliveredCount'],
                redeliveredLimit: redeliveredLimit });
              strDone = Promise.resolve();
            }
          }
          strDone.finally(function() {
            L.isEnabledFor('info') && L.log('info', {
              message: 'turn back with failure',
              requestId: requestId });
            done(err);
          });
        } else {
          L.isEnabledFor('info') && L.log('info', {
            message: 'turn back with success',
            requestId: requestId });
          done(null);
        }
      });
    }, options).then(function(consumerRef) {
      consumerRefs.push(consumerRef);
      return consumerRef;
    });
  }

  this.close = function() {
    L.isEnabledFor('info') && L.log('info', {
      message: 'close() is invoked',
      pubsubHandlerId: params.engineId });
    return Promise.mapSeries(consumerRefs, function(consumerRef) {
      return engine.cancelConsumer(consumerRef);
    }).then(function() {
      listener = null;
      while(consumerRefs.length > 0) consumerRefs.pop();
      L.isEnabledFor('info') && L.log('info', {
        message: 'close() has been finished',
        pubsubHandlerId: params.engineId });
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

  if (params.autoinit !== false) {
    L.isEnabledFor('debug') && L.log('debug', {
      message: 'auto execute ready()',
      pubsubHandlerId: params.engineId });
    misc.notifyConstructor(self.ready(), self);
  }

  L.isEnabledFor('info') && L.log('info', {
    message: 'new PubsubHandler() done!',
    pubsubHandlerId: params.engineId });
}

util.inherits(Handler, events.EventEmitter);

module.exports = Handler;
