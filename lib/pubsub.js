'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var Engine = require('./engine');
var Executor = require('./executor');
var misc = require('./util');
var LogAdapter = require('./logadapter');
var LX = LogAdapter.getLogger({ scope: 'opflow:pubsub' });

var Handler = function(params) {
  events.EventEmitter.call(this);

  params = lodash.defaults({mode: 'pubsub'}, params, { engineId: misc.getUUID() });
  var pubsubHandlerId = params.engineId;
  var self = this;

  LX.isEnabledFor('info') && LX.log('info', {
    message: 'PubsubHandler.new()',
    pubsubHandlerId: pubsubHandlerId,
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
    LX.isEnabledFor('info') && LX.log('info', {
      message: 'ready() is invoked',
      pubsubHandlerId: pubsubHandlerId });
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
    return Promise.all(asyncFuns).then(function(result) {
      LX.isEnabledFor('info') && LX.log('info', {
        message: 'ready() has been done',
        pubsubHandlerId: pubsubHandlerId });
      return result;
    });
  }

  self.publish = function(body, headers, routingKey) {
    headers = headers || {};
    headers['requestId'] = headers['requestId'] || misc.getUUID();
    var properties = { headers: headers }
    var override = { routingKey: routingKey }
    LX.isEnabledFor('info') && LX.log('info', {
      message: 'PubsubHandler.publish',
      pubsubHandlerId: pubsubHandlerId,
      requestId: headers['requestId'],
      routingKey: routingKey });
    return engine.produce(body, properties, override);
  }

  self.subscribe = function(callback) {
    listener = listener || callback;
    if (listener == null) {
      var error = {
        message: 'Pubsub listener should not be null',
        pubsubHandlerId: pubsubHandlerId
      }
      LX.isEnabledFor('debug') && LX.log('debug', error);
      return Promise.reject(error);
    } else if (listener != callback) {
      var error = {
        message: 'Pubsub only supports single listener',
        pubsubHandlerId: pubsubHandlerId
      }
      LX.isEnabledFor('debug') && LX.log('debug', error);
      return Promise.reject(error);
    }
    var options = {
      noAck: true,
      queueName: subscriberName
    }
    return engine.consume(function(msg, sandbox, done) {
      var headers = msg && msg.properties && msg.properties.headers;
      var requestId = misc.getRequestId(headers);
      LX.isEnabledFor('info') && LX.log('info', {
        message: 'Subscriber receive a message',
        subscriberId: sandbox.id,
        requestId: requestId });
      callback(msg.content, headers, function(err, result) {
        if (err) {
          LX.isEnabledFor('info') && LX.log('info', {
            message: 'consume() - processing failed, requeue the message',
            subscriberId: sandbox.id,
            requestId: requestId });
          var strDone;
          var props = lodash.clone(msg.properties);
          props.headers = props.headers || {};
          props.headers['redeliveredCount'] = (props.headers['redeliveredCount'] || 0) + 1;
          if (props.headers['redeliveredCount'] <= redeliveredLimit) {
            LX.isEnabledFor('debug') && LX.log('debug', {
              message: 'subscribe() - requeue to Subscriber',
              subscriberId: sandbox.id,
              requestId: requestId,
              redeliveredCount: props.headers['redeliveredCount'],
              redeliveredLimit: redeliveredLimit });
            strDone = executor.sendToQueue(msg.content, props, options, sandbox);
          } else {
            if (recyclebinName) {
              LX.isEnabledFor('debug') && LX.log('debug', {
                message: 'subscribe() - enqueue to Recyclebin',
                subscriberId: sandbox.id,
                requestId: requestId,
                redeliveredCount: props.headers['redeliveredCount'],
                redeliveredLimit: redeliveredLimit });
              strDone = executor.sendToQueue(msg.content, props, {
                queueName: recyclebinName
              }, sandbox);
            } else {
              LX.isEnabledFor('debug') && LX.log('debug', {
                message: 'subscribe() - throw message away',
                subscriberId: sandbox.id,
                requestId: requestId,
                redeliveredCount: props.headers['redeliveredCount'],
                redeliveredLimit: redeliveredLimit });
              strDone = Promise.resolve();
            }
          }
          strDone.finally(function() {
            LX.isEnabledFor('info') && LX.log('info', {
              message: 'turn back with failure',
              subscriberId: sandbox.id,
              requestId: requestId });
            done(err);
          });
        } else {
          LX.isEnabledFor('info') && LX.log('info', {
            message: 'turn back with success',
            subscriberId: sandbox.id,
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
    LX.isEnabledFor('info') && LX.log('info', {
      message: 'close() is invoked',
      pubsubHandlerId: pubsubHandlerId });
    return Promise.mapSeries(consumerRefs, function(consumerRef) {
      return engine.cancelConsumer(consumerRef);
    }).then(function() {
      listener = null;
      while(consumerRefs.length > 0) consumerRefs.pop();
      LX.isEnabledFor('info') && LX.log('info', {
        message: 'close() has been finished',
        pubsubHandlerId: pubsubHandlerId });
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
    LX.isEnabledFor('debug') && LX.log('debug', {
      message: 'auto execute ready()',
      pubsubHandlerId: pubsubHandlerId });
    misc.notifyConstructor(self.ready(), self);
  }

  LX.isEnabledFor('info') && LX.log('info', {
    message: 'PubsubHandler.new() end!',
    pubsubHandlerId: pubsubHandlerId });
}

util.inherits(Handler, events.EventEmitter);

module.exports = Handler;
