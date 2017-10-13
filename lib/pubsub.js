'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var Engine = require('./engine');
var Executor = require('./executor');
var errors = require('./exception');
var misc = require('./util');
var LogTracer = require('./log_tracer');
var LogAdapter = require('./log_adapter');
var LX = LogAdapter.getLogger({ scope: 'opflow:pubsub' });

var Handler = function(params) {
  var self = this;
  events.EventEmitter.call(this);

  params = lodash.defaults({mode: 'pubsub'}, params, { engineId: misc.getLogID() });
  var pubsubTrail = LogTracer.ROOT.branch({ key:'pubsubHandlerId', value:params.engineId });
  var pubsubHandlerId = params.engineId;

  LX.isEnabledFor('info') && LX.log('info', pubsubTrail.add({
    message: 'PubsubHandler.new()'
  }).toString());

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

  if (subscriberName != null && recyclebinName != null && subscriberName == recyclebinName) {
    throw new errors.BootstrapError("subscriberName should be different with recyclebinName");
  }

  var redeliveredLimit = 0;
  if (lodash.isNumber(params.redeliveredLimit)) {
    redeliveredLimit = params.redeliveredLimit;
    if (redeliveredLimit < 0) redeliveredLimit = 0;
  }

  this.ready = function() {
    LX.isEnabledFor('info') && LX.log('info', pubsubTrail.add({
      message: 'ready() is invoked'
    }).toString());
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
      LX.isEnabledFor('info') && LX.log('info', pubsubTrail.add({
        message: 'ready() has been done'
      }).toString());
      return result;
    });
  }

  var produce = function(producerType, body, headers, routingKey) {
    headers = headers || {};
    headers['requestId'] = headers['requestId'] || misc.getLogID();
    var properties = { headers: headers }
    var override = { routingKey: routingKey }
    LX.isEnabledFor('info') && LX.log('info', pubsubTrail.reset().add({
      message: 'PubsubHandler.publish',
      requestId: headers['requestId'],
      routingKey: routingKey
    }).toString());
    if (producerType === 'stream') {
      return engine.exhaust(body, properties, override);
    }
    return engine.produce(body, properties, override);
  }

  self.exhaust = produce.bind(self, 'stream');

  self.publish = produce.bind(self, 'single');

  self.subscribe = function(callback) {
    listener = listener || callback;
    if (listener == null) {
      var error = {
        message: 'Pubsub listener should not be null',
        pubsubHandlerId: pubsubHandlerId
      }
      LX.isEnabledFor('debug') && LX.log('debug', pubsubTrail.add(error).toString());
      return Promise.reject(error);
    } else if (listener != callback) {
      var error = {
        message: 'Pubsub only supports single listener',
        pubsubHandlerId: pubsubHandlerId
      }
      LX.isEnabledFor('debug') && LX.log('debug', pubsubTrail.add(error).toString());
      return Promise.reject(error);
    }
    var options = {
      noAck: true,
      queueName: subscriberName
    }
    return engine.consume(function(msg, sandbox, done) {
      var consumeTrail = (sandbox.id) ? pubsubTrail.branch({
        key: 'consumerId', value: sandbox.id
      }) : pubsubTrail.copy();

      var headers = msg && msg.properties && msg.properties.headers;
      var requestId = misc.getRequestId(headers);
      var requestTrail = consumeTrail.branch({key:'requestId',value:requestId});
      LX.isEnabledFor('info') && LX.log('info', requestTrail.add({
        message: 'subscribe() receive a message'
      }).toString());

      callback(msg.content, headers, function(err, result) {
        if (err) {
          LX.isEnabledFor('info') && LX.log('info', requestTrail.add({
            message: 'consume() - processing failed, requeue the message'
          }).toString());
          var strDone;
          var props = lodash.clone(msg.properties);
          props.headers = props.headers || {};
          props.headers['redeliveredCount'] = (props.headers['redeliveredCount'] || 0) + 1;
          if (props.headers['redeliveredCount'] <= redeliveredLimit) {
            LX.isEnabledFor('debug') && LX.log('debug', requestTrail.add({
              message: 'subscribe() - requeue to Subscriber',
              redeliveredCount: props.headers['redeliveredCount'],
              redeliveredLimit: redeliveredLimit
            }).toString({reset:true}));
            strDone = executor.sendToQueue(msg.content, props, options, sandbox);
          } else {
            if (recyclebinName) {
              LX.isEnabledFor('debug') && LX.log('debug', requestTrail.add({
                message: 'subscribe() - enqueue to Recyclebin',
                redeliveredCount: props.headers['redeliveredCount'],
                redeliveredLimit: redeliveredLimit
              }).toString({reset:true}));
              strDone = executor.sendToQueue(msg.content, props, {
                queueName: recyclebinName
              }, sandbox);
            } else {
              LX.isEnabledFor('debug') && LX.log('debug', requestTrail.add({
                message: 'subscribe() - throw message away',
                redeliveredCount: props.headers['redeliveredCount'],
                redeliveredLimit: redeliveredLimit
              }).toString({reset:true}));
              strDone = Promise.resolve();
            }
          }
          strDone.finally(function() {
            LX.isEnabledFor('info') && LX.log('info', requestTrail.add({
              message: 'turn back with failure'
            }).toString());
            done(err);
          });
        } else {
          LX.isEnabledFor('info') && LX.log('info', requestTrail.add({
            message: 'turn back with success'
          }).toString());
          done(null);
        }
      });
    }, options).then(function(consumerRef) {
      consumerRefs.push(consumerRef);
      return consumerRef;
    });
  }

  this.close = function() {
    var closeTrail = pubsubTrail.copy();
    LX.isEnabledFor('info') && LX.log('info', closeTrail.add({
      message: 'close() is invoked'
    }).toString());
    return Promise.mapSeries(consumerRefs, function(consumerRef) {
      return engine.cancelConsumer(consumerRef);
    }).then(function() {
      listener = null;
      while(consumerRefs.length > 0) consumerRefs.pop();
      LX.isEnabledFor('info') && LX.log('info', closeTrail.add({
        message: 'close() has been finished'
      }).toString());
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
    LX.isEnabledFor('debug') && LX.log('debug', pubsubTrail.reset().add({
      message: 'auto execute ready()'
    }).toString());
    misc.notifyConstructor(self.ready(), self);
  }

  LX.isEnabledFor('info') && LX.log('info', pubsubTrail.reset().add({
    message: 'PubsubHandler.new() end!'
  }).toString());
}

util.inherits(Handler, events.EventEmitter);

module.exports = Handler;
