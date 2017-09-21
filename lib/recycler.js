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
var LX = LogAdapter.getLogger({ scope: 'opflow:recycler' });

var Recycler = function(params) {
  var self = this;
  events.EventEmitter.call(this);

  params = lodash.defaults({mode: 'recycler'}, params, { engineId: misc.getUUID() });
  var recyclerTrail = LogTracer.ROOT.branch({key: 'recyclerId', value: params.engineId});

  LX.isEnabledFor('info') && LX.log('info', recyclerTrail.add({
    message: 'Recycler.new()'
  }).toString());
  
  var engine = new Engine(params);
  var executor = new Executor({ engine: engine });
  var listener = null;
  var session = {};
  var consumerRefs = [];

  var subscriberConfig = null;
  if (lodash.isString(params.subscriberName)) {
    subscriberConfig = misc.defaultQueueParams({
      queueName: params.subscriberName
    });
  }

  var recyclebinConfig = null;
  if (lodash.isString(params.recyclebinName)) {
    recyclebinConfig = misc.defaultQueueParams({
      queueName: params.recyclebinName
    });
  } else {
    throw new errors.ParameterError();
  }

  if (subscriberConfig.queueName != null && 
      recyclebinConfig.queueName != null && 
      subscriberConfig.queueName == recyclebinConfig.queueName) {
    throw new errors.BootstrapError("subscriberName should be different with recyclebinName");
  }

  this.ready = function() {
    var asyncFuns = [];
    if (subscriberConfig) {
      asyncFuns.push(executor.assertQueue(subscriberConfig));
    }
    if (recyclebinConfig) {
      asyncFuns.push(executor.assertQueue(recyclebinConfig));
    }
    return Promise.all(asyncFuns);
  }

  this.recycle = function(callback) {
    listener = listener || callback;
    if (listener == null) {
      var error = {
        message: 'Recycler listener should not be null',
        recyclerId: params.engineId
      }
      LX.isEnabledFor('debug') && LX.log('debug', recyclerTrail.add(error).toString());
      return Promise.reject(error);
    } else if (listener != callback) {
      var error = {
        message: 'Recycler only supports single listener',
        recyclerId: params.engineId
      }
      LX.isEnabledFor('debug') && LX.log('debug', recyclerTrail.add(error).toString());
      return Promise.reject(error);
    }

    var options = {
      noAck: false,
      requeueFailure: true,
      queueName: recyclebinConfig.queueName,
      binding: false
    }
    
    return engine.consume(function(msg, sandbox, done) {
      var headers = msg && msg.properties && msg.properties.headers;
      var requestId = misc.getRequestId(headers);
      var requestTrail = recyclerTrail.branch({key:'requestId',value:requestId});
      var ch = sandbox.channel;
      LX.isEnabledFor('info') && LX.log('info', requestTrail.add({
        message: 'recycle() get message from Recyclebin'
      }).toString());
      listener(msg.content, lodash.pick(msg, ['fields', 'properties']), function(err) {
        if (err) {
          LX.isEnabledFor('info') && LX.log('info', requestTrail.add({
            message: 'recycle() processing has been failed, nack()'
          }).toString());
          ch.nack(msg);
        } else {
          LX.isEnabledFor('info') && LX.log('info', requestTrail.add({
            message: 'recycle() processing has completed, ack()'
          }).toString());
          ch.ack(msg);
        }
      });
    }, options).then(function(consumerRef) {
      consumerRefs.push(consumerRef);
      return consumerRef;
    });
  }

  this.examine = function(callback) {
    var self = this;
    var result = { obtained: 0 };
    result.callback = lodash.isFunction(callback);
    return examineGarbage(session).then(function(msg) {
      if (msg) {
        result.obtained = 1;
        if (!result.callback) return result;
        return new Promise(function(resolved, rejected) {
          var copied = lodash.pick(msg, ['content', 'fields', 'properties']);
          callback(copied, function update(action, data) {
            if (data && lodash.isObject(data)) {
              if(data.content instanceof Buffer) {
                msg.content = data.content;
              } else if (typeof(data.content) === 'string') {
                msg.content = misc.bufferify(data.content);
              } else if (typeof(data.content) === 'object') {
                msg.content = misc.bufferify(data.content);
              }
              if (data.properties && lodash.isObject(data.properties)) {
                lodash.merge(msg.properties, data.properties);
              }
            }
            if (lodash.isFunction(garbageAction[action])) {
              result.nextAction = true;
              garbageAction[action].call(self, session).then(function() {
                resolved(result);
              }).catch(function(err) {
                result.actionError = err;
                rejected(result);
              });
            } else {
              result.nextAction = false;
              resolved(result);
            }
          });
        });
      } else {
        return result;
      }
    });
  }

  this.checkSubscriber = function() {
    if (!subscriberConfig || !subscriberConfig.queueName) return Promise.resolve({});
    return engine.openSession(session).then(function(ch) {
      return Promise.promisify(ch.checkQueue, {context: ch})(subscriberConfig.queueName);
    });
  }

  this.purgeSubscriber = function() {
    if (!subscriberConfig || !subscriberConfig.queueName) return Promise.resolve({});
    return engine.openSession(session).then(function(ch) {
      return Promise.promisify(ch.purgeQueue, {context: ch})(subscriberConfig.queueName);
    });
  }

  this.checkRecyclebin = function() {
    return engine.openSession(session).then(function(ch) {
      return Promise.promisify(ch.checkQueue, {context: ch})(recyclebinConfig.queueName);
    });
  }

  this.purgeRecyclebin = function() {
    return engine.openSession(session).then(function(ch) {
      return Promise.promisify(ch.purgeQueue, {context: ch})(recyclebinConfig.queueName);
    });
  }

  this.close = function() {
    if (!engine) return Promise.resolve(true);
    return engine.closeSession(session).then(function() {
      return Promise.mapSeries(consumerRefs, function(consumerInfo) {
        return engine.cancelConsumer(consumerInfo);
      });
    }).then(function() {
      return engine.close();
    }).then(function() {
      listener = null;
      while(consumerRefs.length > 0) consumerRefs.pop();
      return true;
    });
  }

  var examineGarbage = function(sandbox) {
    if (sandbox.garbage) return Promise.resolve(sandbox.garbage);
    return engine.openSession(sandbox).then(function(ch) {
      return assertQueue(ch, recyclebinConfig).then(function(qok) {
        return Promise.promisify(ch.get, {context: ch})(qok.queue, {});
      }).then(function(msgOrFalse) {
        LX.isEnabledFor('conlog') && LX.log('conlog', recyclerTrail.add({
          message: 'examineGarbage()',
          garbage: msgOrFalse
        }).toString({reset:true}));
        if (msgOrFalse !== false) sandbox.garbage = msgOrFalse;
        return sandbox.garbage;
      });
    })
  }

  var assertQueue = function(ch, config) {
    var ch_assertQueue = Promise.promisify(ch.assertQueue, {context: ch});
    if (config.queueName) {
      return ch_assertQueue(config.queueName, {
        durable: config.durable,
        exclusive: config.exclusive,
        autoDelete: config.autoDelete
      });
    } else {
      return ch_assertQueue(null, {
        durable: false, exclusive: true, autoDelete: true
      });
    }
  }

  var sendToQueue = function(body, props, config, sandbox) {
    sandbox = sandbox || {};
    config = misc.defaultQueueParams(config);
    if (!config.queueName) return Promise.reject({
      msg: 'queueName should not be null'
    });
    props = props || {};
    LX.isEnabledFor('conlog') && LX.log('conlog', recyclerTrail.add({
      message: 'Recycler send an object to queue'
    }).toString());
    var _sendTo = function(ch) {
      return ch.sendToQueue(config.queueName, misc.bufferify(body), props);
    }
    var _doWith = function(ch) {
      return assertQueue(ch, config).then(function() {
        if (sandbox.sendable !== false) {
          sandbox.sendable =_sendTo(ch);
          LX.isEnabledFor('conlog') && LX.log('conlog', recyclerTrail.add({
            message: 'Recycler channel is writable, msg has been sent'
          }).toString());
        } else {
          ch.once('drain', function() {
            sandbox.sendable =_sendTo(ch);
            LX.isEnabledFor('conlog') && LX.log('conlog', recyclerTrail.add({
              message: 'Recycler channel is drained, flushed'
            }).toString());
          });
          LX.isEnabledFor('conlog') && LX.log('conlog', recyclerTrail.add({
            message: 'Recycler channel is overflowed, waiting'
          }).toString());
        }
        return sandbox.sendable;
      });
    }
    return engine.openSession(sandbox).then(_doWith);
  }

  var garbageAction = {};

  var discardGarbage = garbageAction['discard'] = function(sandbox) {
    if (!sandbox.garbage) return Promise.resolve(false);
    return engine.openSession(sandbox).then(function(ch) {
      LX.isEnabledFor('conlog') && LX.log('conlog', recyclerTrail.add({
        message: 'discardGarbage() - nack()'
      }).toString());
      ch.nack(sandbox.garbage, false, false);
      sandbox.garbage = undefined;
      return true;
    });
  }

  var restoreGarbage = garbageAction['restore'] = function(sandbox) {
    if (!sandbox.garbage) return Promise.resolve(false);
    return engine.openSession(sandbox).then(function(ch) {
      LX.isEnabledFor('conlog') && LX.log('conlog', recyclerTrail.add({
        message: 'restoreGarbage() - nack()'
      }).toString());
      ch.nack(sandbox.garbage);
      sandbox.garbage = undefined;
      return true;
    });
  }

  var recoverGarbage = garbageAction['recover'] = function(sandbox) {
    if (!sandbox.garbage || !subscriberConfig) return Promise.resolve(false);
    return engine.openSession(sandbox).then(function(ch) {
      var msg = sandbox.garbage;
      var ok = sendToQueue(msg.content, msg.properties, subscriberConfig, sandbox);
      return ok.then(function() {
        LX.isEnabledFor('conlog') && LX.log('conlog', recyclerTrail.add({
          message: 'recoverGarbage() - ack()'
        }).toString());
        ch.ack(sandbox.garbage);
        sandbox.garbage = undefined;
        return true;
      });
    });
  }

  var requeueGarbage = garbageAction['requeue'] = function(sandbox) {
    if (!sandbox.garbage) return Promise.resolve(false);
    return engine.openSession(sandbox).then(function(ch) {
      var msg = sandbox.garbage;
      var ok = sendToQueue(msg.content, msg.properties, recyclebinConfig, sandbox);
      return ok.then(function() {
        LX.isEnabledFor('conlog') && LX.log('conlog', recyclerTrail.add({
          message: 'requeueGarbage() - ack()'
        }).toString());
        ch.ack(sandbox.garbage);
        sandbox.garbage = undefined;
        return true;
      });
    });
  }

  Object.defineProperty(this, 'subscriberName', {
    get: function() { return subscriberConfig && subscriberConfig.queueName; },
    set: function(value) {}
  });

  Object.defineProperty(this, 'recyclebinName', {
    get: function() { return recyclebinConfig && recyclebinConfig.queueName; },
    set: function(value) {}
  });

  if (params.autoinit !== false) {
    misc.notifyConstructor(self.ready(), self);
  }

  LX.isEnabledFor('info') && LX.log('info', recyclerTrail.add({
    message: 'Recycler.new() end!'
  }).toString());
}

util.inherits(Recycler, events.EventEmitter);

module.exports = Recycler;
