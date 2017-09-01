'use strict';

var assert = require('assert');
var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var debugx = require('debug')('opflow:recycler');
var Engine = require('./engine');
var Executor = require('./executor');
var misc = require('./misc');

var Recycler = function(params) {
  events.EventEmitter.call(this);

  debugx.enabled && debugx(' + constructor begin ...');

  params = params || {};
  var self = this;
  self.logger = self.logger || params.logger;

  var engine = new Engine(params);
  var executor = new Executor({ engine: engine });
  var session = {};

  var subscriberConfig = null;
  if (lodash.isString(params.subscriberName)) {
    subscriberConfig = misc.defaultQueueParams({
      queueName: params.subscriberName
    });
    executor.assertQueue(subscriberConfig);
  } else {
    throw new errors.ParameterError();
  }

  var recyclebinConfig = null;
  if (lodash.isString(params.recyclebinName)) {
    recyclebinConfig = misc.defaultQueueParams({
      queueName: params.recyclebinName
    });
    executor.assertQueue(recyclebinConfig);
  } else {
    throw new errors.ParameterError();
  }

  self.recycle = function(callback) {
    assert.ok(lodash.isFunction(callback), 'callback should be a function');

    var options = {
      manualAck: true,
      noAck: false,
      queueName: recyclebinConfig.queueName
    }
    
    return engine.consume(function(msg, sandbox, done) {
      var headers = msg && msg.properties && msg.properties.headers;
      var requestId = misc.getRequestId(headers);
      var ch = sandbox.channel;
      debugx.enabled && debugx('Request[%s] - recycle() get message from Recyclebin', requestId);
      callback(msg.content, lodash.pick(msg, ['fields', 'properties']), function(err) {
        err ? ch.nack(msg) : ch.ack(msg);
      });
    }, options);
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
    return engine.openSession(session).then(function(ch) {
      return Promise.promisify(ch.checkQueue, {context: ch})(subscriberConfig.queueName);
    });
  }

  this.purgeSubscriber = function() {
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
      return engine.destroy();
    }).then(function() {
      engine = null;
      return true;
    });
  }

  var examineGarbage = function(sandbox) {
    if (sandbox.garbage) return Promise.resolve(sandbox.garbage);
    return engine.openSession(session).then(function(ch) {
      return assertQueue(ch, recyclebinConfig).then(function(qok) {
        return Promise.promisify(ch.get, {context: ch})(qok.queue, {});
      }).then(function(msgOrFalse) {
        debugx.enabled && debugx('examineGarbage() - msg: %s', JSON.stringify(msgOrFalse));
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
    debugx.enabled && debugx('%s() an object to queue', config.name);
    var _sendTo = function(ch) {
      return ch.sendToQueue(config.queueName, misc.bufferify(body), props);
    }
    var _doWith = function(ch) {
      return assertQueue(ch, config).then(function() {
        if (sandbox.sendable !== false) {
          sandbox.sendable =_sendTo(ch);
          debugx.enabled && debugx('%s() channel is writable, msg has been sent', self.name);
        } else {
          ch.once('drain', function() {
            sandbox.sendable =_sendTo(ch);
            debugx.enabled && debugx('%s() channel is drained, flushed', self.name);
          });
          debugx.enabled && debugx('%s() channel is overflowed, waiting', self.name);
        }
        return sandbox.sendable;
      });
    }
    return engine.openSession(session).then(_doWith);
  }

  var garbageAction = {};

  var discardGarbage = garbageAction['discard'] = function(sandbox) {
    if (!sandbox.garbage) return Promise.resolve(false);
    return engine.openSession(session).then(function(ch) {
      debugx.enabled && debugx('discardGarbage() - nack()');
      ch.nack(sandbox.garbage, false, false);
      sandbox.garbage = undefined;
      return true;
    });
  }

  var restoreGarbage = garbageAction['restore'] = function(sandbox) {
    if (!sandbox.garbage) return Promise.resolve(false);
    return engine.openSession(session).then(function(ch) {
      debugx.enabled && debugx('restoreGarbage() - nack()');
      ch.nack(sandbox.garbage);
      sandbox.garbage = undefined;
      return true;
    });
  }

  var recoverGarbage = garbageAction['recover'] = function(sandbox) {
    if (!sandbox.garbage) return Promise.resolve(false);
    return engine.openSession(session).then(function(ch) {
      var msg = sandbox.garbage;
      var ok = sendToQueue(msg.content, msg.properties, subscriberConfig, sandbox);
      return ok.then(function() {
        debugx.enabled && debugx('recoverGarbage() - ACK');
        ch.ack(sandbox.garbage);
        sandbox.garbage = undefined;
        return true;
      });
    });
  }

  var requeueGarbage = garbageAction['requeue'] = function(sandbox) {
    if (!sandbox.garbage) return Promise.resolve(false);
    return engine.openSession(session).then(function(ch) {
      var msg = sandbox.garbage;
      var ok = sendToQueue(msg.content, msg.properties, recyclebinConfig, sandbox);
      return ok.then(function() {
        debugx.enabled && debugx('requeueGarbage() - ACK');
        ch.ack(sandbox.garbage);
        sandbox.garbage = undefined;
        return true;
      });
    });
  }

  this.destroy = engine.destroy.bind(engine);

  Object.defineProperty(this, 'executor', {
    get: function() { return executor; },
    set: function(value) {}
  });

  Object.defineProperty(this, 'subscriberName', {
    get: function() { return subscriberConfig.queueName; },
    set: function(value) {}
  });

  Object.defineProperty(this, 'recyclebinName', {
    get: function() { return recyclebinConfig.queueName; },
    set: function(value) {}
  });

  debugx.enabled && debugx(' - constructor end!');
}

util.inherits(Recycler, events.EventEmitter);

module.exports = Recycler;
