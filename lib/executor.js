'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var debugx = require('debug')('opflow:executor');
var Engine = require('./engine');
var errors = require('./exception');

var Executor = function(params) {
  var self = this;
  var engine = null;
  if (params.engine) {
    engine = params.engine;
  } else {
    throw new errors.BootstrapError();
  }

  self.assertQueue = function(config, context) {
    context = context || {};
    config = defaultQueueParams(config);
    var _doWith = function(ch) {
      return assertQueue(ch, config);
    }
    if (lodash.isObject(context.channel)) {
      return _doWith(context.channel);
    }
    return engine.acquireChannel(function(promise) {
      return promise.then(_doWith);
    });
  }

  self.checkQueue = function(config, context) {
    context = context || {};
    config = defaultQueueParams(config);
    if (!config.queueName) return Promise.reject({
      msg: 'queueName should not be null'
    });
    var _doWith = function(ch) {
      return Promise.promisify(ch.checkQueue, {context: ch})(config.queueName);
    }
    if (lodash.isObject(context.channel)) {
      return _doWith(context.channel);
    }
    return engine.acquireChannel(function(promise) {
      return promise.then(_doWith);
    });
  }

  self.purgeQueue = function(config, context) {
    context = context || {};
    config = defaultQueueParams(config);
    if (!config.queueName) return Promise.reject({
      msg: 'queueName should not be null'
    });
    var _doWith = function(ch) {
      return assertQueue(ch, config).then(function() {
        return Promise.promisify(ch.purgeQueue, {context: ch})(config.queueName);
      });
    }
    if (lodash.isObject(context.channel)) {
      return _doWith(context.channel);
    }
    return engine.acquireChannel(function(promise) {
      return promise.then(_doWith);
    });
  }

  self.sendToQueue = function(body, props, config, context) {
    context = context || {};
    config = defaultQueueParams(config);
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
        if (context.sendable !== false) {
          context.sendable =_sendTo(ch);
          debugx.enabled && debugx('%s() channel is writable, msg has been sent', self.name);
        } else {
          ch.once('drain', function() {
            context.sendable =_sendTo(ch);
            debugx.enabled && debugx('%s() channel is drained, flushed', self.name);
          });
          debugx.enabled && debugx('%s() channel is overflowed, waiting', self.name);
        }
        return context.sendable;
      });
    }
    if (lodash.isObject(context.channel)) {
      return _doWith(context.channel);
    }
    return engine.acquireChannel(function(promise) {
      return promise.then(_sendTo);
    });
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
}

var defaultQueueParams = function(config) {
  config = config || {};
  return lodash.defaults(config, {
    durable: true,
    exclusive: false,
    autoDelete: false
  });
}

module.exports = Executor;
