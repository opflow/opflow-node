'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var debugx = require('debug')('opflow:executor');
var Engine = require('./engine');
var errors = require('./exception');
var misc = require('./util');

var Executor = function(params) {
  var self = this;
  var engine = params.engine;
  if (!lodash.isObject(engine)) {
    throw new errors.BootstrapError();
  }

  self.assertQueue = function(config, context) {
    var selfManaged = !lodash.isObject(context);
    context = context || {};
    config = misc.defaultQueueParams(config);
    return engine.openSession(context).then(function(ch) {
      return assertQueue(ch, config);
    }).then(function(ok) {
      if (selfManaged) engine.closeSession(context);
      return ok;
    });
  }

  self.checkQueue = function(config, context) {
    var selfManaged = !lodash.isObject(context);
    context = context || {};
    config = misc.defaultQueueParams(config);
    if (!config.queueName) return Promise.reject({
      msg: 'queueName should not be null'
    });
    return engine.openSession(context).then(function(ch) {
      return Promise.promisify(ch.checkQueue, {context: ch})(config.queueName);
    }).then(function(ok) {
      if (selfManaged) engine.closeSession(context);
      return ok;
    });
  }

  self.purgeQueue = function(config, context) {
    var selfManaged = !lodash.isObject(context);
    context = context || {};
    config = misc.defaultQueueParams(config);
    if (!config.queueName) return Promise.reject({
      msg: 'queueName should not be null'
    });
    return engine.openSession(context).then(function(ch) {
      return assertQueue(ch, config).then(function() {
        return Promise.promisify(ch.purgeQueue, {context: ch})(config.queueName);
      });
    }).then(function(ok) {
      if (selfManaged) engine.closeSession(context);
      return ok;
    });
  }

  self.sendToQueue = function(body, props, config, context) {
    var selfManaged = !lodash.isObject(context);
    context = context || {};
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
        if (context.sendable !== false) {
          context.sendable =_sendTo(ch);
          debugx.enabled && debugx('%s() channel is writable, msg has been sent', config.name);
        } else {
          ch.once('drain', function() {
            context.sendable =_sendTo(ch);
            debugx.enabled && debugx('%s() channel is drained, flushed', config.name);
          });
          debugx.enabled && debugx('%s() channel is overflowed, waiting', config.name);
        }
        return context.sendable;
      });
    }
    return engine.openSession(context).then(function(ch) {
      if (selfManaged) {
        return _sendTo(ch);
      } else {
        return _doWith(ch);
      }
    }).then(function(ok) {
      if (selfManaged) engine.closeSession(context);
      return ok;
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

module.exports = Executor;
