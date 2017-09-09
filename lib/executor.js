'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var Engine = require('./engine');
var errors = require('./exception');
var misc = require('./util');
var LogAdapter = require('./logadapter');
var L = LogAdapter.getLogger({ scope: 'opflow:executor' });

var Executor = function(params) {
  params = params || {};
  var self = this;
  var engine = params.engine;
  if (!lodash.isObject(engine)) {
    throw new errors.BootstrapError();
  }

  L.isEnabledFor('info') && L.log('info', {
    message: 'Executor.new()',
    engineId: engine.engineId });

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
    L.isEnabledFor('conlog') && L.log('conlog', {
      message: 'Executor send an object to queue' });
    var _sendTo = function(ch) {
      return ch.sendToQueue(config.queueName, misc.bufferify(body), props);
    }
    var _doWith = function(ch) {
      return assertQueue(ch, config).then(function() {
        if (context.sendable !== false) {
          context.sendable =_sendTo(ch);
          L.isEnabledFor('conlog') && L.log('conlog', {
            message: 'Executor channel is writable, msg has been sent' });
        } else {
          ch.once('drain', function() {
            context.sendable =_sendTo(ch);
            L.isEnabledFor('conlog') && L.log('conlog', {
              message: 'Executor channel is drained, flushed' });
          });
          L.isEnabledFor('conlog') && L.log('conlog', {
            message: 'Executor channel is overflowed, waiting' });
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
      }).then(function(qok) {
        config.queueName = config.queueName || qok.queue;
        return qok;
      });
    }
  }

  L.isEnabledFor('info') && L.log('info', {
    message: 'Executor.new() end!',
    engineId: engine.engineId });
}

module.exports = Executor;
