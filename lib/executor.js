'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var Engine = require('./engine');
var errors = require('./exception');
var misc = require('./util');
var LogTracer = require('./log_tracer');
var LogAdapter = require('./log_adapter');
var LX = LogAdapter.getLogger({ scope: 'opflow:executor' });

var Executor = function(params) {
  params = params || {};
  var self = this;
  var engine = params.engine;
  if (!lodash.isObject(engine)) {
    throw new errors.BootstrapError();
  }
  var executorTrail = LogTracer.ROOT.branch({key:'executorId',value:engine.engineId});

  LX.isEnabledFor('info') && LX.log('info', executorTrail.add({
    message: 'Executor.new()'
  }).toString());

  self.assertQueue = function(config, context) {
    config = misc.defaultQueueParams(config);
    return acquireChannel(function(ch) {
      return assertQueue(ch, config);
    }, context);
  }

  self.checkQueue = function(config, context) {
    config = misc.defaultQueueParams(config);
    if (!config.queueName) return Promise.reject({
      msg: 'queueName should not be null'
    });
    return acquireChannel(function(ch) {
      return Promise.promisify(ch.checkQueue, {context: ch})(config.queueName);
    }, context);
  }

  self.purgeQueue = function(config, context) {
    config = misc.defaultQueueParams(config);
    if (!config.queueName) return Promise.reject({
      msg: 'queueName should not be null'
    });
    return acquireChannel(function(ch) {
      return assertQueue(ch, config).then(function() {
        return Promise.promisify(ch.purgeQueue, {context: ch})(config.queueName);
      });
    }, context);
  }

  self.sendToQueue = function(body, props, config, context) {
    var selfManaged = !lodash.isObject(context);
    config = misc.defaultQueueParams(config);
    if (!config.queueName) return Promise.reject({
      msg: 'queueName should not be null'
    });
    props = props || {};
    LX.isEnabledFor('conlog') && LX.log('conlog', executorTrail.add({
      message: 'Executor send an object to queue'
    }).toString());
    var _sendTo = function(ch) {
      return ch.sendToQueue(config.queueName, misc.bufferify(body), props);
    }
    var _doWith = function(ch) {
      return assertQueue(ch, config).then(function() {
        if (context.sendable !== false) {
          context.sendable =_sendTo(ch);
          LX.isEnabledFor('conlog') && LX.log('conlog', executorTrail.add({
            message: 'Executor channel is writable, msg has been sent'
          }).toString());
        } else {
          ch.once('drain', function() {
            context.sendable =_sendTo(ch);
            LX.isEnabledFor('conlog') && LX.log('conlog', executorTrail.add({
              message: 'Executor channel is drained, flushed'
            }).toString());
          });
          LX.isEnabledFor('conlog') && LX.log('conlog', executorTrail.add({
            message: 'Executor channel is overflowed, waiting'
          }).toString());
        }
        return context.sendable;
      });
    }
    return acquireChannel(function(ch) {
      if (selfManaged) {
        return _sendTo(ch);
      } else {
        return _doWith(ch);
      }
    }, context);
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

  var acquireChannel = function(callback, sandbox) {
    if (!lodash.isFunction(callback)) return Promise.resolve();
    var selfManaged = !lodash.isObject(sandbox);
    sandbox = sandbox || {};
    var p = engine.openSession(sandbox).then(callback);
    if (selfManaged) p = p.then(function(ok) {
      return engine.closeSession(sandbox).then(lodash.wrap(ok));
    }).catch(function(err) {
      return engine.closeSession(sandbox).then(lodash.wrap(Promise.reject(err)));
    });
    return p;
  }

  LX.isEnabledFor('info') && LX.log('info', executorTrail.add({
    message: 'Executor.new() end!'
  }).toString());
}

module.exports = Executor;
