'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var Engine = require('./engine');
var errors = require('./exception');
var misc = require('./util');
var LogTracer = require('logolite').LogTracer;
var LogAdapter = require('logolite').LogAdapter;
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

  self.assertExchange = function(config, context) {
    config = misc.defaultQueueParams(config);
    return engine.obtainSandbox(function(sandbox) {
      return assertExchange(config, sandbox);
    }, context);
  }

  self.assertQueue = function(config, context) {
    config = misc.defaultQueueParams(config);
    return engine.obtainSandbox(function(sandbox) {
      return assertQueue(config, sandbox);
    }, context);
  }

  self.checkQueue = function(config, context) {
    config = misc.defaultQueueParams(config);
    if (!config.queueName) return Promise.reject({
      msg: 'queueName should not be null'
    });
    return engine.obtainSandbox(function(sandbox) {
      var ch = sandbox.channel;
      return Promise.promisify(ch.checkQueue, {context: ch})(config.queueName);
    }, context);
  }

  self.purgeQueue = function(config, context) {
    config = misc.defaultQueueParams(config);
    if (!config.queueName) return Promise.reject({
      msg: 'queueName should not be null'
    });
    return engine.obtainSandbox(function(sandbox) {
      return assertQueue(config, sandbox).then(function() {
        var ch = sandbox.channel;
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
    var _sendTo = function(sandbox) {
      return sandbox.channel.sendToQueue(config.queueName, misc.bufferify(body), props);
    }
    var _doWith = function(sandbox) {
      return assertQueue(config, sandbox).then(function() {
        if (context.sendable !== false) {
          context.sendable = _sendTo(sandbox);
          LX.isEnabledFor('conlog') && LX.log('conlog', executorTrail.add({
            message: 'Executor channel is writable, msg has been sent'
          }).toString());
        } else {
          sandbox.channel.once('drain', function() {
            context.sendable = _sendTo(sandbox);
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
    return engine.obtainSandbox(function(sandbox) {
      if (selfManaged) {
        return _sendTo(sandbox);
      } else {
        return _doWith(sandbox);
      }
    }, context);
  }

  var assertExchange = function(config, sandbox) {
    config = config || {};
    if (!config.exchangeName) return Promise.resolve();
    sandbox = sandbox || {};
    if (sandbox.exchangeAsserted) return Promise.resolve();
    var ch = sandbox.channel;
    var ch_assertExchange = Promise.promisify(ch.assertExchange, {context: ch});
    return ch_assertExchange(config.exchangeName, config.exchangeType, {
      durable: config.durable,
      autoDelete: config.autoDelete
    }).then(function(eok) {
      sandbox.exchangeAsserted = true;
      return eok;
    });
  }

  var assertQueue = function(config, sandbox) {
    sandbox = sandbox || {};
    if (sandbox.queueAsserted) return Promise.resolve();
    var ch = sandbox.channel;
    var ch_assertQueue = Promise.promisify(ch.assertQueue, {context: ch});
    var qp;
    if (config.queueName) {
      qp = ch_assertQueue(config.queueName, {
        durable: config.durable,
        exclusive: config.exclusive,
        autoDelete: config.autoDelete
      }).then(function(qok) {
        sandbox.fixedQueue = true;
        sandbox.queueName = config.queueName;
        sandbox.queueAsserted = true;
        return qok;
      });
    } else {
      qp = ch_assertQueue(null, {
        durable: false, exclusive: true, autoDelete: true
      }).then(function(qok) {
        sandbox.fixedQueue = false;
        sandbox.queueName = config.queueName = qok.queue;
        sandbox.queueAsserted = true;
        return qok;
      });
    }
    return qp;
  }

  LX.isEnabledFor('info') && LX.log('info', executorTrail.add({
    message: 'Executor.new() end!'
  }).toString());
}

module.exports = Executor;
