'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var debugx = require('debug')('opflow:worker');
var Engine = require('./engine');
var Executor = require('./executor');
var misc = require('./util');

var RpcWorker = function(params) {
  debugx.enabled && debugx(' + constructor begin ...');

  params = params || {};
  var self = this;
  self.logger = self.logger || params.logger;
  
  params = lodash.defaults({mode: 'worker'}, params);
  var engine = new Engine(params);
  var executor = new Executor({ engine: engine });
  
  var operatorName = null;
  if (lodash.isString(params.operatorName)) {
    operatorName = params.operatorName;
    executor.assertQueue({
      queueName: operatorName
    });
  }
  debugx.enabled && debugx('operatorName: %s', operatorName);

  var responseName = null;
  if (lodash.isString(params.responseName)) {
    responseName = params.responseName;
    executor.assertQueue({
      queueName: responseName
    });
  }
  debugx.enabled && debugx('responseName: %s', responseName);

  var middlewares = [];
  var consumerRef = null;

  this.process = function(checker, callback) {
    if (typeof(checker) === 'string') {
      middlewares.push({ checker: function(routineId) {
        return checker == routineId;
      }, listener: callback });
      debugx.enabled && debugx('process() - filter is a string: %s', checker);
    }

    if (checker instanceof Array) {
      middlewares.push({ checker: function(routineId) {
        return checker.indexOf(routineId) >= 0;
      }, listener: callback });
    }

    if (typeof(checker) === 'function') {
      middlewares.push({ checker: checker, listener: callback });
    }

    var options = {
      binding: true,
      noAck: true,
      queueName: operatorName,
      replyToName: responseName
    };

    return consumerRef = consumerRef || engine.consume(function(msg, sandbox, done) {
      var response = new RpcResponse({
        body: msg.content,
        properties: msg.properties,
        channel: sandbox.channel,
        workerTag: sandbox.consumerTag,
        replyToName: sandbox.replyToName
      });
      var headers = msg && msg.properties && msg.properties.headers;
      var routineId = misc.getRoutineId(headers);
      var requestId = misc.getRequestId(headers);
      debugx.enabled && debugx('Request[%s]/Routine[%s] - process() receive a message', 
          requestId, routineId);
      var count = 0;
      for(var i=0; i<middlewares.length; i++) {
        var middleware = middlewares[i];
        if (middleware.checker(routineId)) {
          count += 1;
          var nextAction = middleware.listener(msg.content, headers, response);
          if (nextAction == null || nextAction == 'done') break;
        }
      }
      return count > 0;
    }, options);
  };

  this.destroy = function() {
    Promise.resolve().then(function() {
      if (!consumerRef) return false;
      return consumerRef.then(function(consumerInfo) {
        return engine.cancelConsumer(consumerInfo);
      });
    }).then(function() {
      return engine.destroy();
    });
  }

  Object.defineProperty(this, 'executor', {
    get: function() { return executor; },
    set: function(value) {}
  });

  Object.defineProperty(this, 'operatorName', {
    get: function() { return operatorName; },
    set: function(value) {}
  });

  Object.defineProperty(this, 'responseName', {
    get: function() { return responseName; },
    set: function(value) {}
  });

  debugx.enabled && debugx(' - constructor end!');
}

util.inherits(RpcWorker, events.EventEmitter);

module.exports = RpcWorker;

function RpcResponse(params) {
  params = params || {};
  var self = this;

  var __channel = params.channel;
  if (!__channel) {
    throw new errors.BootstrapError();
  }

  var __properties = params.properties;
  if (!__properties) {
    throw new errors.BootstrapError();
  }

  var __workerTag = params.workerTag;
  if (!__workerTag) {
    throw new errors.BootstrapError();
  }

  var __replyTo = __properties.replyTo || params.replyToName;
  if (!__replyTo) {
    throw new errors.BootstrapError();
  }

  var __requestId = misc.getRequestId(__properties.headers, false);

  self.emitStarted = function(info) {
    info = info || {};
    __sendToQueue(info, __buildProperties('started'));
  }

  self.emitProgress = function(completed, total, extra) {
    total = total || 100;
    var percent = -1;
    if (lodash.isNumber(total) && total > 0 &&
        lodash.isNumber(completed) && completed >= 0 &&
        completed <= total) {
      percent = (total === 100) ? completed : lodash.round((completed * 100) / total);
    }
    var result = { percent: percent, data: extra };
    __sendToQueue(result, __buildProperties('progress'));
  }

  self.emitFailed = function(error) {
    error = error || '';
    __sendToQueue(error, __buildProperties('failed', true));
  }

  self.emitCompleted = function(value) {
    value = value || '';
    __sendToQueue(value, __buildProperties('completed', true));
  }

  var __buildHeaders = function(status, isFinished) {
    var headers = { status: status }
    if (__requestId) {
      headers['requestId'] = __requestId;
    }
    if (isFinished === true) {
      headers['workerTag'] = __workerTag;
    }
    return headers;
  }

  var __buildProperties = function(status, isFinished) {
    var properties = {
      appId: __properties.appId,
      correlationId: __properties.correlationId,
      headers: __buildHeaders(status, isFinished)
    };
    return properties;
  }

  var __sendToQueue = function(body, properties) {
    __channel.sendToQueue(__replyTo, misc.bufferify(body), properties);
  }
}