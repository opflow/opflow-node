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
var LW = LogAdapter.getLogger({ scope: 'opflow:rpc:worker' });

var RpcWorker = function(params) {
  var self = this;
  events.EventEmitter.call(this);

  params = lodash.defaults({mode: 'rpc_worker'}, params, {engineId: misc.getUUID()});
  var logTracer = LogTracer.ROOT.branch({key:'rpcWorkerId',value:params.engineId});

  LW.isEnabledFor('info') && LW.log('info', logTracer.add({
    message: 'RpcWorker.new()'
  }).toString());

  var engine = new Engine(params);
  var executor = new Executor({ engine: engine });
  
  var operatorName = null;
  if (lodash.isString(params.operatorName)) {
    operatorName = params.operatorName;
  }
  LW.isEnabledFor('debug') && LW.log('debug', logTracer.add({
    message: 'operatorName',
    queueName: operatorName
  }).toString());

  var responseName = null;
  if (lodash.isString(params.responseName)) {
    responseName = params.responseName;
  }
  LW.isEnabledFor('debug') && LW.log('debug', logTracer.add({
    message: 'responseName',
    queueName: responseName
  }).toString());

  if (operatorName != null && responseName != null && operatorName == responseName) {
    throw new errors.BootstrapError("operatorName should be different with responseName");
  }

  var middlewares = [];
  var consumerRefs = [];
  var consumerRef = null;

  this.ready = function() {
    var logReady = logTracer.copy();
    LW.isEnabledFor('info') && LW.log('info', logReady.add({
      message: 'ready() is invoked'
    }).toString());
    var asyncFuns = [];
    if (operatorName) {
      asyncFuns.push(executor.assertQueue({
        queueName: operatorName
      }));
    }
    if (responseName) {
      asyncFuns.push(executor.assertQueue({
        queueName: responseName
      }));
    }
    return Promise.all(asyncFuns).then(function(result) {
      LW.isEnabledFor('info') && LW.log('info', logReady.add({
        message: 'ready() has been done'
      }).toString());
      return result;
    });
  }

  this.process = function(checker, callback) {
    var logProcess = logTracer.copy();
    if (typeof(checker) === 'string') {
      middlewares.push({ checker: function(routineId) {
        return checker == routineId;
      }, listener: callback });
      LW.isEnabledFor('debug') && LW.log('debug', logProcess.add({
        message: 'process() - filter is a string',
        filter: checker
      }).toString());
    } else if (checker instanceof Array) {
      middlewares.push({ checker: function(routineId) {
        return checker.indexOf(routineId) >= 0;
      }, listener: callback });
      LW.isEnabledFor('debug') && LW.log('debug', logProcess.add({
        message: 'process() - filter is an array',
        filter: checker
      }).toString());
    } else if (typeof(checker) === 'function') {
      middlewares.push({ checker: checker, listener: callback });
      LW.isEnabledFor('debug') && LW.log('debug', logProcess.add({
        message: 'process() - filter is a function'
      }).toString());
    }

    var options = {
      binding: true,
      noAck: true,
      queueName: operatorName,
      replyToName: responseName
    };

    return consumerRef = consumerRef || engine.consume(function(msg, sandbox, done) {
      var logConsume = (sandbox.id) ? logTracer.branch({
        key: 'consumerId', value: sandbox.id
      }) : logTracer.copy();
      var response = new RpcResponse({
        body: msg.content,
        properties: msg.properties,
        channel: sandbox.channel,
        consumerId: sandbox.id,
        consumerTag: sandbox.consumerTag,
        replyToName: sandbox.replyToName
      });
      var headers = msg && msg.properties && msg.properties.headers;
      var routineId = misc.getRoutineId(headers);
      var requestId = misc.getRequestId(headers);
      var logRequest = logConsume.branch({ key:'requestId', value:requestId });

      LW.isEnabledFor('info') && LW.log('info', logRequest.add({
        message: 'process() receives a message',
        routineId: routineId
      }).toString({reset:true}));

      var result = Promise.reduce(middlewares, function(A, middleware, index) {
        LW.isEnabledFor('conlog') && LW.log('conlog', logRequest.add({
          message: 'process() checks middleware',
          index: index
        }).toString());
        if (A.count > 0 && (A.nextAction == null || A.nextAction == 'done')) return A;
        if (middleware.checker(routineId)) {
          LW.isEnabledFor('conlog') && LW.log('conlog', logRequest.add({
            message: 'process() matches middleware',
            index: index
          }).toString());
          A.count += 1;
          var ok = Promise.resolve(middleware.listener(msg.content, headers, response));
          return ok.then(function(nextAction) {
            A.nextAction = nextAction;
            return A;
          })
        }
        return A;
      }, { count: 0, nextAction: null });

      result.then(function(info) {
        LW.isEnabledFor('info') && LW.log('info', logRequest.add({
          message: 'process() turn back with success',
          extra: info
        }).toString({reset:true}));
        done(null, info);
      }).catch(function(exception) {
        LW.isEnabledFor('info') && LW.log('info', logRequest.add({
          message: 'process() turn back with failure'
        }).toString());
        LW.isEnabledFor('conlog') && console.log(exception);
        done(exception);
      });
    }, options).then(function(consumerRef) {
      consumerRefs.push(consumerRef);
      return consumerRef;
    });
  };

  this.close = function() {
    var logClose = logTracer.copy();
    LW.isEnabledFor('info') && LW.log('info', logClose.add({
      message: 'close() is invoked'
    }).toString());
    return Promise.mapSeries(consumerRefs, function(consumerRef) {
      return engine.cancelConsumer(consumerRef);
    }).then(function() {
      while(consumerRefs.length > 0) consumerRefs.pop();
      LW.isEnabledFor('info') && LW.log('info', logClose.add({
        message: 'close() has been finished'
      }).toString());
      return engine.close();
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

  if (params.autoinit !== false) {
    LW.isEnabledFor('debug') && LW.log('debug', logTracer.add({
      message: 'auto execute ready()'
    }).toString());
    misc.notifyConstructor(self.ready(), self);
  }

  LW.isEnabledFor('info') && LW.log('info', logTracer.add({
    message: 'RpcWorker.new() end!'
  }).toString());
}

util.inherits(RpcWorker, events.EventEmitter);

module.exports = RpcWorker;

var LR = LogAdapter.getLogger({ scope: 'opflow:rpc:response' });

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

  var __replyTo = __properties.replyTo || params.replyToName;
  if (!__replyTo) {
    throw new errors.BootstrapError();
  }

  var __consumerId = params.consumerId;

  var __consumerTag = params.consumerTag;

  var __requestId = misc.getRequestId(__properties.headers, false);

  var __progressEnabled = misc.getHeaderField(__properties.headers, 'progressEnabled', false);

  var logRequest = LogTracer.ROOT.branch({ key:'requestId', value:__requestId });

  LR.isEnabledFor('info') && LR.log('info', logRequest.add({
    message: 'RpcResponse.new()',
    replyTo: __replyTo,
    consumerId: __consumerId,
    consumerTag: __consumerTag,
    progressEnabled: __progressEnabled
  }).toString());

  self.emitStarted = function(info) {
    info = info || {};
    __sendToQueue(info, __buildProperties('started'));
  }

  self.emitProgress = function(completed, total, extra) {
    if (__progressEnabled === false) return;
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
      headers['workerTag'] = __consumerTag;
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

  LR.isEnabledFor('info') && LR.log('info', logRequest.add({
    message: 'RpcResponse.new() end!'
  }).toString());
}