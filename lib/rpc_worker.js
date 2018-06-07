'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var Engine = require('./engine');
var Executor = require('./executor');
var errors = require('./exception');
var misc = require('./util');
var LogTracer = require('logolite').LogTracer;
var LogAdapter = require('logolite').LogAdapter;
var LW = LogAdapter.getLogger({ scope: 'opflow:rpc:worker' });

var RpcWorker = function(params) {
  var self = this;
  events.EventEmitter.call(this);

  params = lodash.defaults({mode: 'rpc_worker'}, params, {engineId: misc.getLogID()});
  var rpcWorkerTrail = LogTracer.ROOT.branch({key:'rpcWorkerId',value:params.engineId});

  LW.isEnabledFor('info') && LW.log('info', rpcWorkerTrail.toMessage({
    text: 'RpcWorker.new()'
  }));

  var engine = new Engine(params);
  var executor = new Executor({ engine: engine });
  
  var operatorName = null;
  if (lodash.isString(params.operatorName)) {
    operatorName = params.operatorName;
  }
  LW.isEnabledFor('debug') && LW.log('debug', rpcWorkerTrail.add({
    queueName: operatorName
  }).toMessage({
    text: 'operatorName'
  }));

  var responseName = null;
  if (lodash.isString(params.responseName)) {
    responseName = params.responseName;
  }
  LW.isEnabledFor('debug') && LW.log('debug', rpcWorkerTrail.add({
    queueName: responseName
  }).toMessage({
    text: 'responseName'
  }));

  if (operatorName != null && responseName != null && operatorName == responseName) {
    throw new errors.BootstrapError("operatorName should be different with responseName");
  }

  var middlewares = [];
  var consumerRefs = [];
  var consumerRef = null;

  this.ready = function() {
    var readyTrail = rpcWorkerTrail.copy();
    LW.isEnabledFor('info') && LW.log('info', readyTrail.toMessage({
      text: 'ready() is invoked'
    }));
    var asyncFuns = [engine.ready()];
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
      LW.isEnabledFor('info') && LW.log('info', readyTrail.toMessage({
        text: 'ready() has been done'
      }));
      return result;
    });
  }

  this.process = function(checker, callback) {
    var processTrail = rpcWorkerTrail.copy();
    if (typeof(checker) === 'string') {
      middlewares.push({ checker: function(routineId) {
        return checker == routineId;
      }, listener: callback });
      LW.isEnabledFor('debug') && LW.log('debug', processTrail.add({
        filter: checker
      }).toMessage({
        text: 'process() - filter is a string'
      }));
    } else if (checker instanceof Array) {
      middlewares.push({ checker: function(routineId) {
        return checker.indexOf(routineId) >= 0;
      }, listener: callback });
      LW.isEnabledFor('debug') && LW.log('debug', processTrail.add({
        filter: checker
      }).toMessage({
        text: 'process() - filter is an array'
      }));
    } else if (typeof(checker) === 'function') {
      middlewares.push({ checker: checker, listener: callback });
      LW.isEnabledFor('debug') && LW.log('debug', processTrail.toMessage({
        text: 'process() - filter is a function'
      }));
    }

    var options = {
      binding: true,
      noAck: true,
      queueName: operatorName,
      replyToName: responseName
    };

    return consumerRef = consumerRef || engine.consume(function(msg, sandbox, done) {
      var consumeTrail = (sandbox.id) ? rpcWorkerTrail.branch({
        key: 'consumerId', value: sandbox.id
      }) : rpcWorkerTrail.copy();
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
      var requestTrail = consumeTrail.branch({ key:'requestId', value:requestId });

      LW.isEnabledFor('info') && LW.log('info', requestTrail.add({
        routineId: routineId
      }).toMessage({
        text: 'process() receives a message'
      }));

      var result = Promise.reduce(middlewares, function(A, middleware, index) {
        LW.isEnabledFor('conlog') && LW.log('conlog', requestTrail.add({
          index: index
        }).toMessage({
          text: 'process() checks middleware'
        }));
        if (A.count > 0 && (A.nextAction == null || A.nextAction == 'done')) return A;
        if (middleware.checker(routineId)) {
          LW.isEnabledFor('conlog') && LW.log('conlog', requestTrail.add({
            index: index
          }).toMessage({
            text: 'process() matches middleware'
          }));
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
        LW.isEnabledFor('info') && LW.log('info', requestTrail.add({
          extra: info
        }).toMessage({
          text: 'process() turn back with success'
        }));
        done(null, info);
      }).catch(function(exception) {
        LW.isEnabledFor('info') && LW.log('info', requestTrail.toMessage({
          text: 'process() turn back with failure'
        }));
        misc.isConsoleLogEnabled() && console.log(exception);
        done(exception);
      });
    }, options).then(function(consumerRef) {
      consumerRefs.push(consumerRef);
      return consumerRef;
    });
  };

  this.close = function() {
    var closeTrail = rpcWorkerTrail.copy();
    LW.isEnabledFor('info') && LW.log('info', closeTrail.toMessage({
      text: 'close() is invoked'
    }));
    return Promise.mapSeries(consumerRefs, function(consumerRef) {
      return engine.cancelConsumer(consumerRef);
    }).then(function() {
      while(consumerRefs.length > 0) consumerRefs.pop();
      LW.isEnabledFor('info') && LW.log('info', closeTrail.toMessage({
        text: 'close() has been finished'
      }));
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
    LW.isEnabledFor('debug') && LW.log('debug', rpcWorkerTrail.toMessage({
      text: 'auto execute ready()'
    }));
    misc.notifyConstructor(self.ready(), self);
  }

  LW.isEnabledFor('info') && LW.log('info', rpcWorkerTrail.toMessage({
    text: 'RpcWorker.new() end!'
  }));
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

  var requestTrail = LogTracer.ROOT.branch({ key:'requestId', value:__requestId });

  LR.isEnabledFor('info') && LR.log('info', requestTrail.add({
    replyTo: __replyTo,
    consumerId: __consumerId,
    consumerTag: __consumerTag,
    progressEnabled: __progressEnabled
  }).toMessage({
    text: 'RpcResponse[{requestId}].new()'
  }));

  self.emitStarted = function(info) {
    info = info || {};
    return __sendToQueue(info, __buildProperties('started'));
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
    return __sendToQueue(result, __buildProperties('progress'));
  }

  self.emitFailed = function(error) {
    error = error || '';
    return __sendToQueue(error, __buildProperties('failed', true));
  }

  self.emitCompleted = function(value) {
    value = value || '';
    return __sendToQueue(value, __buildProperties('completed', true));
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
    // LR.isEnabledFor('debug') && LR.log('debug', requestTrail.add({
    //   properties: properties
    // }).toMessage({
    //   text: 'RpcResponse[{requestId}] - send message[{properties.headers.status}] to queue'
    // }));
    // return new Promise(function(onResolved, onRejected) {
    //   __channel.sendToQueue(__replyTo, misc.bufferify(body), properties, function(err, ok) {
    //     if (err) {
    //       onRejected(err);
    //     } else {
    //       onResolved(ok);
    //     }
    //   });
    // });
    __channel.sendToQueue(__replyTo, misc.bufferify(body), properties);
  }

  LR.isEnabledFor('info') && LR.log('info', requestTrail.toMessage({
    text: 'RpcResponse[{requestId}].new() end!'
  }));
}
