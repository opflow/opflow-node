'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var Engine = require('./engine');
var Executor = require('./executor');
var TimeoutHandler = require('./task').TimeoutHandler;
var misc = require('./util');
var LogTracer = require('logolite').LogTracer;
var LogAdapter = require('logolite').LogAdapter;
var LM = LogAdapter.getLogger({ scope: 'opflow:rpc:master' });

var RpcMaster = function(params) {
  var self = this;
  events.EventEmitter.call(this);

  params = lodash.defaults({ mode: 'rpc_master', confirmation: {enabled: false} },
    params, {engineId: misc.getLogID()});
  var rpcMasterTrail = LogTracer.ROOT.branch({key:'rpcMasterId',value:params.engineId});
  var store = { tasks: {} }

  LM.isEnabledFor('info') && LM.log('info', rpcMasterTrail.add({
    message: 'RpcMaster.new()'
  }).toString());

  var engine = new Engine(params);
  var executor = new Executor({ engine: engine });

  var responseName = null;
  if (lodash.isString(params.responseName)) {
    responseName = params.responseName;
  }

  var responseReady = true;
  if (lodash.isBoolean(params.responseReady)) {
    responseReady = params.responseReady;
  }

  var timeoutMonitor = null;
  var timeoutConfig = { monitorType: 'rpcResponse', handlerType: 'callback', tasks: store.tasks };

  timeoutConfig.monitorId = params.engineId;
  if (lodash.isString(params.monitorId)) {
    timeoutConfig.monitorId = params.monitorId;
  }

  timeoutConfig.interval = 1000;
  if (lodash.isNumber(params.monitorInterval)) {
    timeoutConfig.interval = params.monitorInterval;
  }

  timeoutConfig.timeout = 2000;
  if (lodash.isNumber(params.monitorTimeout)) {
    timeoutConfig.timeout = params.monitorTimeout;
  }

  var monitorEnabled = true;
  if (lodash.isBoolean(params.monitorEnabled)) {
    monitorEnabled = params.monitorEnabled;
  }

  var consumerRefs = [];
  var responseConsumer = null;

  this.ready = function() {
    LM.isEnabledFor('info') && LM.log('info', rpcMasterTrail.add({
      message: 'ready() is invoked'
    }).toString());
    return engine.ready().then(function() {
      var asyncFuns = [];
      if (responseName) {
        asyncFuns.push(executor.assertQueue({
          queueName: responseName
        }));
      }
      if (responseName && responseReady) {
        responseConsumer = responseConsumer || initResponseConsumer(false);
        asyncFuns.push(responseConsumer);
      }
      return Promise.all(asyncFuns).then(function(result) {
        LM.isEnabledFor('info') && LM.log('info', rpcMasterTrail.add({
          message: 'ready() has been done'
        }).toString());
        return result;
      });
    })
  }

  this.request = function(routineId, data, opts) {
    opts = opts || {};
    opts.requestId = opts.requestId || misc.getLogID();
    var requestTrail = rpcMasterTrail.branch({ key:'requestId', value:opts.requestId });

    LM.isEnabledFor('info') && LM.log('info', requestTrail.add({
      message: 'request() - make a request',
      routineId: routineId
    }).toString({reset:true}));
    
    if (monitorEnabled && !timeoutMonitor) {
      timeoutMonitor = new TimeoutHandler(timeoutConfig);
      timeoutMonitor.start();
    }

    var consumerInfo;
    var forked = opts.mode === 'forked';
    if (forked) {
      consumerInfo = initResponseConsumer(true);
    } else {
      responseConsumer = responseConsumer || initResponseConsumer(false);
      consumerInfo = responseConsumer;
    }

    var correlationId = misc.getLogID();

    if (typeof(routineId) === 'string') {
      opts.routineId = routineId;
    }

    var task = new RpcRequest(opts);
    store.tasks[correlationId] = task;

    var properties = {
      correlationId: correlationId,
      headers: {
        routineId: task.routineId,
        requestId: task.requestId,
        progressEnabled: opts.progressEnabled
      }
    };

    LM.isEnabledFor('debug') && LM.log('debug', requestTrail.add({
      message: 'request() - properties',
      properties: properties
    }).toString({reset:true}));

    return consumerInfo.then(function(sandbox) {
      if (!sandbox['fixedQueue']) {
        properties.replyTo = sandbox['queueName']
      }
      return engine.produce(data, properties);
    }).then(function() {
      LM.isEnabledFor('info') && LM.log('info', requestTrail.add({
        message: 'request() has been done successfully, return task object'
      }).toString());
      return store.tasks[correlationId];
    }).catch(function(error) {
      LM.isEnabledFor('info') && LM.log('info', requestTrail.add({
        message: 'request() - engine.produce() failed, reject with error',
        error: JSON.stringify(error)
      }).toString());
      return Promise.reject(error);
    });
  };

  var initResponseConsumer = function(forked) {
    var logInitResponse = rpcMasterTrail.copy();
    LM.isEnabledFor('info') && LM.log('info', logInitResponse.add({
      message: 'initResponseConsumer()',
      forked: forked
    }).toString());

    var options = { binding: false, prefetch: 1, noAck: true }
    if (!forked) {
      options['queueName'] = responseName;
      options['consumerLimit'] = 1;
      options['forceNewChannel'] = true;
    }

    return engine.consume(function(msg, sandbox, done) {
      sandbox = sandbox || {};
      var responseConsumerTrail = (sandbox.id) ? rpcMasterTrail.branch({
        key: 'responseConsumerId', value:sandbox.id
      }) : rpcMasterTrail.copy();

      var props = msg && msg.properties || {};
      var correlationId = props.correlationId;
      var task = store.tasks[correlationId];

      if (!correlationId || !task) {
        done();
        return;
      }

      var message = msg.content.toString();
      LM.isEnabledFor('conlog') && LM.log('conlog', responseConsumerTrail.add({
        message: 'ResponseConsumer - received a message',
        body: message,
        properties: props
      }).toString({reset:true}));

      try {
        message = JSON.parse(message);
      } catch(error) {
        LM.isEnabledFor('info') && LM.log('info', responseConsumerTrail.add({
          message: 'ResponseConsumer - JSON.parse() failed'
        }).toString());
        if (misc.isConsoleLogEnabled()) {
          console.log('ResponseConsumer - JSON.parse() failed', error);
        }
      }

      var status = props.headers && props.headers.status;
      switch(status) {
        case 'started':
        task.emit(mapping(status), message);
        break;

        case 'progress':
        task.emit(mapping(status), message.percent, message.data);
        break;

        case 'failed':
        case 'completed':
        task.emit(mapping(status), message);
        delete store.tasks[correlationId];
        LM.isEnabledFor('info') && LM.log('info', responseConsumerTrail.add({
          message: 'request() - receive final result',
          status: status
        }).toString({reset:true}));
        break;
      }
      done();
    }, options).then(function(consumerRef) {
      consumerRefs.push(consumerRef);
      return consumerRef;
    });
  }

  this.close = function() {
    var closeTrail = rpcMasterTrail.copy();
    LM.isEnabledFor('info') && LM.log('info', closeTrail.add({
      message: 'close() is invoked'
    }).toString());
    return Promise.resolve().then(function() {
      LM.isEnabledFor('info') && LM.log('info', closeTrail.add({
        message: 'close() - stop TimeoutHandler'
      }).toString());
      if (monitorEnabled && timeoutMonitor) {
        return timeoutMonitor.stop();
      }
      return true;
    }).then(function() {
      return Promise.mapSeries(consumerRefs, function(consumerInfo) {
        return engine.cancelConsumer(consumerInfo);
      })
    }).then(function() {
      responseConsumer = null;
      while(consumerRefs.length > 0) consumerRefs.pop();
      LM.isEnabledFor('info') && LM.log('info', closeTrail.add({
        message: 'close() has been finished'
      }).toString());
      return engine.close();
    });
  }

  Object.defineProperty(this, 'executor', {
    get: function() { return executor; },
    set: function(value) {}
  });

  Object.defineProperty(this, 'responseName', {
    get: function() { return responseName; },
    set: function(value) {}
  });

  if (params.autoinit !== false) {
    LM.isEnabledFor('debug') && LM.log('debug', rpcMasterTrail.add({
      message: 'auto execute ready()'
    }).toString());
    misc.notifyConstructor(self.ready(), self);
  }

  LM.isEnabledFor('info') && LM.log('info', rpcMasterTrail.add({
    message: 'RpcMaster.new() end!'
  }).toString());
}

util.inherits(RpcMaster, events.EventEmitter);

module.exports = RpcMaster;

var mapping = function(status) {
  return status;
}

var LR = LogAdapter.getLogger({ scope: 'opflow:rpc:request' });

var RpcRequest = function RpcRequest(kwargs) {
  var self = this;
  events.EventEmitter.call(this);

  kwargs = kwargs || {};
  var _requestId = kwargs.requestId || kwargs.requestID || misc.getLogID();
  var _routineId = kwargs.routineId || kwargs.routineID || misc.getLogID();
  var _timeout = lodash.isNumber(kwargs.timeout) && kwargs.timeout > 0 && kwargs.timeout || 0;
  var _timestamp = misc.getCurrentTime();

  var requestTrail = LogTracer.ROOT.branch({ key:'requestId', value:_requestId });

  LR.isEnabledFor('info') && LR.log('info', requestTrail.add({
    message: 'RpcRequest.new()',
    routineId: _routineId,
    timeout: _timeout,
    timestamp: _timestamp
  }).toString({reset:true}));

  Object.defineProperties(this, {
    'routineId': {
      get: function() { return _routineId },
      set: function(val) {}
    },
    'requestId': {
      get: function() { return _requestId },
      set: function(val) {}
    },
    'timeout': {
      get: function() { return _timeout },
      set: function(val) {}
    },
    'timestamp': {
      get: function() { return _timestamp },
      set: function(val) {}
    }
  });

  this.raiseTimeout = function(done) {
    var timeoutInfo = { message: 'The request has been timeout' };
    this.emit('timeout', timeoutInfo);
    LR.isEnabledFor('debug') && LR.log('debug', requestTrail.add({
      message: 'raiseTimeout()',
      routineId: _routineId,
      timeout: _timeout,
      timestamp: _timestamp
    }).toString({reset:true}));
    done && done();
  }

  this.extractResult = function() {
    var task = this;
    return new Promise(function(onResolved, onRejected) {
      var steps = [];
      task.on('started', function(info) {
        LR.isEnabledFor('conlog') && LR.log('conlog', requestTrail.add({
          message: 'Task is started'
        }).toString());
      }).on('progress', function(percent, data) {
        steps.push({ percent: percent, info: data });
        LR.isEnabledFor('conlog') && LR.log('conlog', requestTrail.add({
          message: 'Task in progress', percent: percent
        }).toString({reset:true}));
      }).on('timeout', function(info) {
        LR.isEnabledFor('conlog') && LR.log('conlog', requestTrail.add({
          message: 'Task is timeout'
        }).toString());
        onResolved(new RpcResult(task, { progress: steps, status: 'timeout' }));
      }).on('failed', function(error) {
        LR.isEnabledFor('conlog') && LR.log('conlog', requestTrail.add({
          message: 'Task is failed', error: error
        }).toString());
        onResolved(new RpcResult(task, { progress: steps, status: 'failed', data: error }));
      }).on('completed', function(result) {
        LR.isEnabledFor('conlog') && LR.log('conlog', requestTrail.add({
          message: 'Task is done', value: result
        }).toString());
        onResolved(new RpcResult(task, { progress: steps, status: 'completed', data: result }));
      });
    });
  }

  LR.isEnabledFor('info') && LR.log('info', requestTrail.add({
    message: 'RpcRequest.new() end!'
  }).toString());
}

util.inherits(RpcRequest, events.EventEmitter);


var LT = LogAdapter.getLogger({ scope: 'opflow:rpc:result' });

var RpcResult = function(task, info) {
  var self = this;
  task = task || {};
  info = info || {};

  var requestId = task.requestId || misc.getLogID();
  var requestTrail = LogTracer.ROOT.branch({ key:'requestId', value:requestId });

  LT.isEnabledFor('info') && LT.log('info', requestTrail.add({
    message: 'RpcResult.new()'
  }).toString());

  ['requestId', 'routineId', 'timestamp'].forEach(function(propName) {
    Object.defineProperty(self, propName, {
      get: function() { return task[propName] },
      set: function(val) {}
    });
  });

  Object.defineProperties(self, {
    'status': {
      get: function() { return info.status },
      set: function(val) {}
    },
    'data': {
      get: function() { return info.data },
      set: function(val) {}
    },
    'timeout': {
      get: function() { return info.status === 'timeout' },
      set: function(val) {}
    },
    'failed': {
      get: function() { return info.status === 'failed' },
      set: function(val) {}
    },
    'completed': {
      get: function() { return info.status === 'completed' },
      set: function(val) {}
    },
    'error': {
      get: function() { return info.data },
      set: function(val) {}
    },
    'value': {
      get: function() { return info.data },
      set: function(val) {}
    }
  });

  LT.isEnabledFor('info') && LT.log('info', requestTrail.add({
    message: 'RpcResult.new() end!'
  }).toString());
}
