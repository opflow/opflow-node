'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var Engine = require('./engine');
var Executor = require('./executor');
var TimeoutHandler = require('./task').TimeoutHandler;
var misc = require('./util');
var LogAdapter = require('./logadapter');
var LM = LogAdapter.getLogger({ scope: 'opflow:rpc:master' });

var RpcMaster = function(params) {
  events.EventEmitter.call(this);

  params = lodash.defaults({mode: 'rpc_master'}, params, {engineId: misc.getUUID()});
  var rpcMasterId = params.engineId;
  var self = this;
  var store = { tasks: {} }

  LM.isEnabledFor('info') && LM.log('info', {
    message: 'RpcMaster.new()',
    rpcMasterId: rpcMasterId,
    instanceId: misc.instanceId });

  var engine = new Engine(params);
  var executor = new Executor({ engine: engine });
  
  var responseName = null;
  if (lodash.isString(params.responseName)) {
    responseName = params.responseName;
  }

  var timeoutMonitor = null;
  var timeoutConfig = { supervisorId: rpcMasterId, tasks: store.tasks };

  timeoutConfig.monitorId = misc.getUUID();
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
    LM.isEnabledFor('info') && LM.log('info', {
      message: 'ready() is invoked',
      rpcMasterId: rpcMasterId });
    var asyncFuns = [];
    asyncFuns.push(engine.ready());
    if (responseName) {
      asyncFuns.push(executor.assertQueue({
        queueName: responseName
      }));
    }
    return Promise.all(asyncFuns).then(function(result) {
      LM.isEnabledFor('info') && LM.log('info', {
        message: 'ready() has been done',
        rpcMasterId: rpcMasterId });
      return result;
    });
  }

  this.request = function(routineId, data, opts) {
    opts = opts || {};
    opts.requestId = opts.requestId || misc.getUUID();

    LM.isEnabledFor('info') && LM.log('info', {
      message: 'request() - make a request',
      rpcMasterId: rpcMasterId,
      routineId: routineId,
      requestId: opts.requestId });
    
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

    var correlationId = misc.getUUID();

    if (typeof(routineId) === 'string') {
      opts.routineId = routineId;
    }

    opts.supervisorId = rpcMasterId;

    var task = new RpcRequest(opts);
    store.tasks[correlationId] = task;

    var properties = {
      correlationId: correlationId,
      headers: {
        routineId: task.routineId,
        requestId: task.requestId
      }
    };

    LM.isEnabledFor('debug') && LM.log('debug', {
      message: 'request() - properties',
      requestId: opts.requestId,
      properties: properties });

    return consumerInfo.then(function(sandbox) {
      if (!sandbox['fixedQueue']) {
        properties.replyTo = sandbox['queueName']
      }
      return engine.produce(data, properties);
    }).then(function() {
      LM.isEnabledFor('info') && LM.log('info', {
        message: 'request() has been done successfully, return task object',
        requestId: opts.requestId });
      return store.tasks[correlationId];
    }).catch(function(error) {
      LM.isEnabledFor('info') && LM.log('info', {
        message: 'request() - engine.produce() failed, reject with error',
        error: JSON.stringify(error),
        requestId: opts.requestId });
      return Promise.reject(error);
    });
  };

  var initResponseConsumer = function(forked) {
    LM.isEnabledFor('info') && LM.log('info', {
      message: 'initResponseConsumer()',
      forked: forked,
      rpcMasterId: rpcMasterId });

    var options = { binding: false, prefetch: 1, noAck: true }
    if (!forked) {
      options['queueName'] = responseName;
      options['consumerLimit'] = 1;
      options['forceNewChannel'] = true;
    }

    return engine.consume(function(msg, sandbox, done) {
      var props = msg && msg.properties || {};
      var correlationId = props.correlationId;
      var task = store.tasks[correlationId];

      if (!correlationId || !task) {
        done();
        return;
      }

      var message = msg.content.toString();
      LM.isEnabledFor('conlog') && LM.log('conlog', {
        message: 'ResponseConsumer - message: %s, properties: %s',
        body: message,
        properties: props });

      try {
        message = JSON.parse(message);
      } catch(error) {
        LM.isEnabledFor('info') && LM.log('info', {
          message: 'ResponseConsumer - JSON.parse() failed' });
        if (LM.isEnabledFor('conlog')) {
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
        break;
      }
      done();
    }, options).then(function(consumerRef) {
      consumerRefs.push(consumerRef);
      return consumerRef;
    });
  }

  this.close = function() {
    if (monitorEnabled && timeoutMonitor) {
      timeoutMonitor.stop();
      timeoutMonitor = null;
    }
    LM.isEnabledFor('info') && LM.log('info', {
      message: 'close() is invoked',
      rpcMasterId: rpcMasterId });
    return Promise.mapSeries(consumerRefs, function(consumerInfo) {
      return engine.cancelConsumer(consumerInfo);
    }).then(function() {
      responseConsumer = null;
      while(consumerRefs.length > 0) consumerRefs.pop();
      LM.isEnabledFor('info') && LM.log('info', {
        message: 'close() has been finished',
        rpcMasterId: rpcMasterId });
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
    LM.isEnabledFor('debug') && LM.log('debug', {
      message: 'auto execute ready()',
      rpcMasterId: rpcMasterId });
    misc.notifyConstructor(self.ready(), self);
  }

  LM.isEnabledFor('info') && LM.log('info', {
    message: 'RpcMaster.new() end!',
    rpcMasterId: rpcMasterId });
}

util.inherits(RpcMaster, events.EventEmitter);

module.exports = RpcMaster;

var mapping = function(status) {
  return status;
}

var LR = LogAdapter.getLogger({ scope: 'opflow:rpc:request' });

var RpcRequest = function RpcRequest(kwargs) {
  events.EventEmitter.call(this);

  kwargs = kwargs || {};
  var _supervisorId = kwargs.supervisorId || misc.getUUID();
  var _routineId = kwargs.routineId || kwargs.routineID || misc.getUUID();
  var _requestId = kwargs.requestId || kwargs.requestID || misc.getUUID();
  var _timeout = lodash.isNumber(kwargs.timeout) && kwargs.timeout > 0 && kwargs.timeout || 0;
  var _timestamp = misc.getCurrentTime();
  var self = this;

  LR.isEnabledFor('info') && LR.log('info', {
    message: 'RpcRequest.new()',
    supervisorId: _supervisorId,
    requestId: _requestId,
    timestamp: _timestamp });

  Object.defineProperties(this, {
    'supervisorId': {
      get: function() { return _supervisorId },
      set: function(val) {}
    },
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

  this.raiseTimeout = function() {
    var timeoutInfo = { message: 'The request has been timeout' };
    this.emit('failed', timeoutInfo);
    this.emit('timeout', timeoutInfo);
    LR.isEnabledFor('debug') && LR.log('debug', {
      message: 'raiseTimeout()',
      supervisorId: _supervisorId,
      requestId: _requestId,
      timeout: _timeout,
      timestamp: _timestamp });
  }

  this.extractResult = function() {
    var task = this;
    var superId = _supervisorId;
    var requestID = _requestId;
    return new Promise(function(onResolved, onRejected) {
      var steps = [];
      task.on('started', function(info) {
        LR.isEnabledFor('conlog') && LR.log('conlog', {
          message: 'Task is started', supervisorId: superId, requestId: requestID });
      }).on('progress', function(percent, data) {
        steps.push({ percent: percent, info: data });
        LR.isEnabledFor('conlog') && LR.log('conlog', {
          message: 'Task in progress', percent: percent, supervisorId: superId, requestId: requestID });
      }).on('timeout', function(info) {
        LR.isEnabledFor('conlog') && LR.log('conlog', {
          message: 'Task is timeout', supervisorId: superId, requestId: requestID });
        onResolved(new RpcResult(task, { progress: steps, event: 'timeout' }));
      }).on('failed', function(error) {
        LR.isEnabledFor('conlog') && LR.log('conlog', {
          message: 'Task is failed', supervisorId: superId, requestId: requestID, error: error });
        onResolved(new RpcResult(task, { progress: steps, event: 'failed', error: error }));
      }).on('completed', function(result) {
        LR.isEnabledFor('conlog') && LR.log('conlog', {
          message: 'Task is done', supervisorId: superId, requestId: requestID, value: result });
        onResolved(new RpcResult(task, { progress: steps, event: 'completed', value: result }));
      });
    });
  }

  LR.isEnabledFor('info') && LR.log('info', {
    message: 'RpcRequest.new() end!',
    supervisorId: _supervisorId,
    requestId: _requestId });
}

util.inherits(RpcRequest, events.EventEmitter);


var LT = LogAdapter.getLogger({ scope: 'opflow:rpc:result' });

var RpcResult = function(task, info) {
  var self = this;
  task = task || {};
  info = info || {};

  LT.isEnabledFor('info') && LT.log('info', {
    message: 'RpcResult.new()',
    supervisorId: task.supervisorId,
    requestId: task.requestId });

  ['requestId', 'routineId', 'timestamp'].forEach(function(propName) {
    Object.defineProperty(self, propName, {
      get: function() { return task[propName] },
      set: function(val) {}
    });
  });

  Object.defineProperties(self, {
    'timeout': {
      get: function() { return info.event === 'timeout' },
      set: function(val) {}
    },
    'failed': {
      get: function() { return info.event === 'failed' },
      set: function(val) {}
    },
    'error': {
      get: function() { return info.error },
      set: function(val) {}
    },
    'completed': {
      get: function() { return info.event === 'completed' },
      set: function(val) {}
    },
    'value': {
      get: function() { return info.value },
      set: function(val) {}
    }
  });

  LT.isEnabledFor('info') && LT.log('info', {
    message: 'RpcResult.new() end!',
    supervisorId: task.supervisorId,
    requestId: task.requestId });
}
