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
  LM.isEnabledFor('verbose') && LM.log('verbose', ' + constructor begin ...');

  events.EventEmitter.call(this);

  params = params || {};
  var self = this;
  var store = { tasks: {} }
  params = lodash.defaults({mode: 'rpc_master'}, params);
  var engine = new Engine(params);
  var executor = new Executor({ engine: engine });
  
  var responseName = null;
  if (lodash.isString(params.responseName)) {
    responseName = params.responseName;
  }

  var timeoutMonitor = null;
  var timeoutConfig = { tasks: store.tasks };

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
    var asyncFuns = [];
    asyncFuns.push(engine.ready());
    if (responseName) {
      asyncFuns.push(executor.assertQueue({
        queueName: responseName
      }));
    }
    return Promise.all(asyncFuns);
  }

  this.request = function(routineId, data, opts) {
    opts = opts || {};
    LM.isEnabledFor('verbose') && LM.log('verbose', 'request() - data: %s, opts: %s', JSON.stringify(data), JSON.stringify(opts));
    
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

    var task = new RpcRequest(opts);
    store.tasks[correlationId] = task;

    LM.isEnabledFor('verbose') && LM.log('verbose', 'request() - RpcRequest[%s] is created, engine.produce() invoked', correlationId);

    var properties = {
      correlationId: correlationId,
      headers: {
        routineId: task.routineId,
        requestId: task.requestId
      }
    };

    return consumerInfo.then(function(sandbox) {
      if (!sandbox['fixedQueue']) {
        properties.replyTo = sandbox['queueName']
      }
      return engine.produce(data, properties);
    }).then(function() {
      LM.isEnabledFor('verbose') && LM.log('verbose', 'request() - engine.produce() successfully, return task');
      return store.tasks[correlationId];
    }).catch(function(error) {
      LM.isEnabledFor('verbose') && LM.log('verbose', 'request() - engine.produce() failed, error: %s', JSON.stringify(error));
      return Promise.reject(error);
    });
  };

  var initResponseConsumer = function(forked) {
    LM.isEnabledFor('verbose') && LM.log('verbose', 'initResponseConsumer(forked:%s)', forked);
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
      LM.isEnabledFor('verbose') && LM.log('verbose', 'ResponseConsumer - message: %s, properties: %s', message, JSON.stringify(props));
      try {
        message = JSON.parse(message);
      } catch(error) {
        LM.isEnabledFor('verbose') && LM.log('verbose', 'ResponseConsumer - JSON.parse() failed, error: %s', JSON.stringify(error));
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
    return Promise.mapSeries(consumerRefs, function(consumerInfo) {
      return engine.cancelConsumer(consumerInfo);
    }).then(function() {
      responseConsumer = null;
      while(consumerRefs.length > 0) consumerRefs.pop();
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
    misc.notifyConstructor(self.ready(), self);
  }

  LM.isEnabledFor('verbose') && LM.log('verbose', ' - constructor end!');
}

util.inherits(RpcMaster, events.EventEmitter);

module.exports = RpcMaster;

var mapping = function(status) {
  return status;
}

var LR = LogAdapter.getLogger({ scope: 'opflow:rpc:request' });

var RpcRequest = function RpcRequest(kwargs) {
  events.EventEmitter.call(this);

  LR.isEnabledFor('verbose') && LR.log('verbose', ' + constructor begin ...');

  kwargs = kwargs || {};
  var _routineId = kwargs.routineId || kwargs.routineID;
  var _requestId = kwargs.requestId || kwargs.requestID;
  var _timeout = lodash.isNumber(kwargs.timeout) && kwargs.timeout > 0 && kwargs.timeout || 0;
  var _timestamp = misc.getCurrentTime();
  var self = this;

  Object.defineProperties(this, {
    'routineId': {
      get: function() { return _routineId = _routineId || misc.getUUID() },
      set: function(val) {}
    },
    'requestId': {
      get: function() { return _requestId = _requestId || misc.getUUID() },
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
  }

  this.extractResult = function() {
    var task = this;
    var requestID = task.requestId;
    return new Promise(function(onResolved, onRejected) {
      var steps = [];
      task.on('started', function(info) {
        LR.isEnabledFor('verbose') && LR.log('verbose', 'Request[%s] started', requestID);
      }).on('progress', function(percent, data) {
        steps.push({ percent: percent, info: data });
        LR.isEnabledFor('verbose') && LR.log('verbose', 'Request[%s] progress: %s', requestID, percent);
      }).on('timeout', function(info) {
        LR.isEnabledFor('verbose') && LR.log('verbose', 'Request[%s] timeout', requestID);
        onResolved(new RpcResult(task, { progress: steps, event: 'timeout' }));
      }).on('failed', function(error) {
        LR.isEnabledFor('verbose') && LR.log('verbose', 'Request[%s] failed, error: %s', requestID, JSON.stringify(error));
        onResolved(new RpcResult(task, { progress: steps, event: 'failed', error: error }));
      }).on('completed', function(result) {
        LR.isEnabledFor('verbose') && LR.log('verbose', 'Request[%s] done, result: %s', requestID, JSON.stringify(result));
        onResolved(new RpcResult(task, { progress: steps, event: 'completed', value: result }));
      });
    });
  }

  LR.isEnabledFor('verbose') && LR.log('verbose', ' - constructor end!');
}

util.inherits(RpcRequest, events.EventEmitter);


var LT = LogAdapter.getLogger({ scope: 'opflow:rpc:result' });

var RpcResult = function(task, info) {
  LT.isEnabledFor('verbose') && LT.log('verbose', ' + constructor begin ...');

  var self = this;
  task = task || {};
  info = info || {};

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

  LT.isEnabledFor('verbose') && LT.log('verbose', ' - constructor end!');
}
