'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var debugx = require('debug')('opflow:master');
var debug9 = require('debug')('opflow:master:request');
var Engine = require('./engine');
var Executor = require('./executor');
var TimeoutHandler = require('./task').TimeoutHandler;
var misc = require('./util');

var RpcMaster = function(params) {
  debugx.enabled && debugx(' + constructor begin ...');

  params = params || {};
  var self = this;
  self.logger = self.logger || params.logger;

  var store = { tasks: {} }
  params = lodash.defaults({mode: 'rpc_master'}, params);
  var engine = new Engine(params);
  var executor = new Executor({ engine: engine });
  
  var responseName = null;
  if (lodash.isString(params.responseName)) {
    responseName = params.responseName;
    executor.assertQueue({
      queueName: responseName
    });
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

  this.request = function(routineId, data, opts) {
    opts = opts || {};
    debugx.enabled && debugx('request() - data: %s, opts: %s', JSON.stringify(data), JSON.stringify(opts));
    
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

    debugx.enabled && debugx('request() - RpcRequest[%s] is created, engine.produce() invoked', correlationId);

    return engine.produce(data, {
      correlationId: correlationId,
      headers: {
        routineId: task.routineId,
        requestId: task.requestId
      }
    }).then(function() {
      debugx.enabled && debugx('request() - engine.produce() successfully, return task');
      return store.tasks[correlationId];
    }).catch(function(error) {
      debugx.enabled && debugx('request() - engine.produce() failed, error: %s', JSON.stringify(error));
      return Promise.reject(error);
    });
  };

  var initResponseConsumer = function(forked) {
    debugx.enabled && debugx('initResponseConsumer(forked:%s)', forked);
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
      debugx.enabled && debugx('ResponseConsumer - message: %s, properties: %s', message, JSON.stringify(props));
      try {
        message = JSON.parse(message);
      } catch(error) {
        debugx.enabled && debugx('ResponseConsumer - JSON.parse() failed, error: %s', JSON.stringify(error));
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

  this.ready = engine.ready.bind(engine);

  this.close = function() {
    if (monitorEnabled && timeoutMonitor) timeoutMonitor.stop();
    return Promise.mapSeries(consumerRefs, function(consumerInfo) {
      return engine.cancelConsumer(consumerInfo);
    }).then(function() {
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

  debugx.enabled && debugx(' - constructor end!');
}

util.inherits(RpcMaster, events.EventEmitter);

module.exports = RpcMaster;

var mapping = function(status) {
  return status;
}

var RpcRequest = function RpcRequest(kwargs) {
  events.EventEmitter.call(this);

  debug9.enabled && debug9(' + constructor begin ...');

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

  debug9.enabled && debug9(' - constructor end!');
}

util.inherits(RpcRequest, events.EventEmitter);
