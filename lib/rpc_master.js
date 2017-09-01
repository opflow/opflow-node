'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var debugx = require('debug')('opflow:master');
var Engine = require('./engine');
var Executor = require('./executor');
var Task = require('./task');
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

  var consumerRefs = [];
  var responseConsumer = null;

  this.request = function(routineId, data, opts) {
    opts = opts || {};
    debugx.enabled && debugx('request() - data: %s, opts: %s', JSON.stringify(data), JSON.stringify(opts));
    
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

    var task = new Task(opts);
    store.tasks[correlationId] = task;

    debugx.enabled && debugx('request() - Task[%s] is created, engine.produce() invoked', correlationId);

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
    var consumerRef = engine.consume(function(msg, sandbox, done) {
      var props = msg && msg.properties || {};
      var correlationId = props.correlationId;
      if (!correlationId || !store.tasks[correlationId]) {
        done();
        return;
      }
      var task = store.tasks[correlationId];

      var message = msg.content.toString();
      debugx.enabled && debugx('initResponseConsumer() - message: %s, properties: %s', message, JSON.stringify(props));
      try {
        message = JSON.parse(message);
      } catch(error) {
        debugx.enabled && debugx('initResponseConsumer() - JSON.parse() failed, error: %s', JSON.stringify(error));
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
    }, options);
    consumerRefs.push(consumerRef);
    return consumerRef;
  }

  this.ready = engine.ready.bind(engine);

  this.destroy = function() {
    return Promise.mapSeries(consumerRefs, function(consumerInfo) {
      return engine.cancelConsumer(consumerInfo);
    }).then(function() {
      return engine.destroy();
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
