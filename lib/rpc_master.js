'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var debugx = require('debug')('opflow:master');
var Engine = require('./engine');
var Task = require('./task');
var misc = require('./misc');

var RpcMaster = function(params) {
  debugx.enabled && debugx(' + constructor begin ...');

  params = params || {};
  var self = this;
  self.logger = self.logger || params.logger;

  var store = { tasks: {} }
  params = lodash.defaults({mode: 'master'}, params);
  var engine = new Engine(params);
  
  var responseConsumer = null;
  var responseName = params['responseName'];

  this.execute = function(data, opts) {
    opts = opts || {};
    debugx.enabled && debugx('execute() - data: %s, opts: %s', JSON.stringify(data), JSON.stringify(opts));
    
    var correlationId = misc.getUUID();

    if (typeof(routineId) === 'string') {
      opts.routineId = routineId;
    }

    var task = new Task(opts);
    store.tasks[correlationId] = task;

    debugx.enabled && debugx('execute() - Task[%s] is created, engine.produce() invoked', correlationId);

    return engine.produce(data, {
      correlationId: correlationId,
      headers: {
        routineId: task.routineId,
        requestId: task.requestId
      }
    }).then(function() {
      debugx.enabled && debugx('execute() - engine.produce() successfully, return task');
      return store.tasks[correlationId];
    }).catch(function(error) {
      debugx.enabled && debugx('execute() - engine.produce() failed, error: %s', JSON.stringify(error));
      return Promise.reject(error);
    });
  };

  var initResponseConsumer = function(forked) {
    debugx.enabled && debugx('initResponseConsumer(forked:%s)', forked);
    var options = { binding: false, prefetch: 1 }
    if (!forked) {
      options['queueName'] = responseName;
      options['consumerLimit'] = 1;
      options['forceNewChannel'] = false;
    }
    return engine.pullout(function(message, info, done) {
      var props = info && info.properties || {};
      var correlationId = props.correlationId;
      if (!correlationId || !store.tasks[correlationId]) {
        done();
        return;
      }
      var task = store.tasks[correlationId];

      debugx.enabled && debugx('engine.pullout() - message: %s, properties: %s', message, JSON.stringify(props));
      try {
        message = JSON.parse(message);
      } catch(error) {
        debugx.enabled && debugx('engine.pullout() - JSON.parse() failed, error: %s', JSON.stringify(error));
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
  }

  responseConsumer = initResponseConsumer(false);

  this.ready = engine.ready.bind(engine);

  this.purge = engine.purge.bind(engine);

  this.destroy = engine.destroy.bind(engine);

  debugx.enabled && debugx(' - constructor end!');
}

util.inherits(RpcMaster, events.EventEmitter);

module.exports = RpcMaster;

var mapping = function(status) {
  return status;
}
