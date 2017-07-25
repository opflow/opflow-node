'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var uuidV4 = require('uuid/v4');
var debugx = require('debug')('opflow:runner');
var debug0 = require('debug')('opflow:master');
var debug9 = require('debug')('opflow:master:task');
var debug1 = require('debug')('opflow:worker');
var Engine = require('./engine');

var Runner = function(params) {
  events.EventEmitter.call(this);

  (['master', 'worker'].indexOf(params.mode) < 0) &&
  debugx.enabled && debugx(' + constructor begin ...');

  params = params || {};
  var self = this;
  self.logger = self.logger || params.logger;

  var store = { tasks: {} }
  var engine = new Engine(params);

  if (['master'].indexOf(params.mode) >= 0)
  this.execute = function(data, opts) {
    opts = opts || {};
    debugx.enabled && debugx('execute() - data: %s, opts: %s', JSON.stringify(data), JSON.stringify(opts));
    var task = new Task();
    var correlationId = task.id;
    store.tasks[correlationId] = task;
    debugx.enabled && debugx('execute() - Task[%s] is created, engine.produce() invoked', correlationId);
    return engine.produce(data, {
      correlationId: correlationId,
      //replyTo: self.config.dequeueName,
      headers: {
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

  if (['master'].indexOf(params.mode) >= 0)
  engine.pullout(function(message, info, done) {
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
  });

  if (['worker'].indexOf(params.mode) >= 0)
  this.process = function(callback) {
    return engine.readyConsumer().then(function() {
      return engine.consume(function(message, info, done, notifier) {
        var headers = info && info.properties && info.properties.headers || {};
        callback(message, headers, done, notifier);
      });
    });
  };

  this.ready = engine.ready.bind(engine);

  this.purge = engine.purge.bind(engine);

  this.destroy = engine.destroy.bind(engine);

  (['master', 'worker'].indexOf(params.mode) < 0) &&
  debugx.enabled && debugx(' - constructor end!');
}

util.inherits(Runner, events.EventEmitter);

module.exports = Runner;

var Task = function(args) {
  events.EventEmitter.call(this);

  debug9.enabled && debug9(' + constructor begin ...');

  args = args || {};
  var _id = args.id;
  var _requestId = args.requestId || args.requestID;
  var self = this;

  Object.defineProperties(this, {
    'id': {
      get: function() { return _id = _id || uuidV4() },
      set: function(val) {}
    },
    'requestId': {
      get: function() { return _requestId = _requestId || self.id },
      set: function(val) {}
    }
  });

  debug9.enabled && debug9(' - constructor end!');
}

util.inherits(Task, events.EventEmitter);

var mapping = function(status) {
  return status;
}
