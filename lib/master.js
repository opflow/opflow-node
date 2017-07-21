'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var uuidV4 = require('uuid/v4');
var debugx = require('debug')('opflow:master');
var Engine = require('./engine');

var Handler = function(params) {
  events.EventEmitter.call(this);

  debugx.enabled && debugx(' + constructor begin ...');

  var self = this;
  self.logger = self.logger || params.logger;

  var store = { tasks: {} }
  var engine = new Engine(params);

  this.execute = function(data, opts) {
    opts = opts || {};
    debugx.enabled && debugx('execute() - data: %s, opts: %s', JSON.stringify(data), JSON.stringify(opts));
    var task = new Task();
    var correlationId = task.id;
    store.tasks[correlationId] = task;
    debugx.enabled && debugx('execute() - Task[%s] has been created', correlationId);
    return engine.produce(data, {
      correlationId: correlationId,
      //replyTo: self.config.dequeueName
    }).then(function() {
      debugx.enabled && debugx('execute() - engine produce() successfully, return task');
      return store.tasks[correlationId];
    }).catch(function(error) {
      debugx.enabled && debugx('execute() - engine produce() failed, error: %s', JSON.stringify(error));
      return Promise.reject(error);
    });
  }

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

  debugx.enabled && debugx(' - constructor end!');
}

util.inherits(Handler, events.EventEmitter);

module.exports = Handler;

var Task = function(args) {
  events.EventEmitter.call(this);

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
}

util.inherits(Task, events.EventEmitter);

var mapping = function(status) {
  return status;
}
