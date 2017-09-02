'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var debugx = require('debug')('opflow:task');
var errors = require('./exception');
var misc = require('./util');

var TimeoutHandler = function(kwargs) {
  debugx.enabled && debugx(' + TimeoutHandler constructor begin ...');

  kwargs = kwargs || {};
  var config = lodash.pick(kwargs, ['tasks', 'interval', 'timeout', 'monitorId']);

  config.monitorId = config.monitorId || misc.getUUID();

  var timerTask = function() {
    if (lodash.isEmpty(config.tasks)) return;
    var current = misc.getCurrentTime();
    debugx.enabled && debugx('timerTask[%s] is invoked, current time: %s', config.monitorId, current.toISOString());
    lodash.forEach(lodash.keys(config.tasks), function(taskId) {
      var task = config.tasks[taskId];
      if (lodash.isObject(task)) {
        var timeout = task.timeout || config.timeout || 0;
        if (timeout > 0) {
          var timediff = current - (task.timestamp || 0);
          debugx.enabled && debugx('timerTask() - request[%s] - current/timestamp/timediff/timeout: %s/%s/%s/%s', 
              task.requestId, current, task.timestamp, timediff, timeout);
          if (timediff > timeout) {
            delete config.tasks[taskId];
            lodash.isFunction(task.raiseTimeout) && task.raiseTimeout();
            debugx.enabled && debugx('timerTask[%s] run, task[%s] is timeout, will be removed', config.monitorId, taskId);
          } else {
            debugx.enabled && debugx('timerTask[%s] run, task[%s] in good state, keep running', config.monitorId, taskId);
          }
        }
      }
    });
    debugx.enabled && debugx('timerTask[%s] checktime[%s] has been done', config.monitorId, current.toISOString());
  }

  var timer = new RepeatedTimer({ target: timerTask, period: config.interval });

  this.start = function() {
    debugx.enabled && debugx(' - TimeoutHandler is starting');
    timer.start();
  }

  this.stop = function() {
    debugx.enabled && debugx(' - TimeoutHandler will be stopped');
    timer.stop();
  }

  debugx.enabled && debugx(' - TimeoutHandler constructor end!');
}

var RepeatedTimer = function(kwargs) {
  events.EventEmitter.call(this);
  debugx.enabled && debugx(' + RepeatedTimer constructor begin ...');

  kwargs = kwargs || {};

  var config = lodash.pick(kwargs, ['target', 'period', 'offset', 'total', 'activated', 'name']);

  if (!lodash.isFunction(config.target)) {
    throw new errors.ParameterError();
  }

  config.total = config.total || 0;
  config.period = standardizeInt(MIN_PERIOD, config.period || 1000);
  config.offset = standardizeInt(MIN_OFFSET, config.offset || 0);

  var taskHandler = null;
  var taskCounter = 0;

  var taskWrapper = function() {
    taskCounter++;
    if (0 == config.total || taskCounter <= config.total) {
      config.target();
    } else {
      self.stop();
    }
  };

  this.start = function() {
    debugx.enabled && debugx(' - RepeatedTimer is starting');
    this.emit('started', {});
    return this.startInSilent();
  }

  this.startInSilent = function() {
    if (0 < config.total && config.total < taskCounter) {
      return this;
    }
    if (!taskHandler) {
      var taskFunction = taskWrapper;
      if (config.offset > 0) {
        taskFunction = function() {
          setTimeout(taskWrapper, getRandomInt(0, config.offset));
        };
      }
      taskHandler = setInterval(taskFunction, config.period);
    }
    return this;
  }

  this.stop = function() {
    debugx.enabled && debugx(' - RepeatedTimer will be stopped');
    this.emit('stopped', {});
    return this.stopInSilent();
  }

  this.stopInSilent = function() {
    if (taskHandler) {
      clearInterval(taskHandler);
      taskHandler = null;
    }
    return this;
  }

  this.isRunning = function() {
    return (taskHandler != null);
  }

  this.isStopped = function() {
    return (taskHandler == null);
  }

  if (config.activated) this.start();

  debugx.enabled && debugx(' - RepeatedTimer constructor end!');
}

util.inherits(RepeatedTimer, events.EventEmitter);

function standardizeInt(min, number) {
  return (number > min) ? number : min;
}

function getRandomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

var MIN_PERIOD = 10;
var MIN_OFFSET = 0;

module.exports = {
  TimeoutHandler: TimeoutHandler
};