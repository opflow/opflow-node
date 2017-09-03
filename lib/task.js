'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var errors = require('./exception');
var misc = require('./util');
var LogAdapter = require('./logadapter');

var L1 = LogAdapter.getLogger({ scope: 'opflow:task:TimeoutHandler' });

var TimeoutHandler = function(kwargs) {
  L1.isEnabledFor('verbose') && L1.log('verbose', ' + constructor begin ...');

  kwargs = kwargs || {};
  var config = lodash.pick(kwargs, ['tasks', 'interval', 'timeout', 'monitorId']);

  config.monitorId = config.monitorId || misc.getUUID();

  var timerTask = function() {
    if (lodash.isEmpty(config.tasks)) return;
    var current = misc.getCurrentTime();
    L1.isEnabledFor('verbose') && L1.log('verbose', 'timerTask[%s] is invoked, current time: %s', config.monitorId, current.toISOString());
    lodash.forEach(lodash.keys(config.tasks), function(taskId) {
      var task = config.tasks[taskId];
      if (lodash.isObject(task)) {
        var timeout = task.timeout || config.timeout || 0;
        if (timeout > 0) {
          var timediff = current - (task.timestamp || 0);
          L1.isEnabledFor('verbose') && L1.log('verbose', 'timerTask() - request[%s] - current/timestamp/timediff/timeout: %s/%s/%s/%s', 
              task.requestId, current, task.timestamp, timediff, timeout);
          if (timediff > timeout) {
            delete config.tasks[taskId];
            lodash.isFunction(task.raiseTimeout) && task.raiseTimeout();
            L1.isEnabledFor('verbose') && L1.log('verbose', 'timerTask[%s] run, task[%s] is timeout, will be removed', config.monitorId, taskId);
          } else {
            L1.isEnabledFor('verbose') && L1.log('verbose', 'timerTask[%s] run, task[%s] in good state, keep running', config.monitorId, taskId);
          }
        }
      }
    });
    L1.isEnabledFor('verbose') && L1.log('verbose', 'timerTask[%s] checktime[%s] has been done', config.monitorId, current.toISOString());
  }

  var timer = new RepeatedTimer({ target: timerTask, period: config.interval });

  this.start = function() {
    L1.isEnabledFor('verbose') && L1.log('verbose', ' - daemon is starting');
    timer.start();
  }

  this.stop = function() {
    L1.isEnabledFor('verbose') && L1.log('verbose', ' - daemon will be stopped');
    timer.stop();
  }

  L1.isEnabledFor('verbose') && L1.log('verbose', ' - constructor end!');
}

var L2 = LogAdapter.getLogger({ scope: 'opflow:task:RepeatedTimer' });

var RepeatedTimer = function(kwargs) {
  events.EventEmitter.call(this);
  L2.isEnabledFor('verbose') && L2.log('verbose', ' + constructor begin ...');

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
    L2.isEnabledFor('verbose') && L2.log('verbose', ' - daemon is starting');
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
    L2.isEnabledFor('verbose') && L2.log('verbose', ' - daemon will be stopped');
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

  L2.isEnabledFor('verbose') && L2.log('verbose', ' - constructor end!');
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