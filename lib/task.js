'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var errors = require('./exception');
var misc = require('./util');
var LogTracer = require('./log_tracer');
var LogAdapter = require('./log_adapter');
var L1 = LogAdapter.getLogger({ scope: 'opflow:task:TimeoutHandler' });

var TimeoutHandler = function(kwargs) {
  kwargs = kwargs || {};
  var config = lodash.pick(kwargs, [
    'monitorId', 'monitorType', 'tasks', 'interval', 'timeout', 'raiseTimeout', 'teardown'
  ]);

  config.monitorId = config.monitorId || misc.getLogID();
  var monitorTrail = LogTracer.ROOT.branch({ key:'monitorId', value:config.monitorId });

  config.raiseTimeout = config.raiseTimeout || function(task, done) { done() }

  L1.isEnabledFor('info') && L1.log('info', monitorTrail.add({
    monitorType: config.monitorType,
    message: 'TimeoutHandler.new()'
  }).toString({reset:true}));

  var timerTask = function() {
    if (lodash.isEmpty(config.tasks)) return;

    var current = misc.getCurrentTime();
    var currentTrail = monitorTrail.branch({ key:'checktime', value:current.toISOString() })

    L1.isEnabledFor('conlog') && L1.log('conlog', currentTrail.add({
      message: 'TimeoutHandler checking loop is invoked'
    }).toString());

    lodash.forEach(lodash.keys(config.tasks), function(taskId) {
      var task = config.tasks[taskId];
      if (lodash.isObject(task)) {
        var timeout = task.timeout || config.timeout || 0;
        if (timeout > 0) {
          var timediff = current - (task.timestamp || 0);
          L1.isEnabledFor('conlog') && L1.log('conlog', currentTrail.add({
            message: 'TimeoutHandler check timeout for a task',
            taskId: taskId, 
            requestId: task.requestId,
            timestamp: task.timestamp,
            timeout: timeout,
            timediff: timediff
          }).toString({reset:true}));
          if (timediff > timeout) {
            if (lodash.isFunction(task.raiseTimeout)) {
              delete config.tasks[taskId];
              task.raiseTimeout();
            } else {
              config.raiseTimeout(task, function(err) {
                delete config.tasks[taskId];
              });
            }
            L1.isEnabledFor('conlog') && L1.log('conlog', currentTrail.add({
              message: 'TimeoutHandler task is timeout, will be removed',
              taskId: taskId
            }).toString({reset:true}));
          } else {
            L1.isEnabledFor('conlog') && L1.log('conlog', currentTrail.add({
              message: 'TimeoutHandler task in good status, keep running',
              taskId: taskId
            }).toString({reset:true}));
          }
        }
      }
    });
    L1.isEnabledFor('conlog') && L1.log('conlog', currentTrail.add({
      message: 'TimeoutHandler checking loop has been done'
    }).toString({reset:true}));
  }

  var timer = new RepeatedTimer({
    target: timerTask,
    period: config.interval,
    monitorId: config.monitorId
  });

  var defaultTeardown = function(state) {
    state = state || {};
    state.tasks = state.tasks || {};
    lodash.forEach(lodash.keys(state.tasks), function(taskId) {
      L1.isEnabledFor('error') && L1.log('error', monitorTrail.add({
        message: 'TimeoutHandler paused, SAVE UNFINISHED TASKS',
        task: lodash.omitBy(state.tasks[taskId], lodash.isFunction)
      }).toString());
    });
  }

  this.start = function() {
    L1.isEnabledFor('info') && L1.log('info', monitorTrail.add({
      message: 'TimeoutHandler daemon is starting'
    }).toString());
    timer.start();
  }

  this.stop = function(opts) {
    opts = opts || {};
    timer.stop();
    L1.isEnabledFor('info') && L1.log('info', monitorTrail.add({
      message: 'TimeoutHandler daemon will be stopped'
    }).toString());
    opts.timeout = typeof(opts.timeout) === 'number' ? opts.timeout : 30000;
    if (opts.timeout <= 0) return Promise.resolve();
    return new Promise(function(onResolved, onRejected) {
      var handler1, handler2;
      var stopWaiting = function(ok) {
        handler1 && clearInterval(handler1);
        handler1 = null;
        handler2 && clearTimeout(handler2);
        handler2 = null;
        if (!ok) {
          var teardown = opts.teardown || config.teardown || defaultTeardown;
          if (lodash.isFunction(teardown)) {
            teardown({ tasks: config.tasks });
          }
        }
        onResolved(ok === true);
      }
      handler1 = setInterval(function() {
        if (lodash.isEmpty(config.tasks)) stopWaiting(true);
      }, 100);
      handler2 = setTimeout(stopWaiting, opts.timeout);
    });
  }

  L1.isEnabledFor('info') && L1.log('info', monitorTrail.add({
    message: 'TimeoutHandler.new() end!'
  }).toString());
}

var L2 = LogAdapter.getLogger({ scope: 'opflow:task:RepeatedTimer' });

var RepeatedTimer = function(kwargs) {
  events.EventEmitter.call(this);

  kwargs = kwargs || {};
  kwargs.monitorId = kwargs.monitorId || misc.getLogID();

  var monitorTrail = LogTracer.ROOT.branch({ key:'monitorId', value:kwargs.monitorId });

  L2.isEnabledFor('info') && L2.log('info', monitorTrail.add({
    message: 'RepeatedTimer.new()'
  }).toString());

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
    L2.isEnabledFor('info') && L2.log('info', monitorTrail.add({
      message: 'RepeatedTimer daemon is starting'
    }).toString());
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
    L2.isEnabledFor('info') && L2.log('info', monitorTrail.add({
      message: 'RepeatedTimer daemon will be stopped'
    }).toString());
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

  L2.isEnabledFor('info') && L2.log('info', monitorTrail.add({
    message: 'RepeatedTimer.new() end!'
  }).toString());
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