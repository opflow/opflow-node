'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var Readable = require('stream').Readable;
var Writable = require('stream').Writable;
var errors = require('./exception');
var misc = require('./util');
var LogTracer = require('logolite').LogTracer;
var LogAdapter = require('logolite').LogAdapter;
var L1 = LogAdapter.getLogger({ scope: 'opflow:task:TimeoutHandler' });

var TimeoutHandler = function(kwargs) {
  kwargs = kwargs || {};
  var config = lodash.pick(kwargs, [
    'monitorId', 'monitorType', 'tasks', 'interval', 'timeout', 'raiseTimeout', 'teardown', 'handlerType'
  ]);

  config.monitorId = config.monitorId || misc.getLogID();
  var monitorTrail = LogTracer.ROOT.branch({ key:'monitorId', value:config.monitorId });

  L1.isEnabledFor('info') && L1.log('info', monitorTrail.add({
    monitorType: config.monitorType
  }).toMessage({
    tags: ['TimeoutHandler.new()'],
    text: 'TimeoutHandler[{monitorId}].new()'
  }));

  var timerTask = function() {
    if (lodash.isEmpty(config.tasks)) return;

    var current = misc.getCurrentTime();
    var currentTrail = monitorTrail.branch({ key:'checktime', value:current.toISOString() })

    L1.isEnabledFor('conlog') && L1.log('conlog', currentTrail.toMessage({
      tags: ['TimeoutHandler checking loop'],
      text: 'TimeoutHandler[{monitorId}] checking loop is invoked'
    }));

    lodash.forEach(lodash.keys(config.tasks), function(taskId) {
      var task = config.tasks[taskId];
      if (lodash.isObject(task)) {
        var timeout = task.timeout || config.timeout || 0;
        if (timeout > 0) {
          var timediff = current - (task.timestamp || 0);
          L1.isEnabledFor('conlog') && L1.log('conlog', currentTrail.add({
            taskId: taskId, 
            requestId: task.requestId,
            timestamp: task.timestamp,
            timeout: timeout,
            timediff: timediff
          }).toMessage({
            text: 'TimeoutHandler[{monitorId}] check timeout for a task'
          }));
          if (timediff > timeout) {
            var raiseTimeout = task.raiseTimeout || config.raiseTimeout;
            if (lodash.isFunction(raiseTimeout)) {
              if (config.handlerType === 'callback') {
                raiseTimeout.call(task, function(err) {
                  delete config.tasks[taskId];
                });
              } else {
                new Promise(function(onResolved, onRejected) {
                  Promise.resolve(raiseTimeout.call(task, function(err) {
                    (err) ? onRejected(err) : onResolved();
                  })).then(onResolved).catch(onRejected);
                }).then(misc.getEmptyFunc).catch(misc.getEmptyFunc).finally(function() {
                  delete config.tasks[taskId];
                });
              }
              L1.isEnabledFor('conlog') && L1.log('conlog', currentTrail.add({
                taskId: taskId
              }).toMessage({
                tags: ['TimeoutHandler raised timeout event'],
                text: 'TimeoutHandler[{monitorId}] task[{taskId}] is timeout, event will be raised'
              }));
            } else {
              delete config.tasks[taskId];
              L1.isEnabledFor('conlog') && L1.log('conlog', currentTrail.add({
                taskId: taskId
              }).toMessage({
                tags: ['TimeoutHandler removed timeout task'],
                text: 'TimeoutHandler[{monitorId}] task[{taskId}] is timeout, task will be removed'
              }));
            }
          } else {
            L1.isEnabledFor('conlog') && L1.log('conlog', currentTrail.add({
              taskId: taskId
            }).toMessage({
              text: 'TimeoutHandler[{monitorId}] task in good status, keep running'
            }));
          }
        }
      }
    });
    L1.isEnabledFor('conlog') && L1.log('conlog', currentTrail.toMessage({
      text: 'TimeoutHandler[{monitorId}] checking loop has been done'
    }));
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
        task: lodash.omitBy(state.tasks[taskId], lodash.isFunction)
      }).toMessage({
        tags: ['TimeoutHandler log unfinished tasks'],
        text: 'TimeoutHandler[{monitorId}] will be stopped, SAVE UNFINISHED TASKS'
      }));
    });
  }

  var _touch = function(task) {
    task.timestamp = misc.getCurrentTime();
    return task;
  }

  this.start = function() {
    L1.isEnabledFor('info') && L1.log('info', monitorTrail.toMessage({
      text: 'TimeoutHandler[{monitorId}] daemon is starting'
    }));
    timer.start();
  }

  this.add = function(taskId, task) {
    if (typeof(taskId) !== 'string' || taskId.length === 0) {
      throw new errors.ParameterError('taskId should not be null or empty');
    }
    if (typeof(task) !== 'object' || task === null) {
      throw new errors.ParameterError('task must be an object and should not be null');
    }
    config.tasks[taskId] = _touch(task);
    return this;
  }

  this.get = function(taskId) {
    return config.tasks[taskId];
  }

  this.touch = function(taskId) {
    var task = config.tasks[taskId];
    return (task) ? _touch(task) : task;
  }

  this.remove = function(taskId) {
    delete config.tasks[taskId];
    return this;
  }

  this.stop = function(opts) {
    opts = opts || {};
    timer.stop();
    L1.isEnabledFor('info') && L1.log('info', monitorTrail.toMessage({
      text: 'TimeoutHandler[{monitorId}] daemon will be stopped'
    }));
    opts.timeout = typeof(opts.timeout) === 'number' ? opts.timeout : 30000;
    if (opts.timeout <= 0) {
      L1.isEnabledFor('info') && L1.log('info', monitorTrail.toMessage({
        text: 'TimeoutHandler[{monitorId}] daemon has stopped immediately'
      }));
      return Promise.resolve();
    }
    return new Promise(function(onResolved, onRejected) {
      L1.isEnabledFor('info') && L1.log('info', monitorTrail.toMessage({
        text: 'TimeoutHandler[{monitorId}] daemon will be stopped in ' + opts.timeout
      }));
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
        L1.isEnabledFor('info') && L1.log('info', monitorTrail.toMessage({
          text: 'TimeoutHandler[{monitorId}] daemon has stopped'
        }));
        onResolved(ok === true);
      }
      handler1 = setInterval(function() {
        if (lodash.isEmpty(config.tasks)) stopWaiting(true);
      }, 100);
      handler2 = setTimeout(stopWaiting, opts.timeout);
    });
  }

  L1.isEnabledFor('info') && L1.log('info', monitorTrail.toMessage({
    text: 'TimeoutHandler[{monitorId}].new() end!'
  }));
}

var L2 = LogAdapter.getLogger({ scope: 'opflow:task:RepeatedTimer' });

var RepeatedTimer = function(kwargs) {
  events.EventEmitter.call(this);

  kwargs = kwargs || {};
  kwargs.monitorId = kwargs.monitorId || misc.getLogID();

  var monitorTrail = LogTracer.ROOT.branch({ key:'monitorId', value:kwargs.monitorId });

  L2.isEnabledFor('info') && L2.log('info', monitorTrail.toMessage({
    text: 'RepeatedTimer[{monitorId}].new()'
  }));

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
    L2.isEnabledFor('info') && L2.log('info', monitorTrail.toMessage({
      text: 'RepeatedTimer[{monitorId}] daemon is starting'
    }));
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
    L2.isEnabledFor('info') && L2.log('info', monitorTrail.toMessage({
      text: 'RepeatedTimer[{monitorId}] daemon will be stopped'
    }));
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

  L2.isEnabledFor('info') && L2.log('info', monitorTrail.toMessage({
    text: 'RepeatedTimer[{monitorId}].new() end!'
  }));
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


var L4 = LogAdapter.getLogger({ scope: 'opflow:task:PayloadReader' });

var PayloadReader = function(kwargs) {
  kwargs = kwargs || {};
  Readable.call(this, {objectMode: (kwargs.objectMode === true)});
  var payloadId = kwargs.payloadId || misc.getLogID();
  var payloadTrail = LogTracer.ROOT.branch({ key: 'payloadId', value: payloadId });

  var self = this;
  var _chunk = {};
  var _index = 0;
  var _max = 0;
  var _ack = kwargs.ack;
  var _notifier = new events.EventEmitter();

  L4.isEnabledFor('info') && L4.log('info', payloadTrail.toMessage({
    text: 'PayloadReader.new()'
  }));

  var doRead = function(position) {
    if (position >= 0 && _chunk[position] !== undefined) {
      L4.isEnabledFor('trace') && L4.log('trace', payloadTrail.add({
        index: position,
        state: lodash.reduce(_chunk, function(accum, chunk, index) {
          if (chunk) {
            accum.total += 1;
            accum.index.push(index);
            accum.size += Buffer.byteLength(chunk.msg || '');
          }
          return accum;
        }, { total: 0, size: 0, index: [] })
      }).toMessage({
        text: 'doRead() - status'
      }));
      var ack = _chunk[position].ack || _ack;
      if (typeof(ack) === 'function') {
        ack(_chunk[position].msg, position);
      }
      self.push(_chunk[position].msg);
      delete _chunk[position];
      L4.isEnabledFor('info') && L4.log('info', payloadTrail.add({
        index: position
      }).toMessage({
        text: 'doRead() - pushed'
      }));
    } else {
      L4.isEnabledFor('info') && L4.log('info', payloadTrail.add({
        index: position
      }).toMessage({
        text: 'doRead() - waiting'
      }));
      _notifier.once('add', function() {
        doRead(position);
      });
    }
  }

  self._read = function(size) {
    doRead(_index++);
  }

  self.addChunk = function(index, chunk, callback) {
    if (chunk && 0 <= index && _chunk[index] === undefined) {
      L4.isEnabledFor('info') && L4.log('info', payloadTrail.add({
        index: index
      }).toMessage({
        text: 'addChunk() - inserted'
      }));
      _chunk[index] = { msg: misc.bufferify(chunk), ack: callback };
      _max = (_max < index) ? index : _max;
      _notifier.emit('add', {});
    } else {
      L4.isEnabledFor('info') && L4.log('info', payloadTrail.add({
        index: index
      }).toMessage({
        text: 'addChunk() - skipped'
      }));
    }
  }

  self.raiseFinal = function(total) {
    total = total || (_max + 1);
    L4.isEnabledFor('info') && L4.log('info', payloadTrail.add({
      index: _index,
      total: total
    }).toMessage({
      text: 'raiseFinal() - total'
    }));
    _chunk[total] = { msg: null };
    _notifier.emit('add', {});
  }

  self.raiseError = function(error) {
    self.emit('error', error);
    L4.isEnabledFor('error') && L4.log('error', payloadTrail.add({
      index: _index
    }).toMessage({
      text: 'raiseError() - error'
    }));
  }

  L4.isEnabledFor('info') && L4.log('info', payloadTrail.toMessage({
    text: 'PayloadReader.new() end!'
  }));
}

util.inherits(PayloadReader, Readable);


var L3 = LogAdapter.getLogger({ scope: 'opflow:task:PayloadWriter' });

var PayloadWriter = function(next, kwargs) {
  kwargs = kwargs || {};
  Writable.call(this, {objectMode: (kwargs.objectMode === true)});
  var payloadId = kwargs.payloadId || misc.getLogID();
  var payloadTrail = LogTracer.ROOT.branch({ key: 'payloadId', value: payloadId });

  var self = this;
  var _context = { count: 0 };
  var _notifier = new events.EventEmitter();
  var _next = next;

  L3.isEnabledFor('info') && L3.log('info', payloadTrail.toMessage({
    text: 'PayloadWriter.new()'
  }));

  this.count = function() {
    return _context.count;
  }

  this.forceClose = function(opts) {
    L3.isEnabledFor('info') && L3.log('info', payloadTrail.toMessage({
      tags: ['payloadWriter is interrupted'],
      text: 'payloadWriter is interrupted'
    }));
    this.emit('error', { message: 'payloadWriter is interrupted' });
  }

  this._write = function(chunk, encoding, callback) {
    if (kwargs.usePromise !== false) {
      _next(chunk, _context.count++).then(function(drained) {
        L3.isEnabledFor('info') && L3.log('info', payloadTrail.add({
          count: _context.count,
          drained: drained
        }).toMessage({
          text: 'processing has done'
        }));
        callback();
      }).catch(function(error) {
        L3.isEnabledFor('error') && L3.log('error', payloadTrail.add({
          count: _context.count
        }).toMessage({
          text: 'processing has failed'
        }));
        callback(error);
      });
    } else {
      try {
        drained = _next(chunk, _context.count++, _notifier);
        L3.isEnabledFor('info') && L3.log('info', payloadTrail.add({
          count: _context.count,
          drained: drained
        }).toMessage({
          text: 'processing has done'
        }));
        if (drained) {
          callback();
        } else {
          _notifier.once('drain', function() {
            callback();
          });
        }
      } catch (error) {
        L3.isEnabledFor('error') && L3.log('error', payloadTrail.add({
          count: _context.count
        }).toMessage({
          text: 'processing has failed'
        }));
        callback(error);
      }
    }
  }

  L3.isEnabledFor('info') && L3.log('info', payloadTrail.toMessage({
    text: 'PayloadWriter.new() end!'
  }));
}

util.inherits(PayloadWriter, Writable);


module.exports = {
  PayloadReader: PayloadReader,
  PayloadWriter: PayloadWriter,
  TimeoutHandler: TimeoutHandler,
  RepeatedTimer: RepeatedTimer
};