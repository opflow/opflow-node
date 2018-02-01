'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var LogTracer = require('logolite').LogTracer;
var misc = {}

misc.getCurrentTime = function() {
  return new Date();
}

misc.getLogID = LogTracer.getLogID;

misc.stringify = function(data) {
  return (typeof(data) === 'string') ? data : JSON.stringify(data);
}

misc.bufferify = function(data) {
  return (data instanceof Buffer) ? data : new Buffer(this.stringify(data));
}

misc.getHeaderField = function(headers, fieldName, uuidIfNotFound, defVal) {
  if (headers == null || headers[fieldName] == null) {
    return (uuidIfNotFound) ? this.getLogID() : defVal;
  }
  return headers[fieldName];
}

misc.getRequestId = function(headers, uuidIfNotFound) {
  if (typeof(uuidIfNotFound) == 'undefined') uuidIfNotFound = true;
  return this.getHeaderField(headers, 'requestId', uuidIfNotFound);
}

misc.getRoutineId = function(headers, uuidIfNotFound) {
  if (typeof(uuidIfNotFound) == 'undefined') uuidIfNotFound = false;
  return this.getHeaderField(headers, 'routineId', uuidIfNotFound);
}

misc.defaultQueueParams = function(config) {
  return lodash.defaults(config || {}, {
    durable: true,
    exclusive: false,
    autoDelete: false
  });
}

misc.notifyConstructor = function(asyncFuns, emitter) {
  Promise.all(asyncFuns).then(function(qoks) {
    process.nextTick(emitter.emit.bind(emitter, 'ready', {}));
  }).catch(function(exception) {
    process.nextTick(emitter.emit.bind(emitter, 'error', exception));
  }).finally(function() {
    asyncFuns.length = 0;
  });
}

misc.isConsoleLogEnabled = function() {
  return process.env.DEBUG;
}

var _autoRecoveryLevel;

misc.isAutoRecoveryLevel = function() {
  _autoRecoveryLevel = _autoRecoveryLevel ||
      parseInt(process.env.OPFLOW_AUTO_RECOVERY_LEVEL) || 2;
  return _autoRecoveryLevel;
}

misc.buildURI = function(params) {
  params = params || {};
  var conargs = {};
  var _uri = [];

  conargs.protocol = 'amqp';
  _uri.push(conargs.protocol, '://');

  conargs.username = params.username;
  if (conargs.username) {
    _uri.push(conargs.username);
    conargs.password = params.password;
    if (conargs.password) {
      _uri.push(':', conargs.password);
    }
    _uri.push('@');
  }

  conargs.host = params.host || 'localhost';
  _uri.push(conargs.host);

  if (lodash.isInteger(params.port)) {
    conargs.port = params.port;
    _uri.push(':', conargs.port);
  }

  conargs.virtualHost = params.virtualHost;
  if (conargs.virtualHost) {
    _uri.push('/', conargs.virtualHost);
  }

  var opts = [];
  ['channelMax', 'frameMax', 'heartbeat', 'locale'].forEach(function(name) {
    if (params[name]) {
      conargs[name] = params[name];
      opts.push(name + '=' + conargs[name]);
    }
  });

  if (opts.length > 0) {
    _uri.push('?', opts.join('&'));
  }

  conargs.uri = _uri.join('');
  return conargs;
}

misc.checkToExit = function(error, opts) {
  opts = opts || {};
  var skips = (process.env.OPFLOW_SKIPEXITONERROR || '').split(',');
  if (opts.methodName && skips.indexOf(opts.methodName) >= 0) return;
  if (['EHOSTUNREACH'].indexOf(error.code) >= 0) {
    console.error('Fatal error: %s - %s. Exit!', error.code, error.message);
    process.exit(1);
  }
}

misc.getEmptyFunc = function() {
  return function() {};
}

module.exports = misc;
