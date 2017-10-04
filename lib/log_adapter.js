'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var debug = require('debug');
var misc = require('./util');
var LogTracer = require('./log_tracer');
var LOG_LEVELS = ['none', 'fatal', 'error', 'warn', 'info', 'debug', 'trace', 'conlog'];

var Logger = function(kwargs) {
  var self = this;
  var defaultLog = debug(kwargs.scope || 'opflow:default');
  var realLogger = kwargs.store.realLogger;

  self.isEnabledFor = function(level) {
    if (process.env.OPFLOW_LOGTRACER) {
      return LOG_LEVELS.indexOf(process.env.OPFLOW_LOGTRACER) >= LOG_LEVELS.indexOf(level);
    }
    if (process.env.OPFLOW_CONLOG || level === 'conlog') return defaultLog.enabled;
    return realLogger != null && realLogger[level] !== undefined;
  }

  self.log = function(level) {
    if (process.env.OPFLOW_CONLOG || level === 'conlog') {
      var logargs = Array.prototype.slice.call(arguments, 1);
      defaultLog.apply(null, logargs);
      return;
    }
    realLogger && realLogger.log.apply(realLogger, arguments);
  }
}

var detector = function() {
  try {
    var winston = require('winston');
    winston.configure({
      transports: [
        new winston.transports.Console({
          level: 'debug',
          json: false,
          timestamp: true,
          colorize: true
        })
      ]
    });
    return winston;
  } catch(err) {
    return null;
  }
};

var LogAdapter = new (function() {
  var store = { realLogger: null };

  this.getLogger = function(kwargs) {
    kwargs = kwargs || {};
    kwargs.store = store;
    return new Logger(kwargs);
  }

  this.connectTo = function(logger) {
    if (logger) {
      store.realLogger = logger;
    }
    var LX = this.getLogger({ scope: 'opflow:LogTracer' });
    LX.log('info', LogTracer.libraryInfoString);
  }

  this.connectTo(detector());
})();

module.exports = LogAdapter;
