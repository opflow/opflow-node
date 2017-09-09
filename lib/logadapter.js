'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var debug = require('debug');

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
  var store = { realLogger: detector() };

  this.getLogger = function(kwargs) {
    kwargs = kwargs || {};
    kwargs.store = store;
    return new Logger(kwargs);
  }

  this.connectTo = function(logger) {
    if (logger) {
      store.realLogger = logger;
    }
  }
})();

module.exports = LogAdapter;

var Logger = function(kwargs) {
  var self = this;
  var defaultLog = debug(kwargs.scope || 'opflow:default');
  var realLogger = kwargs.store.realLogger;

  self.isEnabledFor = function(level) {
    if (process.env.OPFLOW_CONLOG || level === 'conlog') return defaultLog.enabled;
    return realLogger != null && realLogger[level] !== undefined;
  }

  self.log = function(level) {
    if (process.env.OPFLOW_CONLOG || level === 'conlog') {
      if (arguments.length === 2 && typeof(arguments[1]) === 'object') {
        defaultLog(JSON.stringify(arguments[1]));
      } else {
        var logargs = Array.prototype.slice.call(arguments, 1);
        defaultLog.apply(null, logargs);
      }
      return;
    }
    realLogger && realLogger.log.apply(realLogger, arguments);
  }
}
