'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var debug = require('debug');
var winston = require('winston');

var LogAdapter = new (function() {
  var store = { realLogger: winston };

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

  self.isEnabledFor = function(level) {
    if (level === 'verbose') return defaultLog.enabled;
    return kwargs.store.realLogger[level] !== undefined;
  }

  self.log = function(level) {
    if (level === 'verbose') {
      var logargs = Array.prototype.slice.call(arguments, 1);
      defaultLog.apply(null, logargs);
    } else {
      kwargs.store.realLogger.log.apply(kwargs.store.realLogger, arguments);
    }
  }
}