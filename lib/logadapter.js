'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var debug = require('debug');

var LogAdapter = new (function() {
  this.getLogger = function(kwargs) {
    kwargs = kwargs || {};
    return new Logger(kwargs);
  }
})();

module.exports = LogAdapter;

var Logger = function(kwargs) {
  var self = this;
  var defaultLog = debug(kwargs.scope || 'opflow:default');

  self.isEnabledFor = function(level) {
    return defaultLog.enabled;
  }

  self.log = function(level) {
    var logargs = Array.prototype.slice.call(arguments, 1);
    defaultLog.apply(defaultLog, logargs);
  }
}
