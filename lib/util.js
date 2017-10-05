'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var slugid = require('slugid');
var uuidV4 = require('uuid/v4');
var misc = {}

misc.getCurrentTime = function() {
  return new Date();
}

misc.getUUID = uuidV4;
misc.getLogID = process.env.OPFLOW_BASE64UUID ? slugid.v4 : uuidV4;

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
    setImmediate(emitter.emit.bind(emitter, 'ready', {}));
  }).catch(function(exception) {
    setImmediate(emitter.emit.bind(emitter, 'error', exception));
  }).finally(function() {
    asyncFuns.length = 0;
  });
}

misc.isConsoleLogEnabled = function() {
  return process.env.DEBUG;
}

module.exports = misc;
