'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var debug = require('debug');
var misc = require('./util');

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
    if (arguments.length === 2) {
      extendLogMeta(arguments[1]);
    }
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

  var extendLogMeta = function(log) {
    if (log !== null && typeof(log) === 'object') {
      var node = extractNode(log);
      if (!log.requestId) {
        logIdTree.registerNode(node);
      }
      return logIdTree.resolvePath(node, log);
    }
    return log;
  }

  var extractNode = function(log) {
    if (log === null || typeof(log) !== 'object') return null;

    var sandboxId = log.consumerId || log.producerId || log.sandboxId;
    if (log.requestId && sandboxId) {
      return {parentId: sandboxId, id: log.requestId, type: 'requestId'}
    }

    var engineId = log.engineId || log.pubsubHandlerId || log.rpcMasterId || log.rpcWorkerId;
    if (log.consumerId) {
      return {parentId: engineId, id: log.consumerId, type: 'consumerId'}
    }
    if (log.producerId) {
      return {parentId: engineId, id: log.producerId, type: 'producerId'}
    }
    if (log.sandboxId) {
      return {parentId: engineId, id: log.sandboxId, type: 'sandboxId'}
    }

    if (log.engineId) {
      return {parentId: log.instanceId, id: log.engineId, type: 'engineId'}
    }
    if (log.rpcWorkerId) {
      return {parentId: log.instanceId, id: log.rpcWorkerId, type: 'rpcWorkerId'}
    }
    if (log.rpcMasterId) {
      return {parentId: log.instanceId, id: log.rpcMasterId, type: 'rpcMasterId'}
    }
    if (log.pubsubHandlerId) {
      return {parentId: log.instanceId, id: log.pubsubHandlerId, type: 'pubsubHandlerId'}
    }
    if (log.recyclerId) {
      return {parentId: log.instanceId, id: log.recyclerId, type: 'recyclerId'}
    }
    if (log.supervisorId) {
      return {parentId: log.instanceId, id: log.supervisorId, type: 'supervisorId'}
    }

    if (log.instanceId) {
      return {id: log.instanceId, type: 'instanceId'}
    }

    return null;
  }
}

var LogIdTree = function(kwargs) {
  kwargs = kwargs || {};

  var treepathExtended = kwargs.treepathExtended || false;

  var idTree = {};

  var findNode = function(id, queue) {
    if (!id) return null;
    queue = queue || [idTree];
    var node = queue.pop();
    if (node) {
      if (node['id'] === id) return node;
      else {
        node['children'] = node['children'] || [];
        node['children'].forEach(function(child) {
          queue.push(child);
        });
        return findNode(id, queue);
      }
    }
    return null;
  }

  this.defineRoot = function(node) {
    node = node || {};
    idTree.id = node.id || idTree.id;
    idTree.type = node.type || idTree.type;
  }

  this.registerNode = function(node) {
    if (!node || !node.id || !node.type) {
      return false;
    }
    var parent = findNode(node.parentId);
    if (parent) {
      parent['children'] = parent['children'] || [];
      if (parent['children'].every(function(child) {
        return !child || (child.id !== node.id);
      })) {
        parent['children'].push({parent:parent, id: node.id, type: node.type});
      }
      return true;
    } else {
      return false;
    }
  }

  this.resolvePath = function(node, target) {
    if (node === null) return target;
    target = target || {};

    var treepath = [];
    var parent;
    if (node.parentId) {
      treepath.unshift(node.type + ':' + node.id);
      parent = findNode(node.parentId);
    } else {
      parent = findNode(node.id);
    }

    while (parent) {
      if (treepathExtended) {
        treepath.unshift(parent.type + ':' + parent.id);
      } else {
        target[parent.type] = parent.id;
      }
      parent = parent.parent;
    }
    if (treepathExtended) {
      target.treepath = treepath.join('>');
    }
    return target;
  }
}

var logIdTree = new LogIdTree();

logIdTree.defineRoot({ id: misc.instanceId, type: 'instanceId' });

var LX = LogAdapter.getLogger({ scope: 'opflow:log:adapter' });

LX.isEnabledFor('info') && LX.log('info', {
  instanceId: misc.instanceId,
  libraryInfo: misc.libraryInfo,
  note: 'Library information'});
