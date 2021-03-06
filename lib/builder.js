'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var fs = require('fs');
var path = require('path');
var yaml = require('js-yaml');
var misc = require('./util');
var PubsubHandler = require('./pubsub');
var RpcMaster = require('./rpc_master');
var RpcWorker = require('./rpc_worker');
var Recycler = require('./recycler');
var Commander = require('./commander');
var Serverlet = require('./serverlet');
var LogTracer = require('logolite').LogTracer;
var LogAdapter = require('logolite').LogAdapter;
var LX = LogAdapter.getLogger({ scope: 'opflow:loader' });

var Builder = function() {
  var self = this;
  var loaderTrail = LogTracer.ROOT.copy();

  LX.isEnabledFor('conlog') && LX.log('conlog', loaderTrail.toMessage({
    text: 'Builder.new()'
  }));

  this.createPubsubHandler = function(opts) {
    return new Promise(function(onResolved, onRejected) {
      var handler = new PubsubHandler(_filterConfig(self.loadConfig(opts), [
        'uri', 'exchangeName', 'routingKey', 'otherKeys', 'applicationId',
        'subscriberName', 'recyclebinName', 'redeliveredLimit',  'verbose'
      ], 'pubsub'));
      handler.on('ready', function() {
        onResolved(handler);
      }).on('error', function(exception) {
        onRejected(exception);
      });
    });
  }

  this.createRpcMaster = function(opts) {
    return new Promise(function(onResolved, onRejected) {
      var handler = new RpcMaster(_filterConfig(self.loadConfig(opts), [
        'uri', 'exchangeName', 'routingKey', 'applicationId',
        'responseName', 'verbose'
      ], 'rpc_master'));
      handler.on('ready', function() {
        onResolved(handler);
      }).on('error', function(exception) {
        onRejected(exception);
      });
    });
  }

  this.createRpcWorker = function(opts) {
    return new Promise(function(onResolved, onRejected) {
      var handler = new RpcWorker(_filterConfig(self.loadConfig(opts), [
        'uri', 'exchangeName', 'routingKey', 'applicationId',
        'operatorName', 'responseName', 'verbose'
      ], 'rpc_worker'));
      handler.on('ready', function() {
        onResolved(handler);
      }).on('error', function(exception) {
        onRejected(exception);
      });
    });
  }

  this.createRecycler = function(opts) {
    return new Promise(function(onResolved, onRejected) {
      var handler = new Recycler(_filterConfig(self.loadConfig(opts), [
        'uri', 'subscriberName', 'recyclebinName', 'verbose'
      ], 'recycler'));
      handler.on('ready', function() {
        onResolved(handler);
      }).on('error', function(exception) {
        onRejected(exception);
      });
    });
  }

  this.createCommander = function(opts) {
    return new Promise(function(onResolved, onRejected) {
      var _config = self.loadConfig(opts);
      var handler = new Commander({
        autoinit: false,
        configurer: _filterConfig(_config, [
          'uri', 'exchangeName', 'routingKey', 'applicationId', 'verbose', 'enabled'
        ], 'commander', 'configurer'),
        rpcMaster: _filterConfig(_config, [
          'uri', 'exchangeName', 'routingKey', 'applicationId', 'verbose', 'enabled',
          'responseName'
        ], 'commander', 'rpcMaster'),
        publisher: _filterConfig(_config, [
          'uri', 'exchangeName', 'routingKey', 'applicationId', 'verbose', 'enabled'
        ], 'commander', 'publisher'),
      });
      handler.ready().then(function() {
        onResolved(handler);
      }).catch(function(errors) {
        handler.close();
        return Promise.reject(errors);
      });
    });
  }

  this.createServerlet = function(handlers, opts) {
    return new Promise(function(onResolved, onRejected) {
      var _config = self.loadConfig(opts);
      var handler = new Serverlet(handlers, {
        autoinit: false,
        configurer: _filterConfig(_config, [
          'uri', 'exchangeName', 'routingKey', 'applicationId', 'verbose', 'enabled'
        ], 'serverlet', 'configurer'),
        rpcWorker: _filterConfig(_config, [
          'uri', 'exchangeName', 'routingKey', 'applicationId', 'verbose', 'enabled',
          'operatorName', 'responseName'
        ], 'serverlet', 'rpcWorker'),
        subscriber: _filterConfig(_config, [
          'uri', 'exchangeName', 'routingKey', 'applicationId', 'verbose', 'enabled',
          'subscriberName', 'recyclebinName', 'consumerTotal'
        ], 'serverlet', 'subscriber'),
      });
      handler.ready().then(function() {
        onResolved(handler);
      }).catch(function(errors) {
        handler.close();
        return Promise.reject(errors);
      });
    });
  }

  this.loadConfig = function(opts) {
    opts = opts || {};

    var config = lodash.isObject(opts.default) ? lodash.cloneDeep(opts.default) : {};
    
    var fullbase = null;
    if (opts.skipConfigFile !== true) {
      fullbase = path.join(opts.configDir || misc.getConfigDir(),
        opts.configName || misc.getConfigName());
    }
    if (fullbase == null) return config;

    var extname = path.extname(fullbase);
    if (['.json', '.js', '.yml'].indexOf(extname) < 0) extname = null;
    var extfile = null;

    if (extname === '.json' || extname === null) {
      extfile = (extname === null) ? '.json' : '';
      config = _extendConfig(config, _getConfigPath(fullbase + extfile), function(confFile) {
        return JSON.parse(fs.readFileSync(confFile, 'utf8'));
      }, opts);
    }

    if (extname === '.js' || extname === null) {
      extfile = (extname === null) ? '.js' : '';
      config = _extendConfig(config, _getConfigPath(fullbase + extfile), function(confFile) {
        return require(confFile);
      }, opts);
    }

    if (extname === '.yml' || extname === null) {
      extfile = (extname === null) ? '.yml' : '';
      config = _extendConfig(config, _getConfigPath(fullbase + extfile), function(confFile) {
        return yaml.safeLoad(fs.readFileSync(confFile, 'utf8'));
      }, opts);
    }

    return config;
  }

  var _getConfigPath = function(configName) {
    if (path.isAbsolute(configName)) return configName;
    return path.join(_getRootDir(), configName);
  }

  var _getRootDir = function() {
    return path.dirname(require.main.filename);
  }

  var _extendConfig = function(config, filename, fileLoader, options) {
    try {
      var confObject = fileLoader(filename);
      if (lodash.isObject(confObject)) {
        config = lodash.merge(config, confObject);
      }
      LX.isEnabledFor('conlog') && LX.log('conlog', loaderTrail.add({
        filename: filename,
        config: config
      }).toMessage({
        text: 'Loading config file: {filename} has completed'
      }));
    } catch (err) {
      LX.isEnabledFor('conlog') && LX.log('conlog', loaderTrail.add({
        filename: filename
      }).toMessage({
        text: 'Error on loading config file: {filename}'
      }));
      if (misc.isConsoleLogEnabled()) {
        options && options.verbose && console.log('Error object: ', err);
      }
    }
    return config;
  }

  var _filterConfig = function(config, fields, scope, subscope) {
    var kwargs = {};
    lodash.forEach(fields, function(field) {
      kwargs[field] = subscope && scope && lodash.get(config, ['opflow', scope, subscope, field]);
      if (lodash.isUndefined(kwargs[field])) {
        kwargs[field] = scope && lodash.get(config, ['opflow', scope, field]);
      }
      if (lodash.isUndefined(kwargs[field])) {
        kwargs[field] = lodash.get(config, ['opflow', field]);
      }
    });
    return kwargs;
  }

  LX.isEnabledFor('conlog') && LX.log('conlog', loaderTrail.toMessage({
    text: 'Builder.new() end!'
  }));
}

var _instance = null;

Object.defineProperty(Builder, 'instance', {
  get: function() {
    return _instance = _instance || new Builder();
  },
  set: function(value) {}
});

module.exports = Builder;
