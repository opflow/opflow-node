'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var fs = require('fs');
var path = require('path');
var misc = require('./util');
var PubsubHandler = require('./pubsub');
var RpcMaster = require('./rpc_master');
var RpcWorker = require('./rpc_worker');
var Recycler = require('./recycler');
var Serverlet = require('./serverlet');
var LogTracer = require('./log_tracer');
var LogAdapter = require('./logadapter');
var L = LogAdapter.getLogger({ scope: 'opflow:loader' });

var OPFLOW_DEFAULT_CONFIG_NAME = 'opflow.conf';

var Loader = function() {
  var loaderCue = LogTracer.ROOT.copy();

  L.isEnabledFor('conlog') && L.log('conlog', loaderCue.add({
    message: 'Loader.new()'
  }).toString());

  var self = this;

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
      handler.start().then(function() {
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
      fullbase = path.join(opts.configDir || process.env['OPFLOW_CONFIG_DIR'] || '',
        opts.configName || process.env['OPFLOW_CONFIG_NAME'] || OPFLOW_DEFAULT_CONFIG_NAME);
    }
    if (fullbase == null) return config;

    config = _extendConfig(config, _getConfigPath(fullbase + '.json'), function(confFile) {
      return JSON.parse(fs.readFileSync(confFile, 'utf8'));
    }, opts);

    config = _extendConfig(config, _getConfigPath(fullbase + '.js'), function(confFile) {
      return require(confFile);
    }, opts);

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
      L.isEnabledFor('conlog') && L.log('conlog', loaderCue.add({
        message: 'Loading config file has completed',
        filename: filename,
        config: config
      }).toString());
    } catch (err) {
      L.isEnabledFor('conlog') && L.log('conlog', loaderCue.add({
        message: 'Error on loading config file',
        filename: filename
      }).toString());
      if (L.isEnabledFor('conlog')) {
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

  L.isEnabledFor('conlog') && L.log('conlog', loaderCue.add({
    message: 'Loader.new() end!'
  }).toString());
}

var _instance = null;

Object.defineProperty(Loader, 'instance', {
  get: function() {
    return _instance = _instance || new Loader();
  },
  set: function(value) {}
});

module.exports = Loader;
