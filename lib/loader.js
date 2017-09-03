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
var LogAdapter = require('./logadapter');
var L = LogAdapter.getLogger({ scope: 'opflow:loader' });

var OPFLOW_CONFIGURATION_FILE = 'opflow.conf';

var Loader = function() {
  L.isEnabledFor('verbose') && L.log('verbose', ' + constructor begin ...');

  this.createPubsubHandler = function(opts) {
    return new PubsubHandler(_filterConfig(this.loadConfig(opts), 'pubsub', [
      'uri', 'exchangeName', 'routingKey', 'otherKeys', 'applicationId',
      'subscriberName', 'recyclebinName', 'redeliveredLimit',  'verbose'
    ]));
  }

  this.createRpcMaster = function(opts) {
    return new RpcMaster(_filterConfig(this.loadConfig(opts), 'rpc_master', [
      'uri', 'exchangeName', 'routingKey', 'applicationId',
      'responseName', 'verbose'
    ]));
  }

  this.createRpcWorker = function(opts) {
    return new RpcWorker(_filterConfig(this.loadConfig(opts), 'rpc_worker', [
      'uri', 'exchangeName', 'routingKey', 'applicationId',
      'operatorName', 'responseName', 'verbose'
    ]));
  }

  this.createRecycler = function(opts) {
    return new Recycler(_filterConfig(this.loadConfig(opts), 'recycler', [
      'uri', 'subscriberName', 'recyclebinName',  'verbose'
    ]));
  }

  this.loadConfig = function(opts) {
    opts = opts || {};

    var config = lodash.isObject(opts.default) ? lodash.cloneDeep(opts.default) : {};
    var fullbase = path.join(opts.configDir || '', opts.configName || OPFLOW_CONFIGURATION_FILE);

    config = _extendConfig(config, _getConfigPath(fullbase + '.json'), function(confFile) {
      return JSON.parse(fs.readFileSync(confFile, 'utf8'));
    }, opts);

    config = _extendConfig(config, _getConfigPath(fullbase + '.js'), function(confFile) {
      return require(confFile);
    }, opts);

    return config;
  }

  L.isEnabledFor('verbose') && L.log('verbose', ' - constructor end!');
}

var _getConfigPath = function(configName) {
  if (path.isAbsolute(configName)) return configName;
  return path.join(_getRootDir(), configName);
}

var _getRootDir = function() {
  return path.dirname(require.main.configName);
}

var _extendConfig = function(config, filename, fileLoader, options) {
  try {
    var confObject = fileLoader(filename);
    if (lodash.isObject(confObject)) {
      config = lodash.merge(config, confObject);
    }
    L.isEnabledFor('verbose') && L.log('verbose', 'load config from file [%s]: %s', filename, JSON.stringify(config));
  } catch (err) {
    if (L.isEnabledFor('verbose')) {
      L.log('verbose', 'Error on loading config from [%s]', filename);
      options && options.verbose && console.log('Error object: ', err);
    }
  }
  return config;
}

var _filterConfig = function(config, scope, fields) {
  var kwargs = {};
  lodash.forEach(fields, function(field) {
    kwargs[field] = lodash.get(config, ['opflow', scope, field]) || 
        lodash.get(config, ['opflow', field]);
  });
  return kwargs;
}

var _instance = null;

Object.defineProperty(Loader, 'instance', {
  get: function() {
    return _instance = _instance || new Loader();
  },
  set: function(value) {}
});

module.exports = Loader;
