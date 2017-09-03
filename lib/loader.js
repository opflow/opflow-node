'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var fs = require('fs');
var path = require('path');
var debugx = require('debug')('opflow:loader');
var misc = require('./util');
var PubsubHandler = require('./pubsub');

var OPFLOW_CONFIGURATION_FILE = 'opflow.conf';

var Loader = function() {
  debugx.enabled && debugx(' + constructor begin ...');

  this.createPubsubHandler = function(opts) {
    return new PubsubHandler(_filterConfig(this.loadConfig(opts), 'pubsub', [
      'uri', 'exchangeName', 'routingKey', 'otherKeys', 'applicationId',
      'subscriberName', 'recyclebinName', 'redeliveredLimit',  'verbose'
    ]));
  }

  this.loadConfig = function(opts) {
    opts = opts || {};

    var config = lodash.isObject(opts.default) ? lodash.cloneDeep(opts.default) : {};
    var basename = (opts.basename || OPFLOW_CONFIGURATION_FILE);

    config = _extendConfig(config, _getConfigPath(basename + '.json'), function(confFile) {
      return JSON.parse(fs.readFileSync(confFile, 'utf8'));
    }, opts);

    config = _extendConfig(config, _getConfigPath(basename + '.js'), function(confFile) {
      return require(confFile);
    }, opts);

    return config;
  }

  debugx.enabled && debugx(' - constructor end!');
}

var _getConfigPath = function(basename) {
  if (path.isAbsolute(basename)) return basename;
  return path.join(_getRootDir(), basename);
}

var _getRootDir = function() {
  return path.dirname(require.main.basename);
}

var _extendConfig = function(config, filename, fileLoader, options) {
  try {
    var confObject = fileLoader(filename);
    if (lodash.isObject(confObject)) {
      config = lodash.merge(config, confObject);
    }
    debugx.enabled && debugx('load config from file [%s]: %s', filename, JSON.stringify(config));
  } catch (err) {
    if (debugx.enabled) {
      debugx('Error on loading config from [%s]', filename);
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
