'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var fs = require('fs');
var path = require('path');
var debugx = require('debug')('opflow:loader');
var misc = require('./util');

var OPFLOW_CONFIGURATION_FILE = 'opflow.conf';

var Loader = function(kwargs) {
  debugx.enabled && debugx(' + constructor begin ...');

  kwargs = kwargs || {};

  this.loadConfig = function(opts) {
    opts = opts || {};

    var config = lodash.isObject(opts.default) ? lodash.cloneDeep(opts.default) : {};
    var basename = (opts.basename || OPFLOW_CONFIGURATION_FILE);

    config = _mergeConfig(config, _getConfigPath(basename + '.json'), function(confFile) {
      return JSON.parse(fs.readFileSync(confFile, 'utf8'));
    });

    config = _mergeConfig(config, _getConfigPath(basename + '.js'), function(confFile) {
      return require(confFile);
    });

    return config;
  }

  debugx.enabled && debugx(' - constructor end!');
}

var _getConfigPath = function(basename) {
  if (path.isAbsolute(basename)) return basename;
  return path.join(_getRootDir(), basename);
}

var _getRootDir = function() {
  // return __dirname;
  // return process.cwd();
  return path.dirname(require.main.basename);
}

var _mergeConfig = function(config, filename, fileLoader) {
  try {
    var confObject = fileLoader(filename);
    if (lodash.isObject(confObject)) {
      config = lodash.merge(config, confObject);
    }
    debugx.enabled && debugx('load config from file [%s]: %s', filename, JSON.stringify(config));
  } catch (err) {
    if (debugx.enabled) {
      debugx('Error on loading config from [%s]', filename);
      console.log('Error object: ', err);
    }
  }
  return config;
}

var _instance = null;

Object.defineProperty(Loader, 'instance', {
  get: function() {
    return _instance = _instance || new Loader();
  },
  set: function(value) {}
});

module.exports = Loader;
