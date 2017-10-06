'use strict';

var Promise = require('bluebird');
var Set = require('collections/set');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var errors = require('./exception');
var misc = require('./util');
var PubsubHandler = require('./pubsub');
var RpcMaster = require('./rpc_master');
var LogTracer = require('./log_tracer');
var LogAdapter = require('./log_adapter');
var LX = LogAdapter.getLogger({ scope: 'opflow:commander' });

var Commander = function(kwargs) {
  var self = this;
  events.EventEmitter.call(this);

  var commanderId = misc.getLogID();
  var commanderTrail = LogTracer.ROOT.branch({ key:'commanderId', value:commanderId });

  LX.isEnabledFor('info') && LX.log('info', commanderTrail.add({
    message: 'Commander.new()'
  }).toString());

  kwargs = kwargs || {};

  LX.isEnabledFor('debug') && LX.log('debug', commanderTrail.add({
    message: 'Before processing connection parameters',
    params: kwargs
  }).toString());

  var configurerCfg, rpcMasterCfg, publisherCfg;

  if (lodash.isObject(kwargs.configurer) && kwargs.configurer.enabled !== false) {
    configurerCfg = lodash.defaults({ autoinit: false }, kwargs.configurer, {
      engineId: misc.getLogID()
    }, lodash.pick(kwargs, ['uri', 'applicationId']));
  }
  if (lodash.isObject(kwargs.rpcMaster) && kwargs.rpcMaster.enabled !== false) {
    rpcMasterCfg = lodash.defaults({ autoinit: false }, kwargs.rpcMaster, {
      engineId: misc.getLogID()
    }, lodash.pick(kwargs, ['uri', 'applicationId']));
  }
  if (lodash.isObject(kwargs.publisher) && kwargs.publisher.enabled !== false) {
    publisherCfg = lodash.defaults({ autoinit: false }, kwargs.publisher, {
      engineId: misc.getLogID()
    }, lodash.pick(kwargs, ['uri', 'applicationId']));
  }

  LX.isEnabledFor('conlog') && LX.log('conlog', commanderTrail.add({
    message: 'Connection parameters after processing',
    configurerCfg: configurerCfg,
    rpcMasterCfg: rpcMasterCfg,
    publisherCfg: publisherCfg
  }).toString());

  var configurer, rpcMaster, publisher;

  if (lodash.isObject(configurerCfg)) {
    LX.isEnabledFor('info') && LX.log('info', commanderTrail.add({
      message: 'Create Configurer[PubsubHandler]',
      engineId: configurerCfg.engineId
    }).toString());
    configurer = new PubsubHandler(configurerCfg);
  }
  if (lodash.isObject(rpcMasterCfg)) {
    LX.isEnabledFor('info') && LX.log('info', commanderTrail.add({
      message: 'Create Manipulator[RpcMaster]',
      engineId: rpcMasterCfg.engineId
    }).toString());
    rpcMaster = new RpcMaster(rpcMasterCfg);
  }
  if (lodash.isObject(publisherCfg)) {
    LX.isEnabledFor('info') && LX.log('info', commanderTrail.add({
      message: 'Create Publisher[PubsubHandler]',
      engineId: publisherCfg.engineId
    }).toString());
    publisher = new PubsubHandler(publisherCfg);
  }

  this.ready = function(opts) {
    var readyTrail = commanderTrail.copy();
    opts = opts || {};
    opts.silent !== true && LX.isEnabledFor('info') && LX.log('info', readyTrail.add({
      message: 'ready() running'
    }).toString());
    var actions = [];
    if (configurer) actions.push(configurer.ready());
    if (rpcMaster) actions.push(rpcMaster.ready());
    if (publisher) actions.push(publisher.ready());
    var ok = Promise.all(actions);
    if (opts.silent !== true) return ok.then(function(results) {
      LX.isEnabledFor('info') && LX.log('info', readyTrail.add({
        message: 'ready() has completed'
      }).toString());
      return results;
    }).catch(function(errors) {
      LX.isEnabledFor('info') && LX.log('info', readyTrail.add({
        message: 'ready() has failed'
      }).toString());
      return Promise.reject(errors);
    });
    return ok;
  }

  this.close = function() {
    var closeTrail = commanderTrail.copy();
    LX.isEnabledFor('info') && LX.log('info', closeTrail.add({
      message: 'close() running'
    }).toString());
    var actions = [];
    if (configurer) actions.push(configurer.close());
    if (rpcMaster) actions.push(rpcMaster.close());
    if (publisher) actions.push(publisher.close());
    return Promise.all(actions).then(function(results) {
      LX.isEnabledFor('info') && LX.log('info', closeTrail.add({
        message: 'close() has completed'
      }).toString());
      return results;
    }).catch(function(errors) {
      LX.isEnabledFor('info') && LX.log('info', closeTrail.add({
        message: 'close() has failed'
      }).toString());
      return Promise.reject(errors);
    });
  }

  self.registerMethod = function(descriptor, target) {
    target = target || {};
    var routineId = descriptor.routineId || descriptor.signature || descriptor.name;
    Object.defineProperty(target, descriptor.name, {
      get: function() {
        return function() {
          var args = Array.prototype.slice.call(arguments);
          return assertRpcMaster().then(function(handler) {
            return handler.request(routineId, args);
          }).then(function(task) {
            return task.extractResult().then(function(result) {
              if (result.failed) return Promise.reject(result.error);
              if (result.completed) return result.value;
            });
          });
        }
      },
      set: function(val) {}
    });
    return target;
  }

  self.registerMethods = function(descriptors, target) {

  }

  var assertRpcMaster = function() {
    if (rpcMaster) return Promise.resolve(rpcMaster);
    return Promise.reject();
  }

  if (kwargs.autoinit !== false) {
    LX.isEnabledFor('debug') && LX.log('debug', commanderTrail.add({
      message: 'auto execute ready()'
    }).toString());
    misc.notifyConstructor(this.ready(), this);
  }

  LX.isEnabledFor('info') && LX.log('info', commanderTrail.add({
    message: 'Commander.new() end!'
  }).toString());
}

module.exports = Commander;

util.inherits(Commander, events.EventEmitter);
