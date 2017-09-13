'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var errors = require('./exception');
var PubsubHandler = require('./pubsub');
var RpcWorker = require('./rpc_worker');
var LogAdapter = require('./logadapter');
var LX = LogAdapter.getLogger({ scope: 'opflow:serverlet' });

var AUTOINIT = { autoinit: false }

var Serverlet = function(handlers, kwargs) {
  handlers = lodash.clone(handlers || {});
  if (!handlers.configurer && !handlers.rpcWorker && !handlers.configurer) {
    throw new errors.BootstrapError('Should provide at least one handler');
  }
  if (handlers.configurer && !lodash.isFunction(handlers.configurer)) {
    throw new errors.BootstrapError('Configurer handler should be a function');
  }
  if (handlers.rpcWorker && !lodash.isArray(handlers.rpcWorker)) {
    throw new errors.BootstrapError('RpcWorker handlers should be an array');
  }
  if (handlers.configurer && !lodash.isFunction(handlers.configurer)) {
    throw new errors.BootstrapError('Configurer handler should be a function');
  }

  kwargs = kwargs || {};
  var configurerCfg, rpcWorkerCfg, subscriberCfg;

  if (lodash.isObject(kwargs.configurer)) {
    configurerCfg = lodash.defaults(AUTOINIT, kwargs.configurer,
      lodash.pick(kwargs, ['uri', 'applicationId']));
  }
  if (lodash.isObject(kwargs.rpcWorker)) {
    rpcWorkerCfg = lodash.defaults(AUTOINIT, kwargs.rpcWorker,
      lodash.pick(kwargs, ['uri', 'applicationId']));
  }
  if (lodash.isObject(kwargs.subscriber)) {
    subscriberCfg = lodash.defaults(AUTOINIT, kwargs.subscriber,
      lodash.pick(kwargs, ['uri', 'applicationId']));
  }

  if (lodash.isObject(configurerCfg)) {
    if (!configurerCfg.uri || !configurerCfg.exchangeName || !configurerCfg.routingKey) {
      throw new errors.BootstrapError('Invalid Configurer connection parameters');
    }
  }

  if (lodash.isObject(rpcWorkerCfg)) {
    if (!rpcWorkerCfg.uri || !rpcWorkerCfg.exchangeName || !rpcWorkerCfg.routingKey) {
      throw new errors.BootstrapError('Invalid RpcWorker connection parameters');
    }
  }

  if (lodash.isObject(subscriberCfg)) {
    if (!subscriberCfg.uri || !subscriberCfg.exchangeName || !subscriberCfg.routingKey) {
      throw new errors.BootstrapError('Invalid Subscriber connection parameters');
    }
  }

  var configurer, rpcWorker, subscriber;
  
  if (lodash.isObject(configurerCfg)) {
    configurer = new PubsubHandler(configurerCfg);
  }
  if (lodash.isObject(rpcWorkerCfg)) {
    rpcWorker = new RpcWorker(rpcWorkerCfg);
  }
  if (lodash.isObject(subscriberCfg)) {
    subscriber = new PubsubHandler(subscriberCfg);
  }

  this.ready = function() {
    var actions = [];

    if (configurer && handlers.configurer) {
      actions.push(configurer.ready());
      if (lodash.isFunction(handlers.configurer)) {
        actions.push(configurer.subscribe(handlers.configurer));
      }
    }

    if (rpcWorker && handlers.rpcWorker) {
      actions.push(rpcWorker.ready());
      var mappings = lodash.filter(handlers.rpcWorker, function(mapping) {
        return lodash.isString(mapping.routineId) && lodash.isFunction(mapping.handler);
      });
      actions.push(Promise.mapSeries(mappings, function(mapping) {
        return rpcWorker.process(mapping.routineId, mapping.handler);
      }));  
    }

    if (subscriber && handlers.subscriber) {
      actions.push(subscriber.ready());
      if (lodash.isFunction(handlers.subscriber)) {
        var consumerTotal = kwargs.subscriber.consumerTotal;
        if (!lodash.isInteger(consumerTotal) || consumerTotal <= 0) consumerTotal = 1;
        var consumers = Promise.all(lodash.range(consumerTotal).map(function(item) {
          return subscriber.subscribe(handlers.subscriber);
        }));
        actions.push(consumers);
      }
    }

    return Promise.all(actions);
  }

  this.close = function() {
    var actions = [];
    if (configurer) actions.push(configurer.close());
    if (rpcWorker) actions.push(rpcWorker.close());
    if (subscriber) actions.push(subscriber.close());
    return Promise.all(actions);
  }
}

module.exports = Serverlet;