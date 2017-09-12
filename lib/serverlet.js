'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var PubsubHandler = require('./pubsub');
var RpcWorker = require('./rpc_worker');
var LogAdapter = require('./logadapter');
var LX = LogAdapter.getLogger({ scope: 'opflow:serverlet' });

var AUTOINIT = { autoinit: false }

var Serverlet = function(kwargs) {
  kwargs = kwargs || {};

  var configurer, rpcWorker, subscriber;
  
  if (lodash.isObject(kwargs.configurer)) {
    configurer = new PubsubHandler(lodash.defaults(AUTOINIT,
      lodash.pick(kwargs, ['uri', 'applicationId']),
      lodash.omit(kwargs.configurer, ['handler'])));
  }

  if (lodash.isObject(kwargs.rpcWorker)) {
    rpcWorker = new RpcWorker(lodash.defaults(AUTOINIT,
      lodash.pick(kwargs, ['uri', 'applicationId']),
      lodash.omit(kwargs.rpcWorker, ['mappings'])));
  }

  if (lodash.isObject(kwargs.subscriber)) {
    subscriber = new PubsubHandler(lodash.defaults(AUTOINIT,
      lodash.pick(kwargs, ['uri', 'applicationId']),
      lodash.omit(kwargs.subscriber, ['handler'])));
  }

  this.start = function() {
    var actions = [];

    if (configurer) {
      actions.push(configurer.ready());
      if (lodash.isFunction(kwargs.configurer.handler)) {
        actions.push(configurer.subscribe(kwargs.configurer.handler));
      }
    }

    if (rpcWorker) {
      actions.push(rpcWorker.ready());
      var mappings = lodash.filter(kwargs.rpcWorker.mappings, function(mapping) {
        return lodash.isString(mapping.routineId) && lodash.isFunction(mapping.handler);
      });
      actions.push(Promise.mapSeries(mappings, function(mapping) {
        return rpcWorker.process(mapping.routineId, mapping.handler);
      }));  
    }

    if (subscriber) {
      actions.push(subscriber.ready());
      if (lodash.isFunction(kwargs.subscriber.handler)) {
        var consumerTotal = kwargs.subscriber.consumerTotal;
        if (!lodash.isInteger(consumerTotal) || consumerTotal <= 0) consumerTotal = 1;
        var consumers = Promise.all(lodash.range(consumerTotal).map(function(item) {
          return subscriber.subscribe(kwargs.subscriber.handler);
        }));
        actions.push(consumers);
      }
    }

    return Promise.all(actions);
  }

  this.stop = function() {
    var actions = [];
    if (configurer) actions.push(configurer.close());
    if (rpcWorker) actions.push(rpcWorker.close());
    if (subscriber) actions.push(subscriber.close());
    return Promise.all(actions);
  }
}

module.exports = Serverlet;