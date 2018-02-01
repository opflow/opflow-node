'use strict';

var Promise = require('bluebird');
var Set = require('collections/set');
var lodash = require('lodash');
var events = require('events');
var util = require('util');
var errors = require('./exception');
var misc = require('./util');
var PubsubHandler = require('./pubsub');
var RpcWorker = require('./rpc_worker');
var LogTracer = require('logolite').LogTracer;
var LogAdapter = require('logolite').LogAdapter;
var LI = LogAdapter.getLogger({ scope: 'opflow:instantiator' });
var LX = LogAdapter.getLogger({ scope: 'opflow:serverlet' });

var Serverlet = function(handlers, kwargs) {
  events.EventEmitter.call(this);

  var serverletId = misc.getLogID();
  var serverletTrail = LogTracer.ROOT.branch({ key:'serverletId', value:serverletId });

  LX.isEnabledFor('info') && LX.log('info', serverletTrail.toMessage({
    text: 'Serverlet.new()'
  }));

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
  if (handlers.subscriber && !lodash.isFunction(handlers.subscriber)) {
    throw new errors.BootstrapError('Subscriber handler should be a function');
  }

  kwargs = kwargs || {};

  LX.isEnabledFor('debug') && LX.log('debug', serverletTrail.add({
    params: kwargs
  }).toMessage({
    text: 'Before processing connection parameters'
  }));

  var configurerCfg, rpcWorkerCfg, subscriberCfg;

  if (lodash.isObject(kwargs.configurer) && kwargs.configurer.enabled !== false) {
    configurerCfg = lodash.defaults({ autoinit: false }, kwargs.configurer, {
      engineId: misc.getLogID()
    }, lodash.pick(kwargs, ['uri', 'applicationId']));
  }
  if (lodash.isObject(kwargs.rpcWorker) && kwargs.rpcWorker.enabled !== false) {
    rpcWorkerCfg = lodash.defaults({ autoinit: false }, kwargs.rpcWorker, {
      engineId: misc.getLogID()
    }, lodash.pick(kwargs, ['uri', 'applicationId']));
  }
  if (lodash.isObject(kwargs.subscriber) && kwargs.subscriber.enabled !== false) {
    subscriberCfg = lodash.defaults({ autoinit: false }, kwargs.subscriber, {
      engineId: misc.getLogID()
    }, lodash.pick(kwargs, ['uri', 'applicationId']));
  }

  LX.isEnabledFor('conlog') && LX.log('conlog', serverletTrail.add({
    configurerCfg: configurerCfg,
    rpcWorkerCfg: rpcWorkerCfg,
    subscriberCfg: subscriberCfg
  }).toMessage({
    text: 'Connection parameters after processing'
  }));

  var exchangeKey_Set = new Set();
  var queue_Set = new Set();
  var recyclebin_Set = new Set();

  if (lodash.isObject(configurerCfg)) {
    if (!configurerCfg.uri || !configurerCfg.exchangeName || !configurerCfg.routingKey) {
      throw new errors.BootstrapError('Invalid Configurer connection parameters');
    }
    if (!exchangeKey_Set.add(configurerCfg.exchangeName + configurerCfg.routingKey)) {
      throw new errors.BootstrapError('Duplicated Configurer connection parameters');
    }
    if (configurerCfg.subscriberName && !queue_Set.add(configurerCfg.subscriberName)) {
      throw new errors.BootstrapError('Configurer[subscriberName] must not be duplicated');
    }
    if (configurerCfg.recyclebinName) recyclebin_Set.add(configurerCfg.recyclebinName);
  }

  if (lodash.isObject(rpcWorkerCfg)) {
    if (!rpcWorkerCfg.uri || !rpcWorkerCfg.exchangeName || !rpcWorkerCfg.routingKey) {
      throw new errors.BootstrapError('Invalid RpcWorker connection parameters');
    }
    if (!exchangeKey_Set.add(rpcWorkerCfg.exchangeName + rpcWorkerCfg.routingKey)) {
      throw new errors.BootstrapError('Duplicated RpcWorker connection parameters');
    }
    if (rpcWorkerCfg.operatorName && !queue_Set.add(rpcWorkerCfg.operatorName)) {
      throw new errors.BootstrapError('RpcWorker[operatorName] must not be duplicated');
    }
    if (rpcWorkerCfg.responseName && !queue_Set.add(rpcWorkerCfg.responseName)) {
      throw new errors.BootstrapError('RpcWorker[responseName] must not be duplicated');
    }
  }

  if (lodash.isObject(subscriberCfg)) {
    if (!subscriberCfg.uri || !subscriberCfg.exchangeName || !subscriberCfg.routingKey) {
      throw new errors.BootstrapError('Invalid Subscriber connection parameters');
    }
    if (!exchangeKey_Set.add(subscriberCfg.exchangeName + subscriberCfg.routingKey)) {
      throw new errors.BootstrapError('Duplicated Subscriber connection parameters');
    }
    if (subscriberCfg.subscriberName && !queue_Set.add(subscriberCfg.subscriberName)) {
      throw new errors.BootstrapError('Subscriber[subscriberName] must not be duplicated');
    }
    if (subscriberCfg.recyclebinName) recyclebin_Set.add(subscriberCfg.recyclebinName);
  }

  var common_Set = recyclebin_Set.intersection(queue_Set);
  if (common_Set.length > 0) {
    throw new errors.BootstrapError('Invalid recyclebinName (duplicated with some queueNames)');
  }

  var configurer, rpcWorker, instantiator, subscriber;
  
  if (lodash.isObject(configurerCfg)) {
    LX.isEnabledFor('info') && LX.log('info', serverletTrail.add({
      engineId: configurerCfg.engineId
    }).toMessage({
      text: 'Create Configurer[PubsubHandler]'
    }));
    configurer = new PubsubHandler(configurerCfg);
  }
  if (lodash.isObject(rpcWorkerCfg)) {
    LX.isEnabledFor('info') && LX.log('info', serverletTrail.add({
      engineId: rpcWorkerCfg.engineId
    }).toMessage({
      text: 'Create Manipulator[RpcWorker]'
    }));
    rpcWorker = new RpcWorker(rpcWorkerCfg);
    instantiator = new Instantiator({ instantiatorId: serverletId, rpcWorker: rpcWorker });
  }
  if (lodash.isObject(subscriberCfg)) {
    LX.isEnabledFor('info') && LX.log('info', serverletTrail.add({
      engineId: subscriberCfg.engineId
    }).toMessage({
      text: 'Create Subscriber[PubsubHandler]'
    }));
    subscriber = new PubsubHandler(subscriberCfg);
  }

  this.ready = function(opts) {
    var readyTrail = serverletTrail.copy();
    opts = opts || {};
    opts.silent !== true &&
    LX.isEnabledFor('info') && LX.log('info', readyTrail.toMessage({
      text: 'ready() running'
    }));
    var actions = [];
    if (configurer) actions.push(configurer.ready());
    if (rpcWorker) actions.push(rpcWorker.ready());
    if (subscriber) actions.push(subscriber.ready());
    var ok = Promise.all(actions);
    if (opts.silent !== true) return ok.then(function(results) {
      LX.isEnabledFor('info') && LX.log('info', readyTrail.toMessage({
        text: 'ready() has completed'
      }));
      return results;
    }).catch(function(errors) {
      LX.isEnabledFor('info') && LX.log('info', readyTrail.toMessage({
        text: 'ready() has failed'
      }));
      return Promise.reject(errors);
    });
    return ok;
  }

  this.registerRoutine = function() {
    instantiator.registerRoutine.apply(instantiator, arguments);
  }

  this.start = function() {
    var startTrail = serverletTrail.copy();
    LX.isEnabledFor('info') && LX.log('info', startTrail.toMessage({
      text: 'start() running'
    }));
    return this.ready({ silent: true }).then(function() {
      var actions = [];

      if (configurer && handlers.configurer && lodash.isFunction(handlers.configurer)) {
        actions.push(configurer.subscribe(handlers.configurer));
      }

      if (rpcWorker && handlers.rpcWorker) {
        var mappings = lodash.filter(handlers.rpcWorker, function(mapping) {
          return lodash.isString(mapping.routineId) && lodash.isFunction(mapping.handler);
        });
        actions.push(Promise.mapSeries(mappings, function(mapping) {
          return rpcWorker.process(mapping.routineId, mapping.handler);
        }));
      }

      if (instantiator) {
        actions.push(instantiator.process());
      }

      if (subscriber && handlers.subscriber && lodash.isFunction(handlers.subscriber)) {
        var consumerTotal = kwargs.subscriber.consumerTotal;
        if (!lodash.isInteger(consumerTotal) || consumerTotal <= 0) consumerTotal = 1;
        var consumers = Promise.all(lodash.range(consumerTotal).map(function(item) {
          return subscriber.subscribe(handlers.subscriber);
        }));
        actions.push(consumers);
      }

      return Promise.all(actions);
    }).then(function(results) {
      LX.isEnabledFor('info') && LX.log('info', startTrail.toMessage({
        text: 'start() has completed'
      }));
      return results;
    }).catch(function(errors) {
      LX.isEnabledFor('info') && LX.log('info', startTrail.toMessage({
        text: 'start() has failed'
      }));
      return Promise.reject(errors);
    });
  }

  this.close = function() {
    var closeTrail = serverletTrail.copy();
    LX.isEnabledFor('info') && LX.log('info', closeTrail.toMessage({
      text: 'close() running'
    }));
    var actions = [];
    if (configurer && handlers.configurer) actions.push(configurer.close());
    if (rpcWorker && handlers.rpcWorker) actions.push(rpcWorker.close());
    if (subscriber && handlers.subscriber) actions.push(subscriber.close());
    return Promise.all(actions).then(function(results) {
      LX.isEnabledFor('info') && LX.log('info', closeTrail.toMessage({
        text: 'close() has completed'
      }));
      return results;
    }).catch(function(errors) {
      LX.isEnabledFor('info') && LX.log('info', closeTrail.toMessage({
        text: 'close() has failed'
      }));
      return Promise.reject(errors);
    });
  }

  if (kwargs.autoinit !== false) {
    LX.isEnabledFor('debug') && LX.log('debug', serverletTrail.toMessage({
      text: 'auto execute ready()'
    }));
    misc.notifyConstructor(this.ready(), this);
  }

  LX.isEnabledFor('info') && LX.log('info', serverletTrail.toMessage({
    text: 'Serverlet.new() end!'
  }));
}

module.exports = Serverlet;

util.inherits(Serverlet, events.EventEmitter);

var Instantiator = function(params) {
  var self = this;
  var rpcWorker = params.rpcWorker;
  var routineIds = [];
  var routineDef = {};

  var instantiatorId = params.instantiatorId || misc.getLogID();
  var instantiatorTrail = LogTracer.ROOT.branch({ key:'instantiatorId', value:instantiatorId });

  LI.isEnabledFor('info') && LI.log('info', instantiatorTrail.toMessage({
    text: 'Instantiator.new()'
  }));

  var processor = function(body, headers, response) {
    var routineId = misc.getRoutineId(headers);
    var routineTrail = instantiatorTrail.branch({ key:'routineId', value:routineId});
    var routine = routineDef[routineId];
    if (!routine) return Promise.resolve('next');
    var args = JSON.parse(body);
    var promise = Promise.resolve(routine.handler.apply(routine.context, args));
    return promise.then(function(output) {
      response.emitCompleted(output);
      LI.isEnabledFor('info') && LI.log('info', routineTrail.toMessage({
        text: 'processor() has completed successfully'
      }));
      return Promise.resolve('done');
    }).catch(function(error) {
      error = error || {};
      response.emitFailed({
        code: error.code,
        message: error.message,
        errorClass: error.name,
        errorStack: error.stack
      });
      LI.isEnabledFor('info') && LI.log('info', routineTrail.toMessage({
        text: 'processor() has failed'
      }));
      return Promise.resolve('done');
    }).finally(function() {
      LI.isEnabledFor('info') && LI.log('info', routineTrail.toMessage({
        text: 'processor() has finished'
      }));
    })
  }

  self.process = function() {
    return rpcWorker.process(routineIds, processor);
  }

  self.registerRoutine = function(descriptor) {
    // TODO: validate descriptor here
    descriptor = descriptor || {};
    var routineId = descriptor.routineId || descriptor.signature || descriptor.name;
    routineIds.push(routineId);
    routineDef[routineId] = {
      schema: descriptor.schema,
      handler: descriptor.handler,
      context: descriptor.context
    }
    return self;
  }

  LI.isEnabledFor('info') && LI.log('info', instantiatorTrail.toMessage({
    text: 'Instantiator.new() end!'
  }));
}
