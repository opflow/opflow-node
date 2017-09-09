'use strict';

var assert = require('assert');
var Promise = require('bluebird');
var amqp = require('amqplib/callback_api');
var lodash = require('lodash');
var locks = require('locks');
var misc = require('./util');
var LogAdapter = require('./logadapter');
var LX = LogAdapter.getLogger({ scope: 'opflow:engine' });
var L0 = LogAdapter.getLogger({ scope: 'trace:opflow:engine' });

var Engine = function(params) {
  params = params || {};
  var self = this;

  var engineId = params.engineId || misc.getUUID();
  LX.isEnabledFor('info') && LX.log('info', {
    message: 'Engine.new()',
    engineId: engineId,
    instanceId: misc.instanceId });

  var config = {};
  config.uri = params.uri || params.host;
  config.exchangeName = params.exchangeName || params.exchange;
  config.exchangeType = params.exchangeType || 'direct';
  config.exchangeQuota = (typeof(params.exchangeQuota) === 'number') ? params.exchangeQuota : undefined;
  config.durable = (typeof(params.durable) === 'boolean') ? params.durable : true;
  config.autoDelete = (typeof(params.autoDelete) === 'boolean') ? params.autoDelete : false;
  if (typeof(params.alternateExchange) === 'string') {
    config.alternateExchange = params.alternateExchange;
  }
  config.routingKey = params.routingKey || '';
  config.otherKeys = params.otherKeys || [];

  if (typeof(params.applicationId) === 'string') {
    config.applicationId = params.applicationId;
  }

  config.delayTime = (typeof(params.delayTime) === 'number') ? params.delayTime : 0;

  config.mode = params.mode || 'engine';
  var consumerRefs = [];

  LX.isEnabledFor('debug') && LX.log('debug', {
    message: 'configuration object',
    engineId: engineId,
    config: config });

  this.ready = function() {
    var sandbox = getProducerState();
    return getChannel(sandbox).then(function(ch) {
      return assertExchange(sandbox);
    });
  }

  this.produce = function(body, props, override) {
    override = override || {};
    props = props || {};
    props.appId = props.appId || config.applicationId;
    props.headers = props.headers || {};
    var requestId = props.headers.requestId = props.headers.requestId || misc.getUUID();
    LX.isEnabledFor('info') && LX.log('info', {
      message: 'produce() a message to exchange/queue',
      requestId: requestId,
      engineId: engineId });
    var ok = lockProducer().then(function(sandbox) {
      var sendTo = function() {
        sandbox.sendable = sandbox.channel.publish(config.exchangeName, 
          override.routingKey || config.routingKey, misc.bufferify(body), props);
      }
      if (sandbox.sendable !== false) {
        sendTo();
        LX.isEnabledFor('verbose') && LX.log('verbose', {
          message: 'produce() channel is writable, msg has been sent',
          requestId: requestId });
        return sandbox.sendable;
      } else {
        LX.isEnabledFor('info') && LX.log('info', {
          message: 'produce() channel is overflowed, waiting',
          requestId: requestId });
        return new Promise(function(resolved, rejected) {
          sandbox.channel.once('drain', function() {
            sendTo();
            LX.isEnabledFor('info') && LX.log('info', {
              message: 'produce() channel is drained, flushed',
              requestId: requestId });
            resolved(sandbox.sendable);
          });
        });
      }
    }).then(function(result) {
      unlockProducer();
      return result;
    });
    if (config.delayTime > 0) {
      ok = ok.delay(config.delayTime);
    }
    return ok;
  }

  this.consume = function(callback, options) {
    assert.ok(lodash.isFunction(callback), 'callback should be a function');
    options = options || {};
    var callId = misc.getUUID();

    LX.isEnabledFor('info') && LX.log('info', {
        message: 'consume() is invoked',
        callId: callId,
        engineId: engineId });

    return getConsumerState(options).then(function(sandbox) {
      sandbox.count = sandbox.count || 0;
      sandbox.replyToName = options.replyToName || options.replyTo;

      LX.isEnabledFor('verbose') && LX.log('verbose', {
        message: 'consume() - create a consumer',
        callId: callId });
      return getChannel(sandbox).then(function(ch) {

        var ok = assertSubscriber(sandbox, options);

        ok = ok.then(function(qok) {
          if (options.binding === false) {
            LX.isEnabledFor('verbose') && LX.log('verbose', {
              message: 'consume() - queue is keep standalone',
              callId: callId,
              queueName: qok.queue });
            return qok;
          }
          var ch_bindQueue = Promise.promisify(ch.bindQueue, {context: ch});
          return Promise.resolve().then(function() {
            if (lodash.isEmpty(config.routingKey)) return Promise.resolve();
            return assertExchange(sandbox).then(function() {
              return ch_bindQueue(qok.queue, config.exchangeName, config.routingKey, {});
            });
          }).then(function() {
            if (!lodash.isArray(config.otherKeys)) return Promise.resolve();
            return Promise.mapSeries(config.otherKeys, function(routingKey) {
              return ch_bindQueue(qok.queue, config.exchangeName, routingKey, {});
            });
          }).then(function() {
            LX.isEnabledFor('verbose') && LX.log('verbose', {
              message: 'consume() - queue has been bound',
              callId: callId,
              queueName: qok.queue });
            return qok;
          });
        });

        ok = ok.then(function(qok) {
          LX.isEnabledFor('verbose') && LX.log('verbose', {
            message: 'consume() - queue ready to consume',
            callId: callId,
            queueInfo: qok });

          var ch_consume = Promise.promisify(ch.consume, {context: ch});
          return ch_consume(qok.queue, function(msg) {
            sandbox.count++;

            var props = msg && msg.properties || {};
            var requestId = misc.getRequestId(props.headers);

            LX.isEnabledFor('info') && LX.log('info', {
              message: 'consume() receive new message',
              callId: callId,
              requestId: requestId,
              messageAppId: props.appId });

            L0.isEnabledFor('verbose') && L0.log('verbose', 'consume() - received message: %s, fields: %s, properties: %s, amount: %s', 
              msg.content, JSON.stringify(msg.fields), JSON.stringify(msg.properties), sandbox.count);

            var forceAck = function(err) {
              sandbox.count--;
              if (options.noAck !== true) {
                if (options.requeueFailure !== true) {
                  ch.ack(msg);
                } else {
                  ch.nack(msg, false, true);
                }
              }
            }

            if (config.applicationId && config.applicationId != msg.properties.appId) {
              forceAck();
              LX.isEnabledFor('info') && LX.log('info', {
                message: 'Mismatched applicationId',
                requestId: requestId,
                messageAppId: msg.properties.appId,
                applicationId: config.applicationId });
              return;
            }

            try {
              callback(msg, sandbox, function(err, result) {
                L0.isEnabledFor('verbose') && L0.log('verbose', 'consume() - processed message: %s', msg.content);
                LX.isEnabledFor('info') && LX.log('info', {
                  message: 'Processing finish',
                  requestId: requestId });
                forceAck(err);
              });
            } catch (exception) {
              L0.isEnabledFor('verbose') && console.log('consume() - exception: ', exception);
              L0.isEnabledFor('verbose') && L0.log('verbose', 'consume() - exception: %s', JSON.stringify(exception));
              LX.isEnabledFor('info') && LX.log('info', {
                  message: 'Unmanaged exception',
                  requestId: requestId });
              forceAck(exception);
            }
          }, {noAck: options.noAck});
        });

        return ok.then(function(result) {
          LX.isEnabledFor('info') && LX.log('info', {
            message: 'consume() is running. CTRL+C to exit',
            callId: callId,
            consumerTag: result.consumerTag });
          sandbox.consumerTag = result.consumerTag;
          if (!config.mode || config.mode == 'engine') consumerRefs.push(sandbox);
          return sandbox;
        });
      });
    });
  }

  this.cancelConsumer = function(sandbox) {
    return Promise.resolve().then(function() {
      if (sandbox.consumerTag && sandbox.channel) {
        LX.isEnabledFor('verbose') && LX.log('verbose', {
          message: 'cancelConsumer() subscriber has been invoked',
          sandboxId: sandbox.id,
          consumerTag: sandbox.consumerTag});
        var ch_cancel = Promise.promisify(sandbox.channel.cancel, {
          context: sandbox.channel
        });
        return ch_cancel(sandbox.consumerTag).then(function(ok) {
          delete sandbox.consumerTag;
          LX.isEnabledFor('verbose') && LX.log('verbose', {
            message: 'cancelConsumer() - subscriber is cancelled',
            sandboxId: sandbox.id,
            consumerTag: sandbox.consumerTag});
          return true;
        }).delay(100);
      }
      return true;
    }).then(function() {
      if (sandbox.forceNewChannel || sandbox.forceNewConnection) 
        return closeChannel(sandbox);
      LX.isEnabledFor('verbose') && LX.log('verbose', {
        message: 'cancelConsumer() - skip close channel',
        sandboxId: sandbox.id});
      return true;
    }).then(function() {
      if (sandbox.forceNewConnection) return closeConnection(sandbox);
      LX.isEnabledFor('verbose') && LX.log('verbose', {
        message: 'cancelConsumer() - skip close connection',
        sandboxId: sandbox.id});
      return true;
    });
  }

  this.openSession = function(sandbox) {
    if (!lodash.isObject(sandbox)) return Promise.reject();
    sandbox.name = sandbox.name || 'TemporarySandbox=@' + (new Date()).toISOString();
    return getChannel(sandbox);
  }

  this.closeSession = function(sandbox) {
    if (!lodash.isObject(sandbox)) return Promise.resolve();
    return closeChannel(sandbox).then(function() {
      return closeConnection(sandbox);
    });
  }

  this.close = function() {
    return Promise.resolve().then(function() {
      LX.isEnabledFor('info') && LX.log('info', {
        message: 'close() - cancel producer',
        engineId: engineId });
      return cancelProducer(getProducerState());
    }).then(function() {
      if (config.mode && config.mode != 'engine') return true;
      LX.isEnabledFor('info') && LX.log('info', {
        message: 'close() - cancel consumers',
        engineId: engineId });
      return Promise.mapSeries(consumerRefs, function(consumerRef) {
        return self.cancelConsumer(consumerRef);
      });
    }).then(function() {
      LX.isEnabledFor('info') && LX.log('info', {
        message: 'close() - close default consumer channel',
        engineId: engineId });
      return getConsumerState().then(closeChannel);
    }).then(function() {
      LX.isEnabledFor('info') && LX.log('info', {
        message: 'close() - close default consumer connection',
        engineId: engineId });
      return getConsumerState().then(closeConnection);
    }).catch(function(err) {
      LX.isEnabledFor('info') && LX.log('info', {
        message: 'close() - catched error, reject it',
        engineId: engineId });
      LX.isEnabledFor('info') && console.log('Exception: %s', JSON.stringify(err));
      return Promise.reject(err);
    });
  }

  var getConnection = function(sandbox) {
    sandbox = sandbox || {};
    sandbox.connectionCount = sandbox.connectionCount || 0;
    LX.isEnabledFor('verbose') && LX.log('verbose', {
      message: 'getConnection() - connection amount',
      connectionCount: sandbox.connectionCount,
      sandboxId: sandbox.id });
    if (config.connectIsCached !== false && sandbox.connection) {
      LX.isEnabledFor('verbose') && LX.log('verbose', {
        message: 'getConnection() - connection has been available',
        sandboxId: sandbox.id });
      return Promise.resolve(sandbox.connection);
    } else {
      LX.isEnabledFor('verbose') && LX.log('verbose', {
        message: 'getConnection() - make a new connection',
        sandboxId: sandbox.id });
      var amqp_connect = Promise.promisify(amqp.connect, {context: amqp});
      return amqp_connect(config.uri, {}).then(function(conn) {
        sandbox.connectionCount += 1;
        conn.on('close', function() {
          sandbox.connection = null;
          sandbox.connectionCount -= 1;
          LX.isEnabledFor('verbose') && LX.log('verbose', {
            message: 'connection has been closed',
            sandboxId: sandbox.id });
        });
        conn.on('error', function(error) {
          LX.isEnabledFor('verbose') && LX.log('verbose', {
            message: 'connection has been failed',
            sandboxId: sandbox.id });
        })
        LX.isEnabledFor('verbose') && LX.log('verbose', {
          message: 'getConnection() - connection is created successfully',
          sandboxId: sandbox.id });
        return (sandbox.connection = conn);
      });
    }
  }

  var closeConnection = function(sandbox) {
    sandbox = sandbox || {};
    LX.isEnabledFor('verbose') && LX.log('verbose', {
      message: 'closeConnection() - closing',
      sandboxId: sandbox.id });
    if (sandbox.connection) {
      var connection_close = Promise.promisify(sandbox.connection.close, {
        context: sandbox.connection
      });
      return connection_close().then(function() {
        delete sandbox.connection;
        LX.isEnabledFor('verbose') && LX.log('verbose', {
          message: 'closeConnection() - connection is closed',
          sandboxId: sandbox.id });
        return true;
      });
    }
    return Promise.resolve(true);
  }

  var getChannel = function(sandbox, opts) {
    sandbox = sandbox || {};
    opts = opts || {};
    if (config.channelIsCached !== false && sandbox.channel) {
      LX.isEnabledFor('verbose') && LX.log('verbose', {
        message: 'getChannel() - channel has been available',
        sandboxId: sandbox.id });
      return Promise.resolve(sandbox.channel);
    } else {
      LX.isEnabledFor('verbose') && LX.log('verbose', {
        message: 'getChannel() - make a new channel',
        sandboxId: sandbox.id });
      return getConnection(sandbox).then(function(conn) {
        LX.isEnabledFor('verbose') && LX.log('verbose', {
          message: 'getChannel() - connection is available',
          sandboxId: sandbox.id });
        var createChannel = Promise.promisify(conn.createChannel, {context: conn});
        return createChannel().then(function(ch) {
          ch.on('close', function() {
            sandbox.channel = null;
          });
          LX.isEnabledFor('verbose') && LX.log('verbose', {
            message: 'getChannel() - channel is created',
            sandboxId: sandbox.id });
          return (sandbox.channel = ch);
        });
      });
    }
  }

  var closeChannel = function(sandbox) {
    sandbox = sandbox || {};
    LX.isEnabledFor('verbose') && LX.log('verbose', {
      message: 'closeChannel() - closing',
      sandboxId: sandbox.id });
    if (sandbox.channel) {
      var ch_close = Promise.promisify(sandbox.channel.close, {
        context: sandbox.channel
      });
      return ch_close().then(function() {
        delete sandbox.channel;
        LX.isEnabledFor('verbose') && LX.log('verbose', {
          message: 'closeChannel() - channel is closed',
          sandboxId: sandbox.id });
        return true;
      });
    }
    return Promise.resolve(true);
  }

  var assertExchange = function(sandbox) {
    if (!config.exchangeName) return Promise.resolve();
    if (sandbox.exchangeAsserted) return Promise.resolve();
    var ch = sandbox.channel;
    var ch_assertExchange = Promise.promisify(ch.assertExchange, {context: ch});
    return ch_assertExchange(config.exchangeName, config.exchangeType, {
      durable: config.durable,
      autoDelete: config.autoDelete
    }).then(function(eok) {
      sandbox.exchangeAsserted = true;
      return eok;
    });
  }

  var assertQueue = function(sandbox, options) {
    if (sandbox.queueAsserted) return Promise.resolve();
    var ch = sandbox.channel;
    var ch_assertQueue = Promise.promisify(ch.assertQueue, {context: ch});
    var qp;
    if (options.queueName) {
      sandbox.fixedQueue = true;
      sandbox.queueName = options.queueName;
      qp = ch_assertQueue(options.queueName, {
        durable: options.durable,
        exclusive: options.exclusive,
        autoDelete: options.autoDelete
      });
    } else {
      qp = ch_assertQueue(null, {
        durable: false, exclusive: true, autoDelete: true
      }).then(function(qok) {
        sandbox.fixedQueue = false;
        sandbox.queueName = options.queueName = qok.queue;
        return qok;
      });
    }
    return qp.then(function(qok) {
      sandbox.queueAsserted = true;
      return qok;
    });
  }

  var assertSubscriber = function(sandbox, options) {
    var ch = sandbox.channel;
    if (options.prefetch && options.prefetch >= 0) {
      LX.isEnabledFor('verbose') && LX.log('verbose', {
        message: 'assertSubscriber() - set channel prefetch',
        prefetch: options.prefetch,
        sandboxId: sandbox.id });
      ch.prefetch(options.prefetch, true);
    }
    return assertQueue(sandbox, options).then(function(qok) {
      if (options.maxSubscribers && options.maxSubscribers <= qok.consumerCount) {
        var error = {
          consumerCount: qok.consumerCount,
          maxSubscribers: options.maxSubscribers,
          message: 'exceeding quota limits of subscribers'
        }
        LX.isEnabledFor('verbose') && LX.log('verbose', {
          message: 'assertSubscriber() - Error',
          error: JSON.stringify(error),
          sandboxId: sandbox.id });
        return Promise.reject(error);
      } else {
        LX.isEnabledFor('verbose') && LX.log('verbose', {
          message: 'assertSubscriber() - Queue',
          queue: qok,
          sandboxId: sandbox.id });
        return qok;
      }
    });
  }

  var getProducerState = function() {
    self.producerState = self.producerState || {};
    self.producerState.id = self.producerState.id || misc.getUUID();
    self.producerState.name = self.producerState.name || 'producerSandbox';
    if (config.exchangeQuota > 0 && !self.producerState.fence) {
      self.producerState.count = self.producerState.count || 0;
      self.producerState.fence = self.producerState.fence || locks.createSemaphore(config.exchangeQuota);
    }
    return (self.producerState);
  }

  var lockProducer = function() {
    var producerState = getProducerState();
    return getChannel(producerState).then(function(ch) {
      return assertExchange(producerState).then(function() {
        if (producerState.fence) {
          return new Promise(function(onResolved, onRejected) {
            producerState.fence.wait(function whenResourceAvailable() {
              producerState.count++;
              LX.isEnabledFor('verbose') && LX.log('verbose', {
                message: 'lockProducer() - obtain semaphore',
                count: producerState.count,
                engineId: engineId });
              onResolved(producerState);
            });
          });
        }
        return producerState;
      });
    });
  }

  var unlockProducer = function() {
    var producerState = getProducerState();
    if (producerState.fence) {
      producerState.count--;
      LX.isEnabledFor('verbose') && LX.log('verbose', {
        message: 'unlockProducer() - release semaphore',
        count: producerState.count,
        engineId: engineId });
      producerState.fence.signal();
    }
  }

  var cancelProducer = function(sandbox) {
    return Promise.resolve().then(function() {
      if (sandbox.channel) sandbox.channel.removeAllListeners('drain');
      return true;
    }).then(function() {
      return closeChannel(sandbox);
    }).then(function(result) {
      return closeConnection(sandbox);
    });
  }

  var getConsumerState = function(opts) {
    self.consumerState = self.consumerState || {};
    self.consumerState.id = self.consumerState.id || misc.getUUID();
    self.consumerState.name = self.consumerState.name || 'consumerSandbox';
    LX.isEnabledFor('verbose') && LX.log('verbose', {
      message: 'getConsumerState() - from source sandbox',
      sandboxId: self.consumerState.id,
      engineId: engineId });
    if (!opts) return Promise.resolve(self.consumerState);
    if (opts.forceNewConnection) {
      return Promise.resolve({
        id: misc.getUUID(),
        name: getSandboxNameOf(self.consumerState, 1),
        forceNewConnection: true
      });
    }
    if (opts.forceNewChannel) {
      return getConnection(self.consumerState).then(function(conn) {
        return {
          id: misc.getUUID(),
          name: getSandboxNameOf(self.consumerState, 2),
          forceNewChannel: true,
          connection: conn
        }
      });
    }
    return getChannel(self.consumerState).then(function(ch) {
      return {
        id: misc.getUUID(),
        name: getSandboxNameOf(self.consumerState, 3),
        connection: self.consumerState.connection,
        channel: ch
      }
    });
  }

  var getSandboxNameOf = function(defaultSandbox, sharingLevel) {
    defaultSandbox = defaultSandbox || {};
    return defaultSandbox.name + '[' + sharingLevel + ']' + (new Date()).toISOString();
  }

  LX.isEnabledFor('info') && LX.log('info', {
    message: 'Engine.new() done!',
    engineId: engineId });
};

module.exports = Engine;
