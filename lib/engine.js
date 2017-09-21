'use strict';

var assert = require('assert');
var Promise = require('bluebird');
var amqp = require('amqplib/callback_api');
var lodash = require('lodash');
var locks = require('locks');
var misc = require('./util');
var LogTracer = require('./log_tracer');
var LogAdapter = require('./log_adapter');
var LX = LogAdapter.getLogger({ scope: 'opflow:engine' });
var L0 = LogAdapter.getLogger({ scope: 'trace:opflow:engine' });

var Engine = function(params) {
  params = params || {};
  var self = this;

  var engineId = params.engineId || misc.getUUID();
  var logTracer = LogTracer.ROOT.branch({ key: 'engineId', value: engineId });
  LX.isEnabledFor('info') && LX.log('info', logTracer.add({
    message: 'Engine.new()'
  }).toString());

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

  LX.isEnabledFor('debug') && LX.log('debug', logTracer.reset().add({
    config: config,
    message: 'configuration object'
  }).toString());

  this.ready = function() {
    var logReady = logTracer.copy();
    LX.isEnabledFor('info') && LX.log('info', logReady.add({
      'message': 'ready() is invoked'
    }).toString());
    var sandbox = getProducerState();
    return getChannel(sandbox).then(function(ch) {
      return assertExchange(sandbox);
    }).then(function(result) {
      LX.isEnabledFor('info') && LX.log('info', logReady.add({
        'message': 'ready() has been done'
      }).toString());
      return result;
    });
  }

  this.produce = function(body, props, override) {
    override = override || {};
    props = props || {};
    props.appId = props.appId || config.applicationId;
    props.headers = props.headers || {};
    var requestId = props.headers.requestId = props.headers.requestId || misc.getUUID();
    var logRequest = logTracer.branch({ key: 'requestId', value: requestId });
    var ok = lockProducer().then(function(sandbox) {
      LX.isEnabledFor('info') && LX.log('info', logRequest.add({
        message: 'produce() a message to exchange/queue',
        producerId: sandbox.id
      }).toString());
      var sendTo = function() {
        sandbox.sendable = sandbox.channel.publish(config.exchangeName, 
          override.routingKey || config.routingKey, misc.bufferify(body), props);
      }
      if (sandbox.sendable !== false) {
        sendTo();
        LX.isEnabledFor('conlog') && LX.log('conlog', logRequest.add({
          message: 'produce() channel is writable, msg has been sent',
          producerId: sandbox.id
        }).toString());
        return sandbox.sendable;
      } else {
        LX.isEnabledFor('info') && LX.log('info', logRequest.add({
          message: 'produce() channel is overflowed, waiting',
          producerId: sandbox.id
        }).toString());
        return new Promise(function(resolved, rejected) {
          sandbox.channel.once('drain', function() {
            sendTo();
            LX.isEnabledFor('info') && LX.log('info', logRequest.add({
              message: 'produce() channel is drained, flushed',
              producerId: sandbox.id
            }).toString());
            resolved(sandbox.sendable);
          });
        });
      }
    }).then(function(result) {
      unlockProducer();
      LX.isEnabledFor('info') && LX.log('info', logRequest.add({
        message: 'produce() sandbox has been unlock'
      }).toString());
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

    return getConsumerState(options).then(function(sandbox) {
      var logConsume = logTracer.branch({ key: 'consumerId', value: sandbox.id });

      sandbox.count = sandbox.count || 0;
      sandbox.replyToName = options.replyToName || options.replyTo;

      LX.isEnabledFor('conlog') && LX.log('conlog', logConsume.add({
        message: 'consume() - create a consumer'
      }).toString());

      return getChannel(sandbox).then(function(ch) {

        var ok = assertSubscriber(sandbox, options);

        ok = ok.then(function(qok) {
          if (options.binding === false) {
            LX.isEnabledFor('debug') && LX.log('debug', logConsume.add({
              message: 'consume() - queue is keep standalone',
              queueName: qok.queue
            }).toString());
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
            LX.isEnabledFor('debug') && LX.log('debug', logConsume.add({
              message: 'consume() - queue has been bound',
              queueName: qok.queue
            }).toString());
            return qok;
          });
        });

        ok = ok.then(function(qok) {
          LX.isEnabledFor('debug') && LX.log('debug', logConsume.add({
            message: 'consume() - queue ready to consume',
            queueInfo: qok
          }).toString());

          var ch_consume = Promise.promisify(ch.consume, {context: ch});
          return ch_consume(qok.queue, function(msg) {
            sandbox.count++;

            var props = msg && msg.properties || {};
            var requestId = misc.getRequestId(props.headers);

            LX.isEnabledFor('info') && LX.log('info', logConsume.add({
              message: 'consume() receive new message',
              requestId: requestId,
              appId: props.appId
            }).toString());

            L0.isEnabledFor('conlog') && L0.log('conlog', 'consume() - received message: %s, fields: %s, properties: %s, amount: %s', 
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
              LX.isEnabledFor('info') && LX.log('info', logConsume.add({
                message: 'Mismatched applicationId',
                requestId: requestId,
                appId: msg.properties.appId,
                applicationId: config.applicationId
              }).toString({reset: true}));
              return;
            }

            try {
              callback(msg, sandbox, function(err, result) {
                L0.isEnabledFor('conlog') && L0.log('conlog', 'consume() - processed message: %s', msg.content);
                LX.isEnabledFor('info') && LX.log('info', logConsume.reset().add({
                  message: 'Processing finish',
                  requestId: requestId
                }).toString());
                forceAck(err);
              });
            } catch (exception) {
              L0.isEnabledFor('conlog') && console.log('consume() - exception: ', exception);
              L0.isEnabledFor('conlog') && L0.log('conlog', 'consume() - exception: %s', JSON.stringify(exception));
              LX.isEnabledFor('info') && LX.log('info', logConsume.add({
                message: 'Unmanaged exception',
                requestId: requestId
              }).toString());
              forceAck(exception);
            }
          }, {noAck: options.noAck});
        });

        return ok.then(function(result) {
          LX.isEnabledFor('info') && LX.log('info', logConsume.add({
            message: 'consume() is running. CTRL+C to exit',
            consumerTag: result.consumerTag
          }).toString());
          sandbox.consumerTag = result.consumerTag;
          if (!config.mode || config.mode == 'engine') consumerRefs.push(sandbox);
          return sandbox;
        });
      });
    });
  }

  this.cancelConsumer = function(sandbox) {
    sandbox = sandbox || {};
    var logCancel = logTracer.branch({key:'consumerId', value:sandbox.id});
    return Promise.resolve().then(function() {
      if (sandbox.consumerTag && sandbox.channel) {
        LX.isEnabledFor('conlog') && LX.log('conlog', logCancel.add({
          message: 'cancelConsumer() subscriber has been invoked',
          consumerTag: sandbox.consumerTag
        }).toString());
        var ch_cancel = Promise.promisify(sandbox.channel.cancel, {
          context: sandbox.channel
        });
        return ch_cancel(sandbox.consumerTag).then(function(ok) {
          delete sandbox.consumerTag;
          LX.isEnabledFor('conlog') && LX.log('conlog', logCancel.add({
            message: 'cancelConsumer() - subscriber is cancelled',
            consumerTag: sandbox.consumerTag
          }).toString());
          return true;
        }).delay(100);
      }
      return true;
    }).then(function() {
      if (sandbox.forceNewChannel || sandbox.forceNewConnection) 
        return closeChannel(sandbox);
      LX.isEnabledFor('conlog') && LX.log('conlog', logCancel.add({
        message: 'cancelConsumer() - skip close channel'
      }).toString());
      return true;
    }).then(function() {
      if (sandbox.forceNewConnection) return closeConnection(sandbox);
      LX.isEnabledFor('conlog') && LX.log('conlog', logCancel.add({
        message: 'cancelConsumer() - skip close connection'
      }).toString());
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
    var logClose = logTracer.copy();
    return Promise.resolve().then(function() {
      LX.isEnabledFor('info') && LX.log('info', logClose.add({
        message: 'close() - cancel producer'
      }).toString());
      return cancelProducer(getProducerState());
    }).then(function() {
      if (config.mode && config.mode != 'engine') return true;
      LX.isEnabledFor('info') && LX.log('info', logClose.add({
        message: 'close() - cancel consumers'
      }).toString());
      return Promise.mapSeries(consumerRefs, function(consumerRef) {
        return self.cancelConsumer(consumerRef);
      });
    }).then(function() {
      LX.isEnabledFor('info') && LX.log('info', logClose.add({
        message: 'close() - close default consumer channel'
      }).toString());
      return getConsumerState().then(closeChannel);
    }).then(function() {
      LX.isEnabledFor('info') && LX.log('info', logClose.add({
        message: 'close() - close default consumer connection'
      }).toString());
      return getConsumerState().then(closeConnection);
    }).catch(function(err) {
      LX.isEnabledFor('info') && LX.log('info', logClose.add({
        message: 'close() - catched error, reject it'
      }).toString());
      LX.isEnabledFor('info') && console.log('Exception: %s', JSON.stringify(err));
      return Promise.reject(err);
    });
  }

  var getConnection = function(sandbox) {
    sandbox = sandbox || {};
    var logConnect = (sandbox.id) ? logTracer.branch({
      key: 'sandboxId', value:sandbox.id
    }) : logTracer.copy();
    sandbox.connectionCount = sandbox.connectionCount || 0;
    LX.isEnabledFor('conlog') && LX.log('conlog', logConnect.add({
      message: 'getConnection() - connection amount',
      connectionCount: sandbox.connectionCount
    }).toString({reset: true}));
    if (config.connectIsCached !== false && sandbox.connection) {
      LX.isEnabledFor('conlog') && LX.log('conlog', logConnect.add({
        message: 'getConnection() - connection has been available'
      }).toString());
      return Promise.resolve(sandbox.connection);
    } else {
      LX.isEnabledFor('conlog') && LX.log('conlog', logConnect.add({
        message: 'getConnection() - make a new connection'
      }).toString());
      var amqp_connect = Promise.promisify(amqp.connect, {context: amqp});
      return amqp_connect(config.uri, {}).then(function(conn) {
        sandbox.connectionCount += 1;
        conn.on('close', function() {
          sandbox.connection = null;
          sandbox.connectionCount -= 1;
          LX.isEnabledFor('conlog') && LX.log('conlog', logConnect.add({
            message: 'connection has been closed'
          }).toString());
        });
        conn.on('error', function(error) {
          LX.isEnabledFor('conlog') && LX.log('conlog', logConnect.add({
            message: 'connection has been failed'
          }).toString());
        })
        LX.isEnabledFor('conlog') && LX.log('conlog', logConnect.add({
          message: 'getConnection() - connection is created successfully'
        }).toString());
        return (sandbox.connection = conn);
      });
    }
  }

  var closeConnection = function(sandbox) {
    sandbox = sandbox || {};
    var logConnect = (sandbox.id) ? logTracer.branch({
      key: 'sandboxId', value:sandbox.id
    }) : logTracer.copy();
    LX.isEnabledFor('conlog') && LX.log('conlog', logConnect.add({
      message: 'closeConnection() - closing'
    }).toString());
    if (sandbox.connection) {
      var connection_close = Promise.promisify(sandbox.connection.close, {
        context: sandbox.connection
      });
      return connection_close().then(function() {
        delete sandbox.connection;
        LX.isEnabledFor('conlog') && LX.log('conlog', logConnect.add({
          message: 'closeConnection() - connection is closed'
        }).toString());
        return true;
      });
    }
    return Promise.resolve(true);
  }

  var getChannel = function(sandbox, opts) {
    sandbox = sandbox || {};
    opts = opts || {};
    var logChannel = (sandbox.id) ? logTracer.branch({
      key: 'sandboxId', value:sandbox.id
    }) : logTracer.copy();
    if (config.channelIsCached !== false && sandbox.channel) {
      LX.isEnabledFor('conlog') && LX.log('conlog', logChannel.add({
        message: 'getChannel() - channel has been available'
      }).toString());
      return Promise.resolve(sandbox.channel);
    } else {
      LX.isEnabledFor('conlog') && LX.log('conlog', logChannel.add({
        message: 'getChannel() - make a new channel'
      }).toString());
      return getConnection(sandbox).then(function(conn) {
        LX.isEnabledFor('conlog') && LX.log('conlog', logChannel.add({
          message: 'getChannel() - connection is available'
        }).toString());
        var createChannel = Promise.promisify(conn.createChannel, {context: conn});
        return createChannel().then(function(ch) {
          ch.on('close', function() {
            sandbox.channel = null;
          });
          LX.isEnabledFor('conlog') && LX.log('conlog', logChannel.add({
            message: 'getChannel() - channel is created'
          }).toString());
          return (sandbox.channel = ch);
        });
      });
    }
  }

  var closeChannel = function(sandbox) {
    sandbox = sandbox || {};
    var logChannel = (sandbox.id) ? logTracer.branch({
      key: 'sandboxId', value:sandbox.id
    }) : logTracer.copy();
    LX.isEnabledFor('conlog') && LX.log('conlog', logChannel.add({
      message: 'closeChannel() - closing'
    }).toString());
    if (sandbox.channel) {
      var ch_close = Promise.promisify(sandbox.channel.close, {
        context: sandbox.channel
      });
      return ch_close().then(function() {
        delete sandbox.channel;
        LX.isEnabledFor('conlog') && LX.log('conlog', logChannel.add({
          message: 'closeChannel() - channel is closed'
        }).toString());
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
    sandbox = sandbox || {};
    options = options || {};
    var logSubscriber = (sandbox.id) ? logTracer.branch({
      key: 'sandboxId', value:sandbox.id
    }) : logTracer.copy();
    var ch = sandbox.channel;
    if (options.prefetch && options.prefetch >= 0) {
      LX.isEnabledFor('conlog') && LX.log('conlog', logSubscriber.add({
        message: 'assertSubscriber() - set channel prefetch',
        prefetch: options.prefetch
      }).toString({reset:true}));
      ch.prefetch(options.prefetch, true);
    }
    return assertQueue(sandbox, options).then(function(qok) {
      if (options.maxSubscribers && options.maxSubscribers <= qok.consumerCount) {
        var error = {
          consumerCount: qok.consumerCount,
          maxSubscribers: options.maxSubscribers,
          message: 'exceeding quota limits of subscribers'
        }
        LX.isEnabledFor('conlog') && LX.log('conlog', logSubscriber.add({
          message: 'assertSubscriber() - Error',
          error: error
        }).toString());
        return Promise.reject(error);
      } else {
        LX.isEnabledFor('conlog') && LX.log('conlog', logSubscriber.add({
          message: 'assertSubscriber() - Queue',
          queue: qok
        }).toString());
        return qok;
      }
    });
  }

  var getProducerState = function() {
    self.producerState = self.producerState || {};
    if (!self.producerState.id) {
      self.producerState.id = misc.getUUID();
      LX.isEnabledFor('info') && LX.log('info', logTracer.reset().add({
        message: 'getProducerState() - create producer sandbox',
        producerId: self.producerState.id
      }).toString());
    }
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
              LX.isEnabledFor('conlog') && LX.log('conlog', logTracer.reset().add({
                message: 'lockProducer() - obtain semaphore',
                count: producerState.count
              }).toString());
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
      LX.isEnabledFor('conlog') && LX.log('conlog', logTracer.reset().add({
        message: 'unlockProducer() - release semaphore',
        count: producerState.count
      }).toString());
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
    if (!self.consumerState.id) {
      self.consumerState.id = misc.getUUID();
      LX.isEnabledFor('conlog') && LX.log('conlog', logTracer.reset().add({
        message: 'getConsumerState() - create consumer sandbox',
        consumerId: self.consumerState.id
      }).toString());
    }
    var logConsumer = logTracer.copy();
    self.consumerState.name = self.consumerState.name || 'consumerSandbox';
    if (!opts) {
      LX.isEnabledFor('info') && LX.log('info', logConsumer.add({
        message: 'getConsumerState() - return origin sandbox',
        sandboxId: self.consumerState.id
      }).toString());
      return Promise.resolve(self.consumerState);
    }
    if (opts.forceNewConnection) {
      var sandboxId1 = misc.getUUID();
      LX.isEnabledFor('info') && LX.log('info', logConsumer.add({
        message: 'getConsumerState() - forced Connection and forced Channel',
        sandboxId: sandboxId1
      }).toString());
      return Promise.resolve({
        id: sandboxId1,
        name: getSandboxNameOf(self.consumerState, 1),
        forceNewConnection: true
      });
    }
    if (opts.forceNewChannel) {
      var sandboxId2 = misc.getUUID();
      return getConnection(self.consumerState).then(function(conn) {
        LX.isEnabledFor('info') && LX.log('info', logConsumer.add({
          message: 'getConsumerState() - shared Connection and forced Channel',
          sandboxId: sandboxId2
        }).toString());
        return {
          id: sandboxId2,
          name: getSandboxNameOf(self.consumerState, 2),
          forceNewChannel: true,
          connection: conn
        }
      });
    }
    var sandboxId3 = misc.getUUID();
    return getChannel(self.consumerState).then(function(ch) {
      LX.isEnabledFor('info') && LX.log('info', logConsumer.add({
        message: 'getConsumerState() - shared Connection and shared Channel',
        sandboxId: sandboxId3
      }).toString());
      return {
        id: sandboxId3,
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

  Object.defineProperty(self, 'engineId', {
    get: function() { return engineId },
    set: function(value) {}
  });

  LX.isEnabledFor('info') && LX.log('info', logTracer.reset().add({
    message: 'Engine.new() done!'
  }).toString());
};

module.exports = Engine;
