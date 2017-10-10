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

  var engineId = params.engineId || misc.getLogID();
  var engineTrail = LogTracer.ROOT.branch({ key: 'engineId', value: engineId });
  LX.isEnabledFor('info') && LX.log('info', engineTrail.add({
    message: 'Engine.new()'
  }).toString());

  var config = {};
  config.uri = params.uri;
  if (lodash.isEmpty(config.uri)) {
    var conargs = misc.buildURI(params);
    config.uri = conargs.uri;
  }
  config.exchangeName = params.exchangeName || params.exchange;
  config.exchangeType = params.exchangeType || 'direct';
  config.exchangeQuota = (typeof(params.exchangeQuota) === 'number') ? params.exchangeQuota : undefined;
  config.confirmable = (typeof(params.confirmable) === 'boolean') ? params.confirmable : false;
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

  LX.isEnabledFor('debug') && LX.log('debug', engineTrail.add({
    config: config,
    message: 'configuration object'
  }).toString({reset:true}));

  this.ready = function() {
    var readyTrail = engineTrail.copy();
    LX.isEnabledFor('info') && LX.log('info', readyTrail.add({
      'message': 'ready() is invoked'
    }).toString());
    var sandbox = getProducerSandbox();
    return getChannel(sandbox).then(function(ch) {
      return assertExchange(sandbox);
    }).then(function(result) {
      LX.isEnabledFor('info') && LX.log('info', readyTrail.add({
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
    var requestId = props.headers.requestId = props.headers.requestId || misc.getLogID();
    var requestTrail = engineTrail.branch({ key: 'requestId', value: requestId });
    var ok = lockProducer().then(function(sandbox) {
      LX.isEnabledFor('info') && LX.log('info', requestTrail.add({
        message: 'produce() a message to exchange/queue',
        producerId: sandbox.id
      }).toString());
      var sendTo = function() {
        sandbox.sendable = sandbox.channel.publish(config.exchangeName, 
          override.routingKey || config.routingKey, misc.bufferify(body), props);
      }
      if (sandbox.sendable !== false) {
        sendTo();
        LX.isEnabledFor('conlog') && LX.log('conlog', requestTrail.add({
          message: 'produce() channel is writable, msg has been sent',
          producerId: sandbox.id
        }).toString());
        return sandbox.sendable;
      } else {
        LX.isEnabledFor('info') && LX.log('info', requestTrail.add({
          message: 'produce() channel is overflowed, waiting',
          producerId: sandbox.id
        }).toString());
        return new Promise(function(resolved, rejected) {
          sandbox.channel.once('drain', function() {
            sendTo();
            LX.isEnabledFor('info') && LX.log('info', requestTrail.add({
              message: 'produce() channel is drained, flushed',
              producerId: sandbox.id
            }).toString());
            resolved(sandbox.sendable);
          });
        });
      }
    }).finally(function() {
      unlockProducer();
      LX.isEnabledFor('info') && LX.log('info', requestTrail.add({
        message: 'produce() sandbox has been unlock'
      }).toString());
    });
    if (config.delayTime > 0) {
      ok = ok.delay(config.delayTime);
    }
    return ok;
  }

  this.consume = function(callback, options) {
    assert.ok(lodash.isFunction(callback), 'callback should be a function');
    options = options || {};

    return getConsumerSandbox(options).then(function(sandbox) {
      var consumeTrail = engineTrail.branch({ key: 'consumerId', value: sandbox.id });

      sandbox.replyToName = options.replyToName || options.replyTo;

      LX.isEnabledFor('conlog') && LX.log('conlog', consumeTrail.add({
        message: 'consume() - create a consumer'
      }).toString());

      return getChannel(sandbox).then(function(ch) {

        var ok = assertSubscriber(sandbox, options);

        ok = ok.then(function(qok) {
          if (options.binding === false) {
            LX.isEnabledFor('debug') && LX.log('debug', consumeTrail.add({
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
            LX.isEnabledFor('debug') && LX.log('debug', consumeTrail.add({
              message: 'consume() - queue has been bound',
              queueName: qok.queue
            }).toString());
            return qok;
          });
        });

        ok = ok.then(function(qok) {
          LX.isEnabledFor('debug') && LX.log('debug', consumeTrail.add({
            message: 'consume() - queue ready to consume',
            queueInfo: qok
          }).toString());

          var ch_consume = Promise.promisify(ch.consume, {context: ch});
          return ch_consume(qok.queue, function(msg) {
            var props = msg && msg.properties || {};
            var requestId = misc.getRequestId(props.headers);

            LX.isEnabledFor('info') && LX.log('info', consumeTrail.add({
              message: 'consume() receive new message',
              requestId: requestId,
              appId: props.appId,
              bodySize: Buffer.byteLength(msg.content)
            }).toString());

            L0.isEnabledFor('conlog') && L0.log('conlog', 'consume() - received message: %s, fields: %s, properties: %s', 
              msg.content, JSON.stringify(msg.fields), JSON.stringify(msg.properties));

            var forceAck = function(err) {
              if (options.noAck !== true) {
                if (err) {
                  if (options.requeueFailure == true) {
                    ch.nack(msg, false, true);
                  } else {
                    ch.nack(msg, false, false);
                  }
                } else {
                  ch.ack(msg);
                }
              }
            }

            if (config.applicationId && config.applicationId != msg.properties.appId) {
              forceAck();
              LX.isEnabledFor('info') && LX.log('info', consumeTrail.add({
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
                LX.isEnabledFor('info') && LX.log('info', consumeTrail.reset().add({
                  message: 'Processing finish',
                  requestId: requestId
                }).toString());
                forceAck(err);
              });
            } catch (exception) {
              misc.isConsoleLogEnabled() && console.log('consume() - exception: ', exception);
              L0.isEnabledFor('conlog') && L0.log('conlog', 'consume() - exception: %s', JSON.stringify(exception));
              LX.isEnabledFor('info') && LX.log('info', consumeTrail.add({
                message: 'Unmanaged exception',
                requestId: requestId
              }).toString());
              forceAck(exception);
            }
          }, {noAck: options.noAck});
        });

        return ok.then(function(result) {
          LX.isEnabledFor('info') && LX.log('info', consumeTrail.add({
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
    var cancelTrail = engineTrail.branch({key:'consumerId', value:sandbox.id});
    return Promise.resolve().then(function() {
      if (sandbox.consumerTag && sandbox.channel) {
        LX.isEnabledFor('conlog') && LX.log('conlog', cancelTrail.add({
          message: 'cancelConsumer() subscriber has been invoked',
          consumerTag: sandbox.consumerTag
        }).toString());
        var ch_cancel = Promise.promisify(sandbox.channel.cancel, {
          context: sandbox.channel
        });
        return ch_cancel(sandbox.consumerTag).then(function(ok) {
          delete sandbox.consumerTag;
          LX.isEnabledFor('conlog') && LX.log('conlog', cancelTrail.add({
            message: 'cancelConsumer() - subscriber is cancelled',
            consumerTag: sandbox.consumerTag
          }).toString());
          return true;
        }).delay(100);
      }
      return true;
    }).then(function() {
      return getConsumerSandbox().then(function(sharedSandbox) {
        return Promise.resolve().then(function() {
          if (sandbox.forceNewChannel || 
              sandbox.forceNewConnection || 
              sandbox.channel !== sharedSandbox.channel) 
            return closeChannel(sandbox);
          LX.isEnabledFor('conlog') && LX.log('conlog', cancelTrail.add({
            message: 'cancelConsumer() - skip close channel'
          }).toString());
          return true;
        }).then(function() {
          if (sandbox.forceNewConnection || 
              sandbox.connection !== sharedSandbox.connection)
            return closeConnection(sandbox);
          LX.isEnabledFor('conlog') && LX.log('conlog', cancelTrail.add({
            message: 'cancelConsumer() - skip close connection'
          }).toString());
          return true;
        })
      })
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

  this.acquireChannel = function(callback, sandbox) {
    if (!lodash.isFunction(callback)) return Promise.resolve();
    var selfManaged = !lodash.isObject(sandbox);
    sandbox = sandbox || {};
    var p = self.openSession(sandbox).then(callback);
    if (selfManaged) p = p.then(function(ok) {
      return self.closeSession(sandbox).then(lodash.wrap(ok));
    }).catch(function(err) {
      return self.closeSession(sandbox).then(lodash.wrap(Promise.reject(err)));
    });
    return p;
  }

  this.close = function() {
    var closeTrail = engineTrail.copy();
    return Promise.resolve().then(function() {
      LX.isEnabledFor('info') && LX.log('info', closeTrail.add({
        message: 'close() - cancel producer'
      }).toString());
      return cancelProducer(getProducerSandbox());
    }).then(function() {
      if (config.mode && config.mode != 'engine') return true;
      LX.isEnabledFor('info') && LX.log('info', closeTrail.add({
        message: 'close() - cancel consumers'
      }).toString());
      return Promise.mapSeries(consumerRefs, function(consumerRef) {
        return self.cancelConsumer(consumerRef);
      });
    }).then(function() {
      LX.isEnabledFor('info') && LX.log('info', closeTrail.add({
        message: 'close() - close default consumer channel'
      }).toString());
      return getConsumerSandbox().then(closeChannel);
    }).then(function() {
      LX.isEnabledFor('info') && LX.log('info', closeTrail.add({
        message: 'close() - close default consumer connection'
      }).toString());
      return getConsumerSandbox().then(closeConnection);
    }).catch(function(err) {
      LX.isEnabledFor('info') && LX.log('info', closeTrail.add({
        message: 'close() - catched error, reject it'
      }).toString());
      misc.isConsoleLogEnabled() && console.log('Exception: %s', err);
      return Promise.reject(err);
    });
  }

  var getConnection = function(sandbox) {
    sandbox = sandbox || {};
    var connectTrail = (sandbox.id) ? engineTrail.branch({
      key: 'sandboxId', value:sandbox.id
    }) : engineTrail.copy();
    LX.isEnabledFor('conlog') && LX.log('conlog', connectTrail.add({
      message: 'getConnection() - get a connection'
    }).toString());
    if (config.connectIsCached !== false && sandbox.connection) {
      LX.isEnabledFor('conlog') && LX.log('conlog', connectTrail.add({
        message: 'getConnection() - connection has been available'
      }).toString());
      return Promise.resolve(sandbox.connection);
    } else {
      LX.isEnabledFor('conlog') && LX.log('conlog', connectTrail.add({
        message: 'getConnection() - make a new connection'
      }).toString());
      var amqp_connect = Promise.promisify(amqp.connect, {context: amqp});
      return amqp_connect(config.uri, {}).then(function(conn) {
        conn.on('close', function() {
          sandbox.connection = null;
          sandbox.channel = null;
          LX.isEnabledFor('conlog') && LX.log('conlog', connectTrail.add({
            message: 'connection has been closed'
          }).toString());
        });
        conn.on('error', function(error) {
          sandbox.connection = null;
          sandbox.channel = null;
          LX.isEnabledFor('conlog') && LX.log('conlog', connectTrail.add({
            message: 'connection has been failed'
          }).toString());
        })
        LX.isEnabledFor('conlog') && LX.log('conlog', connectTrail.add({
          message: 'getConnection() - connection is created successfully'
        }).toString());
        return (sandbox.connection = conn);
      }).catch(function(err) {
        LX.isEnabledFor('error') && LX.log('error', connectTrail.add({
          message: 'getConnection() - throw an error object',
          errorCode: err.code,
          errorMessage: err.message
        }).toString({reset:true}));
        misc.checkToExit(err);
        return Promise.reject(err);
      });
    }
  }

  var closeConnection = function(sandbox) {
    sandbox = sandbox || {};
    var connectTrail = (sandbox.id) ? engineTrail.branch({
      key: 'sandboxId', value:sandbox.id
    }) : engineTrail.copy();
    LX.isEnabledFor('conlog') && LX.log('conlog', connectTrail.add({
      message: 'closeConnection() - closing'
    }).toString());
    if (sandbox.connection) {
      var connection_close = Promise.promisify(sandbox.connection.close, {
        context: sandbox.connection
      });
      return connection_close().then(function() {
        LX.isEnabledFor('conlog') && LX.log('conlog', connectTrail.add({
          message: 'closeConnection() - connection is closed'
        }).toString());
        return true;
      }).catch(function(err) {
        LX.isEnabledFor('conlog') && LX.log('conlog', connectTrail.add({
          message: 'closeConnection() - failed'
        }).toString());
        return false;
      }).finally(function() {
        delete sandbox.connection;
      });
    }
    return Promise.resolve(true);
  }

  var getChannel = function(sandbox, opts) {
    sandbox = sandbox || {};
    opts = opts || {};
    var channelTrail = (sandbox.id) ? engineTrail.branch({
      key: 'sandboxId', value:sandbox.id
    }) : engineTrail.copy();
    if (config.channelIsCached !== false && sandbox.channel) {
      LX.isEnabledFor('conlog') && LX.log('conlog', channelTrail.add({
        message: 'getChannel() - channel has been available'
      }).toString());
      return Promise.resolve(sandbox.channel);
    } else {
      LX.isEnabledFor('conlog') && LX.log('conlog', channelTrail.add({
        message: 'getChannel() - make a new channel'
      }).toString());
      return getConnection(sandbox).then(function(conn) {
        LX.isEnabledFor('conlog') && LX.log('conlog', channelTrail.add({
          message: 'getChannel() - connection is available'
        }).toString());
        var createChannel = Promise.promisify(conn.createChannel, {context: conn});
        if (sandbox.confirmable) {
          LX.isEnabledFor('conlog') && LX.log('conlog', channelTrail.add({
            message: 'getChannel() - createConfirmChannel'
          }).toString());
          createChannel = Promise.promisify(conn.createConfirmChannel, {context: conn});
        }
        return createChannel().then(function(ch) {
          ch.on('close', function() {
            sandbox.channel = null;
            LX.isEnabledFor('conlog') && LX.log('conlog', channelTrail.add({
              message: 'channel has been closed'
            }).toString());
          });
          ch.on('error', function() {
            sandbox.channel = null;
            LX.isEnabledFor('conlog') && LX.log('conlog', channelTrail.add({
              message: 'channel has been failed'
            }).toString());
          });
          LX.isEnabledFor('conlog') && LX.log('conlog', channelTrail.add({
            message: 'getChannel() - channel is created'
          }).toString());
          return (sandbox.channel = ch);
        }).catch(function(err) {
          LX.isEnabledFor('error') && LX.log('error', channelTrail.add({
            message: 'getChannel() - failed',
            errorCode: err.code,
            errorMessage: err.message
          }).toString({reset:true}));
          return Promise.reject(err);
        });
      });
    }
  }

  var closeChannel = function(sandbox) {
    sandbox = sandbox || {};
    var channelTrail = (sandbox.id) ? engineTrail.branch({
      key: 'sandboxId', value:sandbox.id
    }) : engineTrail.copy();
    LX.isEnabledFor('conlog') && LX.log('conlog', channelTrail.add({
      message: 'closeChannel() - closing'
    }).toString());
    if (sandbox.channel) {
      var ch_close = Promise.promisify(sandbox.channel.close, {
        context: sandbox.channel
      });
      return ch_close().then(function() {
        LX.isEnabledFor('conlog') && LX.log('conlog', channelTrail.add({
          message: 'closeChannel() - channel is closed'
        }).toString());
        return true;
      }).catch(function(err) {
        LX.isEnabledFor('conlog') && LX.log('conlog', channelTrail.add({
          message: 'closeChannel() - failed'
        }).toString());
        return false;
      }).finally(function() {
        delete sandbox.channel;
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
    var assertTrail = (sandbox.id) ? engineTrail.branch({
      key: 'sandboxId', value:sandbox.id
    }) : engineTrail.copy();
    var ch = sandbox.channel;
    if (options.prefetch && options.prefetch >= 0) {
      LX.isEnabledFor('conlog') && LX.log('conlog', assertTrail.add({
        message: 'assertSubscriber() - set channel prefetch',
        prefetch: options.prefetch
      }).toString({reset:true}));
      ch.prefetch(options.prefetch, true);
    }
    return assertQueue(sandbox, options).then(function(qok) {
      if (options.consumerLimit && options.consumerLimit <= qok.consumerCount) {
        var error = {
          consumerQueue: sandbox.queueName,
          consumerCount: qok.consumerCount,
          consumerLimit: options.consumerLimit,
          message: 'exceeding quota limits of subscribers'
        }
        LX.isEnabledFor('conlog') && LX.log('conlog', assertTrail.add({
          message: 'assertSubscriber() - Error',
          error: error
        }).toString());
        return Promise.reject(error);
      } else {
        LX.isEnabledFor('conlog') && LX.log('conlog', assertTrail.add({
          message: 'assertSubscriber() - Queue',
          queue: qok
        }).toString());
        return qok;
      }
    });
  }

  var getProducerSandbox = function() {
    self.producerSandbox = self.producerSandbox || {};
    if (!self.producerSandbox.id) {
      self.producerSandbox.id = misc.getLogID();
      LX.isEnabledFor('info') && LX.log('info', engineTrail.reset().add({
        message: 'getProducerSandbox() - create producer sandbox',
        producerId: self.producerSandbox.id
      }).toString());
    }
    self.producerSandbox.name = self.producerSandbox.name || 'producerSandbox';
    if (config.exchangeQuota > 0 && !self.producerSandbox.fence) {
      self.producerSandbox.fence = self.producerSandbox.fence || locks.createSemaphore(config.exchangeQuota);
    }
    self.producerSandbox.confirmable = config.confirmable;
    return (self.producerSandbox);
  }

  var lockProducer = function() {
    var producerSandbox = getProducerSandbox();
    return getChannel(producerSandbox).then(function(ch) {
      return assertExchange(producerSandbox).then(function() {
        if (producerSandbox.fence) {
          return new Promise(function(onResolved, onRejected) {
            producerSandbox.fence.wait(function whenResourceAvailable() {
              LX.isEnabledFor('conlog') && LX.log('conlog', engineTrail.reset().add({
                message: 'lockProducer() - obtain semaphore'
              }).toString());
              onResolved(producerSandbox);
            });
          });
        }
        return producerSandbox;
      });
    });
  }

  var unlockProducer = function() {
    var producerSandbox = getProducerSandbox();
    if (producerSandbox.fence) {
      LX.isEnabledFor('conlog') && LX.log('conlog', engineTrail.reset().add({
        message: 'unlockProducer() - release semaphore'
      }).toString());
      producerSandbox.fence.signal();
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

  var getConsumerSandbox = function(opts) {
    self.consumerSandbox = self.consumerSandbox || {};
    if (!self.consumerSandbox.id) {
      self.consumerSandbox.id = misc.getLogID();
      LX.isEnabledFor('conlog') && LX.log('conlog', engineTrail.reset().add({
        message: 'getConsumerSandbox() - create consumer sandbox',
        consumerId: self.consumerSandbox.id
      }).toString());
    }
    self.consumerSandbox.name = self.consumerSandbox.name || 'consumerSandbox';
    self.consumerSandbox.mutex = self.consumerSandbox.mutex || locks.createMutex();

    var consumeTrail = engineTrail.copy();
    return lockConsumer(self.consumerSandbox).then(function() {
      if (!opts) {
        LX.isEnabledFor('info') && LX.log('info', consumeTrail.add({
          message: 'getConsumerSandbox() - return origin sandbox',
          sandboxId: self.consumerSandbox.id
        }).toString());
        return Promise.resolve(self.consumerSandbox);
      }
      if (opts.forceNewConnection) {
        var sandboxId1 = misc.getLogID();
        LX.isEnabledFor('info') && LX.log('info', consumeTrail.add({
          message: 'getConsumerSandbox() - forced Connection and forced Channel',
          sandboxId: sandboxId1
        }).toString());
        return Promise.resolve({
          id: sandboxId1,
          name: getSandboxNameOf(self.consumerSandbox, 1),
          forceNewConnection: true
        });
      }
      if (opts.forceNewChannel) {
        var sandboxId2 = misc.getLogID();
        return getConnection(self.consumerSandbox).then(function(conn) {
          LX.isEnabledFor('info') && LX.log('info', consumeTrail.add({
            message: 'getConsumerSandbox() - shared Connection and forced Channel',
            sandboxId: sandboxId2
          }).toString());
          return {
            id: sandboxId2,
            name: getSandboxNameOf(self.consumerSandbox, 2),
            forceNewChannel: true,
            connection: conn
          }
        });
      }
      var sandboxId3 = misc.getLogID();
      return getChannel(self.consumerSandbox).then(function(ch) {
        LX.isEnabledFor('info') && LX.log('info', consumeTrail.add({
          message: 'getConsumerSandbox() - shared Connection and shared Channel',
          sandboxId: sandboxId3
        }).toString());
        return {
          id: sandboxId3,
          name: getSandboxNameOf(self.consumerSandbox, 3),
          connection: self.consumerSandbox.connection,
          channel: ch
        }
      });
    }).finally(function() {
      unlockConsumer(self.consumerSandbox);
    });
  }

  var lockConsumer = function(consumerSandbox) {
    return new Promise(function(onResolved, onRejected) {
      if (consumerSandbox.mutex) {
        consumerSandbox.mutex.lock(function() {
          onResolved();
          LX.isEnabledFor('info') && LX.log('info', engineTrail.reset().add({
            message: 'lockConsumer() - obtain mutex'
          }).toString());
        });
      } else {
        onResolved();
        LX.isEnabledFor('info') && LX.log('info', engineTrail.reset().add({
          message: 'lockConsumer() - skipped'
        }).toString());
      }
    });
  }

  var unlockConsumer = function(consumerSandbox) {
    if (consumerSandbox.mutex) {
      consumerSandbox.mutex.unlock();
      LX.isEnabledFor('info') && LX.log('info', engineTrail.reset().add({
          message: 'unlockConsumer() - release mutex'
        }).toString());
    } else {
      LX.isEnabledFor('info') && LX.log('info', engineTrail.reset().add({
          message: 'unlockConsumer() - skipped'
        }).toString());
    }
  }

  var getSandboxNameOf = function(defaultSandbox, sharingLevel) {
    defaultSandbox = defaultSandbox || {};
    return defaultSandbox.name + '[' + sharingLevel + ']' + (new Date()).toISOString();
  }

  Object.defineProperty(self, 'engineId', {
    get: function() { return engineId },
    set: function(value) {}
  });

  LX.isEnabledFor('info') && LX.log('info', engineTrail.reset().add({
    message: 'Engine.new() end!'
  }).toString());
};

module.exports = Engine;
