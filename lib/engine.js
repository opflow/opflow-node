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
  LX.isEnabledFor('verbose') && LX.log('verbose', ' + constructor begin ...');

  params = params || {};
  var self = this;

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

  LX.isEnabledFor('verbose') && LX.log('verbose', ' - configuration object: %s', JSON.stringify(config));

  this.ready = function() {
    var sandbox = getProducerState();
    return getChannel(sandbox).then(function(ch) {
      return assertExchange(sandbox);
    });
  }

  this.produce = function(data, opts, override) {
    override = override || {};
    opts = opts || {};
    opts.appId = opts.appId || config.applicationId;
    LX.isEnabledFor('verbose') && LX.log('verbose', 'produce() a message to exchange/queue');
    var ok = lockProducer().then(function(sandbox) {
      var sendTo = function() {
        sandbox.sendable = sandbox.channel.publish(config.exchangeName, 
          override.routingKey || config.routingKey, misc.bufferify(data), opts);
      }
      if (sandbox.sendable !== false) {
        sendTo();
        LX.isEnabledFor('verbose') && LX.log('verbose', 'Producer channel is writable, msg has been sent');
        return sandbox.sendable;
      } else {
        LX.isEnabledFor('verbose') && LX.log('verbose', 'Producer channel is overflowed, waiting');
        return new Promise(function(resolved, rejected) {
          sandbox.channel.once('drain', function() {
            sendTo();
            LX.isEnabledFor('verbose') && LX.log('verbose', 'Producer channel is drained, flushed');
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

    return getConsumerState(options).then(function(sandbox) {
      sandbox.count = sandbox.count || 0;
      sandbox.replyToName = options.replyToName || options.replyTo;

      LX.isEnabledFor('verbose') && LX.log('verbose', 'consume() - consume a message from Queue');
      return getChannel(sandbox).then(function(ch) {

        var ok = assertSubscriber(sandbox, options);

        ok = ok.then(function(qok) {
          if (options.binding === false) {
            LX.isEnabledFor('verbose') && LX.log('verbose', 'consume() - queue: %s is standalone', qok.queue);
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
            LX.isEnabledFor('verbose') && LX.log('verbose', 'consume() - queue: %s has been bound', qok.queue);
            return qok;
          });
        });

        ok = ok.then(function(qok) {
          LX.isEnabledFor('verbose') && LX.log('verbose', 'consume() - queue info: %s', JSON.stringify(qok));
          var ch_consume = Promise.promisify(ch.consume, {context: ch});
          return ch_consume(qok.queue, function(msg) {
            sandbox.count++;
            L0.isEnabledFor('verbose') && L0.log('verbose', 'consume() - received message: %s, fields: %s, properties: %s, amount: %s', 
              msg.content, JSON.stringify(msg.fields), JSON.stringify(msg.properties), sandbox.count);
            var done = function(err, result) {
              L0.isEnabledFor('verbose') && L0.log('verbose', 'consume() - processed message: %s', msg.content);
              sandbox.count--;
              if (options.noAck !== true && options.manualAck !== true) ch.ack(msg);
            }
            if (config.applicationId && config.applicationId != msg.properties.appId) {
              done();
              return;
            }
            try {
              callback(msg, sandbox, done);
            } catch (exception) {
              L0.isEnabledFor('verbose') && console.log('consume() - exception: ', exception);
              L0.isEnabledFor('verbose') && L0.log('verbose', 'consume() - exception: %s', JSON.stringify(exception));
              done(exception);
            }
          }, {noAck: options.noAck});
        });

        return ok.then(function(result) {
          LX.isEnabledFor('verbose') && LX.log('verbose', 'consume() - consumerTag: %s. CTRL+C to exit', result.consumerTag);
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
        LX.isEnabledFor('verbose') && LX.log('verbose', 'cancelConsumer() subscriber[%s] has been invoked', sandbox.consumerTag);
        var ch_cancel = Promise.promisify(sandbox.channel.cancel, {
          context: sandbox.channel
        });
        return ch_cancel(sandbox.consumerTag).then(function(ok) {
          delete sandbox.consumerTag;
          LX.isEnabledFor('verbose') && LX.log('verbose', 'cancelConsumer() - subscriber is cancelled: %s', JSON.stringify(ok));
          return true;
        }).delay(100);
      }
      return true;
    }).then(function() {
      if (sandbox.forceNewChannel || sandbox.forceNewConnection) return closeChannel(sandbox);
      LX.isEnabledFor('verbose') && LX.log('verbose', 'cancelConsumer() - skip close channel on (%s)', sandbox.name);
      return true;
    }).then(function() {
      if (sandbox.forceNewConnection) return closeConnection(sandbox);
      LX.isEnabledFor('verbose') && LX.log('verbose', 'cancelConsumer() - skip close connection on (%s)', sandbox.name);
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
      LX.isEnabledFor('verbose') && LX.log('verbose', 'close() - cancel producer');
      return cancelProducer(getProducerState());
    }).then(function() {
      if (config.mode && config.mode != 'engine') return true;
      LX.isEnabledFor('verbose') && LX.log('verbose', 'close() - cancel consumers');
      return Promise.mapSeries(consumerRefs, function(consumerRef) {
        return self.cancelConsumer(consumerRef);
      });
    }).then(function() {
      LX.isEnabledFor('verbose') && LX.log('verbose', 'close() - close default consumer channel');
      return getConsumerState().then(closeChannel);
    }).then(function() {
      LX.isEnabledFor('verbose') && LX.log('verbose', 'close() - close default consumer connection');
      return getConsumerState().then(closeConnection);
    }).catch(function(err) {
      LX.isEnabledFor('verbose') && LX.log('verbose', 'close() - catched error: %s', JSON.stringify(err));
      return Promise.reject(err);
    });
  }

  var getConnection = function(sandbox) {
    sandbox = sandbox || {};
    sandbox.connectionCount = sandbox.connectionCount || 0;
    LX.isEnabledFor('verbose') && LX.log('verbose', 'getConnection() - connection on (%s) amount: %s', 
      sandbox.name, sandbox.connectionCount);
    if (config.connectIsCached !== false && sandbox.connection) {
      LX.isEnabledFor('verbose') && LX.log('verbose', 'getConnection() - connection has been available');
      return Promise.resolve(sandbox.connection);
    } else {
      LX.isEnabledFor('verbose') && LX.log('verbose', 'getConnection() - make a new connection');
      var amqp_connect = Promise.promisify(amqp.connect, {context: amqp});
      return amqp_connect(config.uri, {}).then(function(conn) {
        sandbox.connectionCount += 1;
        conn.on('close', function() {
          sandbox.connection = null;
          sandbox.connectionCount -= 1;
          LX.isEnabledFor('verbose') && LX.log('verbose', 'connection on (%s) has been closed', sandbox.name);
        });
        conn.on('error', function(error) {
          LX.isEnabledFor('verbose') && LX.log('verbose', 'connection on (%s) has been failed', sandbox.name);
        })
        LX.isEnabledFor('verbose') && LX.log('verbose', 'getConnection() - connection is created successfully');
        return (sandbox.connection = conn);
      });
    }
  }

  var closeConnection = function(sandbox) {
    sandbox = sandbox || {};
    LX.isEnabledFor('verbose') && LX.log('verbose', 'closeConnection() - close %s', sandbox.name);
    if (sandbox.connection) {
      var connection_close = Promise.promisify(sandbox.connection.close, {
        context: sandbox.connection
      });
      return connection_close().then(function() {
        delete sandbox.connection;
        LX.isEnabledFor('verbose') && LX.log('verbose', 'closeConnection() - connection is closed');
        return true;
      });
    }
    return Promise.resolve(true);
  }

  var getChannel = function(sandbox, opts) {
    sandbox = sandbox || {};
    opts = opts || {};
    if (config.channelIsCached !== false && sandbox.channel) {
      LX.isEnabledFor('verbose') && LX.log('verbose', 'getChannel() - channel on (%s) has been available', sandbox.name);
      return Promise.resolve(sandbox.channel);
    } else {
      LX.isEnabledFor('verbose') && LX.log('verbose', 'getChannel() - make a new channel on (%s)', sandbox.name);
      return getConnection(sandbox).then(function(conn) {
        LX.isEnabledFor('verbose') && LX.log('verbose', 'getChannel() - connection on (%s) is available', sandbox.name);
        var createChannel = Promise.promisify(conn.createChannel, {context: conn});
        return createChannel().then(function(ch) {
          ch.on('close', function() {
            sandbox.channel = null;
          });
          LX.isEnabledFor('verbose') && LX.log('verbose', 'getChannel() - channel on (%s) is created', sandbox.name);
          return (sandbox.channel = ch);
        });
      });
    }
  }

  var closeChannel = function(sandbox) {
    sandbox = sandbox || {};
    LX.isEnabledFor('verbose') && LX.log('verbose', 'closeChannel() - close %s', sandbox.name);
    if (sandbox.channel) {
      var ch_close = Promise.promisify(sandbox.channel.close, {
        context: sandbox.channel
      });
      return ch_close().then(function() {
        delete sandbox.channel;
        LX.isEnabledFor('verbose') && LX.log('verbose', 'closeChannel() - channel on (%s) is closed', sandbox.name);
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
      qp = ch_assertQueue(options.queueName, {
        durable: options.durable,
        exclusive: options.exclusive,
        autoDelete: options.autoDelete
      });
    } else {
      qp = ch_assertQueue(null, {
        durable: false, exclusive: true, autoDelete: true
      }).then(function(qok) {
        options.queueName = options.queueName || qok.queue;
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
      LX.isEnabledFor('verbose') && LX.log('verbose', 'assertSubscriber() - set channel prefetch: %s', options.prefetch);
      ch.prefetch(options.prefetch, true);
    }
    return assertQueue(sandbox, options).then(function(qok) {
      if (options.maxSubscribers && options.maxSubscribers <= qok.consumerCount) {
        var error = {
          consumerCount: qok.consumerCount,
          maxSubscribers: options.maxSubscribers,
          message: 'exceeding quota limits of subscribers'
        }
        LX.isEnabledFor('verbose') && LX.log('verbose', 'assertSubscriber() - Error: %s', JSON.stringify(error));
        return Promise.reject(error);
      } else {
        LX.isEnabledFor('verbose') && LX.log('verbose', 'assertSubscriber() - queue: %s', JSON.stringify(qok));
        return qok;
      }
    });
  }

  var getProducerState = function() {
    self.producerState = self.producerState || {};
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
              LX.isEnabledFor('verbose') && LX.log('verbose', 'lockProducer() - obtain semaphore: %s', producerState.count);
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
      LX.isEnabledFor('verbose') && LX.log('verbose', 'unlockProducer() - release semaphore: %s', producerState.count);
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
    self.consumerState.name = self.consumerState.name || 'consumerSandbox';
    LX.isEnabledFor('verbose') && LX.log('verbose', 'getConsumerState() - on sandbox: %s', self.consumerState.name);
    if (!opts) return Promise.resolve(self.consumerState);
    if (opts.forceNewConnection) {
      return Promise.resolve({
        name: getSandboxNameOf(self.consumerState, 1),
        forceNewConnection: true
      });
    }
    if (opts.forceNewChannel) {
      return getConnection(self.consumerState).then(function(conn) {
        return {
          name: getSandboxNameOf(self.consumerState, 2),
          forceNewChannel: true,
          connection: conn
        }
      });
    }
    return getChannel(self.consumerState).then(function(ch) {
      return {
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

  LX.isEnabledFor('verbose') && LX.log('verbose', ' - constructor end!');
};

module.exports = Engine;
