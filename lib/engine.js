'use strict';

var assert = require('assert');
var Promise = require('bluebird');
var amqp = require('amqplib/callback_api');
var lodash = require('lodash');
var locks = require('locks');
var debug = require('debug');
var debugx = debug('opflow:engine');
var debug0 = debug('trace:opflow:engine');
var misc = require('./misc');

var Engine = function(params) {
  debugx.enabled && debugx(' + constructor begin ...');

  var self = this;
  self.logger = self.logger || params.logger;

  var common = {};
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

  config.delayTime = (typeof(params.delayTime) === 'number') ? params.delayTime : 0;
  config.waitSendToQueueDone = typeof(params.waitSendToQueueDone) === 'boolean' ? params.waitSendToQueueDone : true;
  config.privateConnection = typeof(params.privateConnection) === 'boolean' ? params.privateConnection : true;

  debugx.enabled && debugx(' - configuration object: %s', JSON.stringify(config));

  this.ready = function() {
    var sandbox = getProducerState();
    return getChannel(sandbox).then(function(ch) {
      return assertExchange(sandbox);
    });
  }

  this.produce = function(data, opts, override) {
    opts = opts || {};
    debugx.enabled && debugx('produce() a message to exchange/queue');
    var ok = lockProducer(override).then(function(ref) {
      var sendTo = function() {
        config.sendable = ref.channel.publish(ref.exchangeName, ref.routingKey, misc.bufferify(data), opts);
      }
      if (config.sendable !== false) {
        sendTo();
        debugx.enabled && debugx('Producer channel is writable, msg has been sent');
        return config.sendable;
      } else {
        debugx.enabled && debugx('Producer channel is overflowed, waiting');
        return new Promise(function(resolved, rejected) {
          ref.channel.once('drain', function() {
            sendTo();
            debugx.enabled && debugx('Producer channel is drained, flushed');
            resolved(config.sendable);
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

    var sandbox = getConsumerState(options.forceNewChannel);
    sandbox.count = sandbox.count || 0;
    sandbox.replyToName = options.replyToName || options.replyTo;

    debugx.enabled && debugx('consume() - consume a message from Queue');
    return getChannel(sandbox).then(function(ch) {

      var ok = assertSubscriber(sandbox, options);

      ok = ok.then(function(qok) {
        if (options.binding === false) {
          debugx.enabled && debugx('consume() - queue: %s is standalone', qok.queue);
          return qok;
        }
        var ch_bindQueue = Promise.promisify(ch.bindQueue, {context: ch});
        return assertExchange(sandbox).then(function() {
          return ch_bindQueue(qok.queue, config.exchangeName, config.routingKey, {});
        }).then(function() {
          if (!lodash.isArray(config.otherKeys)) return Promise.resolve();
          return Promise.mapSeries(config.otherKeys, function(routingKey) {
            return ch_bindQueue(qok.queue, config.exchangeName, routingKey, {});
          });
        }).then(function() {
          debugx.enabled && debugx('consume() - queue: %s has been bound', qok.queue);
          return qok;
        });
      });

      ok = ok.then(function(qok) {
        debugx.enabled && debugx('consume() - queue info: %s', JSON.stringify(qok));
        var ch_consume = Promise.promisify(ch.consume, {context: ch});
        return ch_consume(qok.queue, function(msg) {
          sandbox.count++;
          debug0.enabled && debug0('consume() - received message: %s, fields: %s, properties: %s, amount: %s', 
            msg.content, JSON.stringify(msg.fields), JSON.stringify(msg.properties), sandbox.count);
          var done = function(err, result) {
            debug0.enabled && debug0('consume() - processed message: %s', msg.content);
            sandbox.count--;
            if (options.noAck !== true && options.manualAck !== true) ch.ack(msg);
          }
          try {
            callback(msg, sandbox, done);
          } catch (exception) {
            console.log('Exception: ', exception);
            debug0.enabled && debug0('consume() - exception: %s', JSON.stringify(exception));
            done();
          }
        }, {noAck: options.noAck});
      });

      return ok.then(function(result) {
        debugx.enabled && debugx('consume() - consumerTag: %s. CTRL+C to exit', result.consumerTag);
        sandbox.consumerTag = result.consumerTag;
        return sandbox;
      });
    });
  }

  this.acquireChannel = function(callback, sandbox) {
    var ok = getConnection(common);

    ok = ok.then(function(conn) {
      var createChannel = Promise.promisify(conn.createChannel, {context: conn});
      return createChannel().then(function(ch) {
        debugx.enabled && debugx('acquireChannel() - channel is created');
        return (ch);
      });
    });

    ok = ok.then(function(ch) {
      var result = Promise.resolve(callback(Promise.resolve(ch)));
      return result.finally(function() {
        var ch_close = Promise.promisify(ch.close, { context: ch });
        return ch_close().then(function(info) {
          debugx.enabled && debugx('acquireChannel() - channel is closed: %s', JSON.stringify(info));
          return result;
        });
      });
    });
    
    return ok;
  }

  this.destroy = function() {
    return Promise.resolve().then(function() {
      return stopPublisher(getProducerState());
    }).then(function() {
      return stopSubscriber(getConsumerState());
    }).then(function() {
      return closeConnection(common);
    });
  }

  var getConnection = function(sandbox) {
    sandbox = sandbox || {};
    sandbox.connectionCount = sandbox.connectionCount || 0;
    debugx.enabled && debugx('getConnection() - connection amount: %s', sandbox.connectionCount);
    if (config.connectIsCached !== false && sandbox.connection) {
      debugx.enabled && debugx('getConnection() - connection has been available');
      return Promise.resolve(sandbox.connection);
    } else {
      debugx.enabled && debugx('getConnection() - make a new connection');
      var amqp_connect = Promise.promisify(amqp.connect, {context: amqp});
      return amqp_connect(config.uri, {}).then(function(conn) {
        sandbox.connectionCount += 1;
        conn.on('close', function() {
          sandbox.connection = null;
          sandbox.connectionCount--;
        });
        debugx.enabled && debugx('getConnection() - connection is created successfully');
        return (sandbox.connection = conn);
      });
    }
  }

  var closeConnection = function(sandbox) {
    sandbox = sandbox || {};
    if (sandbox.connection) {
      var ch_close = Promise.promisify(sandbox.connection.close, {
        context: sandbox.connection
      });
      return ch_close().then(function(ok) {
        delete sandbox.connection;
        debugx.enabled && debugx('closeConnection() - connection is closed: %s', JSON.stringify(ok));
        return true;
      });
    }
    return Promise.resolve(true);
  }

  var getChannel = function(sandbox, opts) {
    sandbox = sandbox || {};
    opts = opts || {};
    if (config.channelIsCached !== false && sandbox.channel) {
      debugx.enabled && debugx('getChannel() - channel has been available (%s)', opts.name);
      return Promise.resolve(sandbox.channel);
    } else {
      debugx.enabled && debugx('getChannel() - make a new channel (%s)', opts.name);
      var store = config.privateConnection ? sandbox : common;
      return getConnection(store).then(function(conn) {
        debugx.enabled && debugx('getChannel() - connection has already (%s)', opts.name);
        var createChannel = Promise.promisify(conn.createChannel, {context: conn});
        return createChannel().then(function(ch) {
          ch.on('close', function() {
            sandbox.channel = null;
          });
          debugx.enabled && debugx('getChannel() - channel is created (%s)', opts.name);
          return (sandbox.channel = ch);
        });
      });
    }
  }

  var closeChannel = function(sandbox) {
    if (sandbox.channel) {
      var ch_close = Promise.promisify(sandbox.channel.close, {
        context: sandbox.channel
      });
      return ch_close().then(function(ok) {
        delete sandbox.channel;
        debugx.enabled && debugx('destroy() - channel is closed: %s', JSON.stringify(ok));
        return true;
      });
    }
    return Promise.resolve(true);
  }

  var assertExchange = function(sandbox) {
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

  var retrieveExchange = function(override, sandbox) {
    override = override || {};
    return getChannel(sandbox).then(function(ch) {
      var ok = assertExchange(sandbox);
      return ok.then(function(eok) {
        return {
          channel: ch,
          exchangeName: config.exchangeName,
          routingKey: override.routingKey || config.routingKey
        };
      });
    });
  }

  var assertQueue = function(sandbox, config) {
    var ch = sandbox.channel;
    var ch_assertQueue = Promise.promisify(ch.assertQueue, {context: ch});
    if (config.queueName) {
      return ch_assertQueue(config.queueName, {
        durable: config.durable,
        exclusive: config.exclusive,
        autoDelete: config.autoDelete
      }).then(function(qok) {
        config.queueName = config.queueName || qok.queue;
        return qok;
      });
    } else {
      return ch_assertQueue(null, {
        durable: false, exclusive: true, autoDelete: true
      }).then(function(qok) {
        config.queueName = config.queueName || qok.queue;
        return qok;
      });
    }
  }

  var assertSubscriber = function(sandbox, config) {
    var ch = sandbox.channel;
    if (config.prefetch && config.prefetch >= 0) {
      debugx.enabled && debugx('assertSubscriber() - set channel prefetch: %s', config.prefetch);
      ch.prefetch(config.prefetch, true);
    }
    return assertQueue(sandbox, config).then(function(qok) {
      if (config.maxSubscribers && config.maxSubscribers <= qok.consumerCount) {
        var error = {
          consumerCount: qok.consumerCount,
          maxSubscribers: config.maxSubscribers,
          message: 'exceeding quota limits of subscribers'
        }
        debugx.enabled && debugx('assertSubscriber() - Error: %s', JSON.stringify(error));
        return Promise.reject(error);
      } else {
        debugx.enabled && debugx('assertSubscriber() - queue: %s', JSON.stringify(qok));
        return qok;
      }
    });
  }

  var getProducerState = function() {
    self.producerState = self.producerState || {};
    if (config.exchangeQuota > 0 && !self.producerState.fence) {
      self.producerState.count = self.producerState.count || 0;
      self.producerState.fence = self.producerState.fence || locks.createSemaphore(config.exchangeQuota);
    }
    return (self.producerState);
  }

  var lockProducer = function(override) {
    var producerState = getProducerState();
    return retrieveExchange(override, producerState).then(function(ref) {
      if (producerState.fence) {
        return new Promise(function(onResolved, onRejected) {
          producerState.fence.wait(function whenResourceAvailable() {
            producerState.count++;
            debugx.enabled && debugx('lockProducer() - obtain semaphore: %s', producerState.count);
            onResolved(ref);
          });
        });
      }
      return ref;
    });
  }

  var unlockProducer = function() {
    var producerState = getProducerState();
    if (producerState.fence) {
      producerState.count--;
      debugx.enabled && debugx('lockProducer() - release semaphore: %s', producerState.count);
      producerState.fence.signal();
    }
  }

  var getConsumerState = function(forceNewChannel) {
    if (forceNewChannel) return {};
    self.consumerState = self.consumerState || {};
    return self.consumerState;
  }

  var stopPublisher = function(sandbox) {
    return Promise.resolve().then(function() {
      if (sandbox.channel) {
        sandbox.channel.removeAllListeners('drain');
      }
      return true;
    }).then(function() {
      return closeChannel(sandbox);
    }).then(function(result) {
      return closeConnection(sandbox);
    });
  }

  var stopSubscriber = function(sandbox) {
    return Promise.resolve().then(function() {
      if (sandbox.consumerTag && sandbox.channel) {
        debugx.enabled && debugx('destroy() subscriber[%s] has been invoked', sandbox.consumerTag);
        var ch_cancel = Promise.promisify(sandbox.channel.cancel, {
          context: sandbox.channel
        });
        return ch_cancel(sandbox.consumerTag).then(function(ok) {
          delete sandbox.consumerTag;
          debugx.enabled && debugx('destroy() - subscriber is cancelled: %s', JSON.stringify(ok));
          return true;
        }).delay(200);
      }
      return true;
    }).then(function() {
      return closeChannel(sandbox);
    }).then(function(result) {
      return closeConnection(sandbox);
    });
  }

  debugx.enabled && debugx(' - constructor end!');
};

module.exports = Engine;
