'use strict';

var assert = require('assert');
var Promise = require('bluebird');
var amqp = require('amqplib/callback_api');
var lodash = require('lodash');
var locks = require('locks');
var debug = require('debug');
var debugx = debug('opflow:engine');
var debug0 = debug('trace:opflow:engine');
var misc = require('./util');

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

  if (typeof(params.applicationId) === 'string') {
    config.applicationId = params.applicationId;
  }

  config.delayTime = (typeof(params.delayTime) === 'number') ? params.delayTime : 0;

  config.mode = params.mode || 'engine';
  var consumerRefs = [];

  debugx.enabled && debugx(' - configuration object: %s', JSON.stringify(config));

  this.ready = function() {
    return retrieveExchange(getProducerState());
  }

  this.produce = function(data, opts, override) {
    override = override || {};
    opts = opts || {};
    opts.appId = opts.appId || config.applicationId;
    debugx.enabled && debugx('produce() a message to exchange/queue');
    var ok = lockProducer().then(function(sandbox) {
      var sendTo = function() {
        sandbox.sendable = sandbox.channel.publish(config.exchangeName, 
          override.routingKey || config.routingKey, misc.bufferify(data), opts);
      }
      if (sandbox.sendable !== false) {
        sendTo();
        debugx.enabled && debugx('Producer channel is writable, msg has been sent');
        return sandbox.sendable;
      } else {
        debugx.enabled && debugx('Producer channel is overflowed, waiting');
        return new Promise(function(resolved, rejected) {
          sandbox.channel.once('drain', function() {
            sendTo();
            debugx.enabled && debugx('Producer channel is drained, flushed');
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

      debugx.enabled && debugx('consume() - consume a message from Queue');
      return getChannel(sandbox).then(function(ch) {

        var ok = assertSubscriber(sandbox, options);

        ok = ok.then(function(qok) {
          if (options.binding === false) {
            debugx.enabled && debugx('consume() - queue: %s is standalone', qok.queue);
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
            if (config.applicationId && config.applicationId != msg.properties.appId) {
              done();
              return;
            }
            try {
              callback(msg, sandbox, done);
            } catch (exception) {
              debug0.enabled && console.log('consume() - exception: ', exception);
              debug0.enabled && debug0('consume() - exception: %s', JSON.stringify(exception));
              done(exception);
            }
          }, {noAck: options.noAck});
        });

        return ok.then(function(result) {
          debugx.enabled && debugx('consume() - consumerTag: %s. CTRL+C to exit', result.consumerTag);
          sandbox.consumerTag = result.consumerTag;
          if (!config.mode || config.mode == 'engine') consumerRefs.push(sandbox);
          return sandbox;
        });
      });
    });
  }

  this.acquireChannel = function(callback) {
    assert.ok(lodash.isFunction(callback), 'callback should be a function');

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

  this.cancelConsumer = function(sandbox) {
    return Promise.resolve().then(function() {
      if (sandbox.consumerTag && sandbox.channel) {
        debugx.enabled && debugx('cancelConsumer() subscriber[%s] has been invoked', sandbox.consumerTag);
        var ch_cancel = Promise.promisify(sandbox.channel.cancel, {
          context: sandbox.channel
        });
        return ch_cancel(sandbox.consumerTag).then(function(ok) {
          delete sandbox.consumerTag;
          debugx.enabled && debugx('cancelConsumer() - subscriber is cancelled: %s', JSON.stringify(ok));
          return true;
        }).delay(100);
      }
      return true;
    }).then(function() {
      if (sandbox.forceNewChannel || sandbox.forceNewConnection) return closeChannel(sandbox);
      debugx.enabled && debugx('cancelConsumer() - skip close channel');
      return true;
    }).then(function() {
      if (sandbox.forceNewConnection) return closeConnection(sandbox);
      debugx.enabled && debugx('cancelConsumer() - skip close connection');
      return true;
    });
  }

  this.openSession = function(sandbox, options) {
    options = options || {};
    if (!lodash.isObject(sandbox)) return Promise.reject();
    return getChannel(sandbox);
  }

  this.closeSession = function(sandbox) {
    return closeChannel(sandbox).then(function() {
      return closeConnection(sandbox);
    });
  }

  this.destroy = function() {
    return Promise.resolve().then(function() {
      debugx.enabled && debugx('close() - cancel producer');
      return cancelProducer(getProducerState());
    }).then(function() {
      if (config.mode && config.mode != 'engine') return true;
      debugx.enabled && debugx('close() - cancel consumers');
      return Promise.mapSeries(consumerRefs, function(consumerRef) {
        return self.cancelConsumer(consumerRef);
      });
    }).then(function() {
      debugx.enabled && debugx('close() - close channel');
      return closeChannel(getConsumerState());
    }).then(function() {
      debugx.enabled && debugx('close() - close connection');
      return closeConnection(getConsumerState());
    }).then(function() {
      return closeConnection(common);
    }).catch(function(err) {
      debugx.enabled && debugx('close() - catch error: %s', JSON.stringify(err));
      return Promise.reject(err);
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
          sandbox.connectionCount -= 1;
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
      return getConnection(sandbox).then(function(conn) {
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
        debugx.enabled && debugx('closeChannel() - channel is closed: %s', JSON.stringify(ok));
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

  var retrieveExchange = function(sandbox) {
    return getChannel(sandbox).then(function(ch) {
      return assertExchange(sandbox);
    });
  }

  var assertQueue = function(sandbox, options) {
    var ch = sandbox.channel;
    var ch_assertQueue = Promise.promisify(ch.assertQueue, {context: ch});
    if (options.queueName) {
      return ch_assertQueue(options.queueName, {
        durable: options.durable,
        exclusive: options.exclusive,
        autoDelete: options.autoDelete
      });
    } else {
      return ch_assertQueue(null, {
        durable: false, exclusive: true, autoDelete: true
      }).then(function(qok) {
        options.queueName = options.queueName || qok.queue;
        return qok;
      });
    }
  }

  var assertSubscriber = function(sandbox, options) {
    var ch = sandbox.channel;
    if (options.prefetch && options.prefetch >= 0) {
      debugx.enabled && debugx('assertSubscriber() - set channel prefetch: %s', options.prefetch);
      ch.prefetch(options.prefetch, true);
    }
    return assertQueue(sandbox, options).then(function(qok) {
      if (options.maxSubscribers && options.maxSubscribers <= qok.consumerCount) {
        var error = {
          consumerCount: qok.consumerCount,
          maxSubscribers: options.maxSubscribers,
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

  var lockProducer = function() {
    var producerState = getProducerState();
    return retrieveExchange(producerState).then(function() {
      if (producerState.fence) {
        return new Promise(function(onResolved, onRejected) {
          producerState.fence.wait(function whenResourceAvailable() {
            producerState.count++;
            debugx.enabled && debugx('lockProducer() - obtain semaphore: %s', producerState.count);
            onResolved(producerState);
          });
        });
      }
      return producerState;
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
    if (!opts) return Promise.resolve(self.consumerState);
    if (opts.forceNewConnection) {
      return Promise.resolve({ forceNewConnection: true });
    }
    if (opts.forceNewChannel) {
      return getConnection(self.consumerState).then(function(conn) {
        return { connection: conn, forceNewChannel: true }
      });
    }
    return getChannel(self.consumerState).then(function(ch) {
      return { connection: self.consumerState.connection, channel: ch }
    });
  }

  debugx.enabled && debugx(' - constructor end!');
};

module.exports = Engine;
