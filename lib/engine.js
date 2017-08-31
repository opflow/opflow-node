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

  debugx.enabled && debugx(' - configuration object: %s', JSON.stringify(config));

  Object.defineProperty(this, 'config', {
    get: function() { return config; },
    set: function(value) {}
  });

  debugx.enabled && debugx(' - constructor end!');
};

var getConnect = function() {
  var self = this;

  self.store.connectionCount = self.store.connectionCount || 0;
  debugx.enabled && debugx('getConnect() - connection amount: %s', self.store.connectionCount);

  if (self.config.connectIsCached !== false && self.store.connection) {
    debugx.enabled && debugx('getConnect() - connection has been available');
    return Promise.resolve(self.store.connection);
  } else {
    debugx.enabled && debugx('getConnect() - make a new connection');
    var amqp_connect = Promise.promisify(amqp.connect, {context: amqp});
    return amqp_connect(self.config.uri, {}).then(function(conn) {
      self.store.connectionCount += 1;
      conn.on('close', function() {
        self.store.connection = null;
        self.store.connectionCount--;
      });
      debugx.enabled && debugx('getConnect() - connection is created successfully');
      return (self.store.connection = conn);
    });
  }
}

var getChannel = function() {
  var self = this;

  if (self.config.channelIsCached !== false && self.store.channel) {
    debugx.enabled && debugx('getChannel() - channel has been available (%s)', self.name);
    return Promise.resolve(self.store.channel);
  } else {
    debugx.enabled && debugx('getChannel() - make a new channel (%s)', self.name);
    return getConnect.call(self).then(function(conn) {
      debugx.enabled && debugx('getChannel() - connection has already (%s)', self.name);
      var createChannel = Promise.promisify(conn.createChannel, {context: conn});
      return createChannel().then(function(ch) {
        ch.on('close', function() {
          self.store.channel = null;
        });
        debugx.enabled && debugx('getChannel() - channel is created (%s)', self.name);
        return (self.store.channel = ch);
      });
    });
  }
}

var assertExchange = function(ch) {
  var self = this;
  if (self.store.exchangeAsserted) return Promise.resolve();
  var ch_assertExchange = Promise.promisify(ch.assertExchange, {context: ch});
  return ch_assertExchange(self.config.exchangeName, self.config.exchangeType, {
    durable: self.config.durable,
    autoDelete: self.config.autoDelete
  }).then(function(eok) {
    self.store.exchangeAsserted = true;
    return eok;
  });
}

var retrieveExchange = function(override) {
  var self = this;
  override = override || {};
  return getChannel.call(self).then(function(ch) {
    var ok = assertExchange.call(self, ch);
    return ok.then(function(eok) {
      return {
        channel: ch,
        exchangeName: self.config.exchangeName,
        routingKey: override.routingKey || self.config.routingKey
      };
    });
  });
}

var assertQueue = function(ch) {
  var self = this;
  var ch_assertQueue = Promise.promisify(ch.assertQueue, {context: ch});
  if (self.config.queueName) {
    return ch_assertQueue(self.config.queueName, {
      durable: self.config.durable,
      exclusive: self.config.exclusive,
      autoDelete: self.config.autoDelete
    }).then(function(qok) {
      self.config.queueName = self.config.queueName || qok.queue;
      return qok;
    });
  } else {
    return ch_assertQueue(null, {
      durable: false, exclusive: true, autoDelete: true
    }).then(function(qok) {
      self.config.queueName = self.config.queueName || qok.queue;
      return qok;
    });
  }
}

var assertSubscriber = function(ch) {
  var self = this;
  if (self.config.prefetch && self.config.prefetch >= 0) {
    debugx.enabled && debugx('assertSubscriber() - set channel prefetch: %s', self.config.prefetch);
    ch.prefetch(self.config.prefetch, true);
  }
  return assertQueue.call(self, ch).then(function(qok) {
    if (self.config.maxSubscribers && self.config.maxSubscribers <= qok.consumerCount) {
      var error = {
        consumerCount: qok.consumerCount,
        maxSubscribers: self.config.maxSubscribers,
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
  this.producerState = this.producerState || {};
  if (this.config.exchangeQuota > 0 && !this.producerState.fence) {
    this.producerState.count = this.producerState.count || 0;
    this.producerState.fence = this.producerState.fence || locks.createSemaphore(this.config.exchangeQuota);
  }
  return (this.producerState);
}

var lockProducer = function(override) {
  var self = this;
  var producerState = getProducerState.call(self);
  return retrieveExchange.call({ config: self.config, store: producerState }, override).then(function(ref) {
    if (producerState.fence) {
      return new Promise(function(onResolved, onRejected) {
        producerState.fence.wait(function whenResourceAvailable() {
          self.producerState.count++;
          debugx.enabled && debugx('lockProducer() - obtain semaphore: %s', self.producerState.count);
          onResolved(ref);
        });
      });
    }
    return ref;
  });
}

var unlockProducer = function() {
  var self = this;
  var producerState = getProducerState.call(self);
  if (producerState.fence) {
    self.producerState.count--;
    debugx.enabled && debugx('lockProducer() - release semaphore: %s', self.producerState.count);
    producerState.fence.signal();
  }
}

Engine.prototype.produce = function(data, opts, override) {
  var self = this;
  opts = opts || {};
  debugx.enabled && debugx('produce() an object to rabbitmq');
  var ok = lockProducer.call(self, override).then(function(ref) {
    var sendTo = function() {
      self.config.sendable = ref.channel.publish(ref.exchangeName, ref.routingKey, misc.bufferify(data), opts);
    }
    if (self.config.sendable !== false) {
      sendTo();
      debugx.enabled && debugx('Producer channel is writable, msg has been sent');
      return self.config.sendable;
    } else {
      debugx.enabled && debugx('Producer channel is overflowed, waiting');
      return new Promise(function(resolved, rejected) {
        ref.channel.once('drain', function() {
          sendTo();
          debugx.enabled && debugx('Producer channel is drained, flushed');
          resolved(self.config.sendable);
        });
      });
    }
  }).then(function(result) {
    unlockProducer.call(self);
    return result;
  });
  if (self.config.delayTime > 0) {
    ok = ok.delay(self.config.delayTime);
  }
  return ok;
}

Engine.prototype.ready = function() {
  var self = this;
  return getChannel.call({ config: self.config, store: getProducerState.call(self) }).then(function(ch) {
    return assertExchange.call({ config: self.config, store: getProducerState.call(self) }, ch);
  });
}

Engine.prototype.acquireChannel = function(callback) {
  var self = this;
  var ok = getConnect.call({ config: self.config, store: getProducerState.call(self) });

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

var getConsumerState = function() {
  return (this.consumerState = this.consumerState || {});
}

Engine.prototype.consume = function(callback, options) {
  var self = this;

  assert.ok(lodash.isFunction(callback), 'callback should be a function');
  options = options || {};

  var consumerState = {};
  var vc = options.forceNewChannel ? {
    config: self.config,
    store: consumerState
  } : {
    config: self.config,
    store: getConsumerState.call(self)
  };
  vc.store.count = vc.store.count || 0;
  vc.store.replyToName = options.replyToName || options.replyTo;

  debugx.enabled && debugx('consume() - consume a message from Queue');
  return getChannel.call(vc).then(function(ch) {

    var ok = assertSubscriber.call({ config: options }, ch);

    ok = ok.then(function(qok) {
      if (options.binding === false) {
        debugx.enabled && debugx('consume() - queue: %s is standalone', qok.queue);
        return qok;
      }
      var ch_bindQueue = Promise.promisify(ch.bindQueue, {context: ch});
      return assertExchange.call({ config: self.config, store: vc.store }, ch).then(function() {
        return ch_bindQueue(qok.queue, self.config.exchangeName, self.config.routingKey, {});
      }).then(function() {
        if (!lodash.isArray(self.config.otherKeys)) return Promise.resolve();
        return Promise.mapSeries(self.config.otherKeys, function(routingKey) {
          return ch_bindQueue(qok.queue, self.config.exchangeName, routingKey, {});
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
        vc.store.count++;
        debug0.enabled && debug0('consume() - received message: %s, fields: %s, properties: %s, amount: %s', 
          msg.content, JSON.stringify(msg.fields), JSON.stringify(msg.properties), vc.store.count);
        var done = function(err, result) {
          debug0.enabled && debug0('consume() - processed message: %s', msg.content);
          vc.store.count--;
          if (options.noAck !== true && options.manualAck !== true) ch.ack(msg);
        }
        try {
          callback(msg, vc.store, done);
        } catch (exception) {
          console.log('Exception: ', exception);
          debug0.enabled && debug0('consume() - exception: %s', JSON.stringify(exception));
          done();
        }
      }, {noAck: options.noAck});
    });

    return ok.then(function(result) {
      debugx.enabled && debugx('consume() - consumerTag: %s. CTRL+C to exit', result.consumerTag);
      vc.store.consumerTag = result.consumerTag;
      return vc.store;
    });
  });
}

Engine.prototype.destroy = function() {
  var self = this;
  return Promise.resolve().then(function() {
    return stopPublisher.call({store: getProducerState.call(self) });
  }).then(function() {
    return stopSubscriber.call({store: getConsumerState.call(self) });
  });
}

var stopPublisher = function() {
  var self = this;
  return Promise.resolve().then(function() {
    if (self.store.channel) {
      self.store.channel.removeAllListeners('drain');
    }
    return true;
  }).then(function() {
    if (self.store.channel) {
      var ch_close = Promise.promisify(self.store.channel.close, {
        context: self.store.channel
      });
      return ch_close().then(function(ok) {
        delete self.store.channel;
        debugx.enabled && debugx('destroy() - channel is closed: %s', JSON.stringify(ok));
        return true;
      });
    }
    return true;
  }).then(function() {
    if (self.store.connection) {
      var ch_close = Promise.promisify(self.store.connection.close, {
        context: self.store.connection
      });
      return ch_close().then(function(ok) {
        delete self.store.connection;
        debugx.enabled && debugx('destroy() - connection is closed: %s', JSON.stringify(ok));
        return true;
      });
    }
    return true;
  });
}

var stopSubscriber = function() {
  var self = this;
  return Promise.resolve().then(function() {
    if (self.store.consumerTag && self.store.channel) {
      debugx.enabled && debugx('destroy() subscriber[%s] has been invoked', self.store.consumerTag);
      var ch_cancel = Promise.promisify(self.store.channel.cancel, {
        context: self.store.channel
      });
      return ch_cancel(self.store.consumerTag).then(function(ok) {
        delete self.store.consumerTag;
        debugx.enabled && debugx('destroy() - subscriber is cancelled: %s', JSON.stringify(ok));
        return true;
      }).delay(200);
    }
    return true;
  }).then(function() {
    if (self.store.channel) {
      var ch_close = Promise.promisify(self.store.channel.close, {
        context: self.store.channel
      });
      return ch_close().then(function(ok) {
        delete self.store.channel;
        debugx.enabled && debugx('destroy() - channel is closed: %s', JSON.stringify(ok));
        return true;
      });
    }
    return true;
  }).then(function() {
    if (self.store.connection) {
      var ch_close = Promise.promisify(self.store.connection.close, {
        context: self.store.connection
      });
      return ch_close().then(function(ok) {
        delete self.store.connection;
        debugx.enabled && debugx('destroy() - connection is closed: %s', JSON.stringify(ok));
        return true;
      });
    }
    return true;
  });
}

module.exports = Engine;
