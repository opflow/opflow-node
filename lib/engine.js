'use strict';

var assert = require('assert');
var Promise = require('bluebird');
var amqp = require('amqplib/callback_api');
var lodash = require('lodash');
var locks = require('locks');
var debug = require('debug');
var debugx = debug('opflow:engine');
var debug0 = debug('trace:opflow:engine');

var Handler = function(params) {
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

  config.delayTime = (typeof(params.delayTime) === 'number') ? params.delayTime : 0;
  config.waitSendToQueueDone = typeof(params.waitSendToQueueDone) === 'boolean' ? params.waitSendToQueueDone : true;

  config.consumer = { uri: config.uri };
  if (params.consumer && typeof(params.consumer) === 'object') {
    lodash.merge(config.consumer, params.consumer);
  }

  if (typeof(config.consumer.queueName) !== 'string') {
    config.consumer.queueName = params.queueName || params.queue || '';
  }
  if (typeof(config.consumer.durable) !== 'boolean') {
    config.consumer.durable = (typeof(params.durable) === 'boolean') ? params.durable : true;
  }
  if (typeof(config.consumer.exclusive) !== 'boolean') {
    config.consumer.exclusive = (typeof(params.exclusive) === 'boolean') ? params.exclusive : false;
  }
  if (typeof(config.consumer.noAck) !== 'boolean') {
    config.consumer.noAck = (typeof(params.noAck) === 'boolean') ? params.noAck : false;
  }
  if (typeof(config.consumer.prefetch) !== 'number') {
    config.consumer.prefetch = (typeof(params.prefetch) === 'number') ? params.prefetch : undefined;
  }
  if (typeof(config.consumer.connectIsCached) !== 'boolean') {
    config.consumer.connectIsCached = typeof(params.connectIsCached) === 'boolean' ? params.connectIsCached : true;
  }
  if (typeof(config.consumer.channelIsCached) !== 'boolean') {
    config.consumer.channelIsCached = typeof(params.channelIsCached) === 'boolean' ? params.channelIsCached : true;
  }
  if (typeof(config.consumer.maxSubscribers) !== 'number') {
    config.consumer.maxSubscribers = params.maxSubscribers;
  }

  if (params.feedback && typeof(params.feedback) === 'object') {
    config.feedback = {
      uri: config.uri
    }
    lodash.merge(config.feedback, params.feedback);
    if (lodash.isEmpty(config.feedback.queueName)) {
      config.feedback.queueName = config.consumer.queueName + '-feedback';
    }
  }

  if (params.recycler && typeof(params.recycler) === 'object') {
    config.recycler = {
      uri: config.uri,
      redeliveredCountName: 'x-redelivered-count',
      redeliveredLimit: 5,
    }
    lodash.merge(config.recycler, params.recycler);
    if (lodash.isEmpty(config.recycler.queueName)) {
      config.recycler.queueName = config.consumer.queueName + '-recycler';
    }
  }

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

var stringify = function(data) {
  return (typeof(data) === 'string') ? data : JSON.stringify(data);
}

var bufferify = function(data) {
  return (data instanceof Buffer) ? data : new Buffer(stringify(data));
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
  return Promise.promisify(ch.assertQueue, {context: ch})(self.config.queueName, {
    durable: self.config.durable,
    exclusive: self.config.exclusive
  }).then(function(qok) {
    self.config.queueName = self.config.queueName || qok.queue;
    return qok;
  });
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

var checkQueue = function(ch) {
  var self = this;
  return Promise.promisify(ch.checkQueue, {context: ch})(self.config.queueName);
}

var purgeQueue = function(ch) {
  var self = this;
  return Promise.promisify(ch.purgeQueue, {context: ch})(self.config.queueName);
}

var sendToQueue = function(data, opts) {
  var self = this;
  opts = opts || {};
  var sendTo = function(ch) {
    self.config.sendable = ch.sendToQueue(self.config.queueName, bufferify(data), opts);
  }
  debugx.enabled && debugx('%s() an object to rabbitmq queue', self.name);
  return getChannel.call({ name: self.name, config: self.config, store: self.store }).then(function(ch) {
    return assertQueue.call({ config: self.config }, ch).then(function() {
      if (self.config.sendable !== false) {
        sendTo(ch);
        debugx.enabled && debugx('%s() channel is writable, msg has been sent', self.name);
      } else {
        ch.once('drain', function() {
          sendTo(ch);
          debugx.enabled && debugx('%s() channel is drained, flushed', self.name);
        });
        debugx.enabled && debugx('%s() channel is overflowed, waiting', self.name);
      }
      return self.config.sendable;
    });
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

Handler.prototype.produce = function(data, opts, override) {
  var self = this;
  opts = opts || {};
  debugx.enabled && debugx('produce() an object to rabbitmq');
  var ok = lockProducer.call(self, override).then(function(ref) {
    var sendTo = function() {
      self.config.sendable = ref.channel.publish(ref.exchangeName, ref.routingKey, bufferify(data), opts);
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

Handler.prototype.ready = function() {
  var self = this;
  return getChannel.call({ config: self.config, store: getProducerState.call(self) }).then(function(ch) {
    return assertExchange.call({ config: self.config, store: getProducerState.call(self) }, ch);
  });
}

Handler.prototype.readyConsumer = function() {
  var self = this;
  return Promise.resolve().then(function() {
    return self.checkChain();
  }).then(function() {
    if (!hasFeedback.call(self)) return Promise.resolve();
    return self.checkFeedback();
  }).then(function() {
    if (!hasRecycler.call(self)) return Promise.resolve();
    return self.checkTrash();
  });
}

var getConsumerState = function() {
  return (this.consumerState = this.consumerState || {});
}

var enqueueConsumer = function(data, opts) {
  var vc = { name: 'enqueueConsumer', config: this.config.consumer, store: getConsumerState.call(this) };
  return sendToQueue.call(vc, data, opts);
}

Handler.prototype.checkChain = function() {
  var vc = { config: this.config.consumer, store: getConsumerState.call(this) };
  return getChannel.call(vc).then(function(ch) {
    return checkQueue.call({ config: vc.config }, ch);
  });
}

Handler.prototype.purgeChain = function() {
  var vc = { config: this.config.consumer, store: getConsumerState.call(this) };
  return getChannel.call(vc).then(function(ch) {
    return assertQueue.call({ config: vc.config }, ch).then(function() {
      return purgeQueue.call({ config: vc.config }, ch);
    });
  });
}

Handler.prototype.consume = function(callback) {
  var self = this;

  assert.ok(lodash.isFunction(callback), 'callback should be a function');

  var vc = { config: self.config.consumer, store: getConsumerState.call(this) };
  vc.store.count = vc.store.count || 0;

  debugx.enabled && debugx('consume() - get an object from Chain');
  return getChannel.call(vc).then(function(ch) {

    var ok = assertSubscriber.call({ config: vc.config }, ch);

    ok = ok.then(function(qok) {
      if (vc.config.binding === false) {
        debugx.enabled && debugx('consume() - queue: %s is standalone', qok.queue);
        return qok;
      }
      var ch_bindQueue = Promise.promisify(ch.bindQueue, {context: ch});
      return assertExchange.call({ config: self.config, store: vc.store }, ch).then(function() {
        return ch_bindQueue(qok.queue, self.config.exchangeName, self.config.routingKey, {});
      }).then(function() {
        if (!lodash.isArray(vc.config.routingKeys)) return Promise.resolve();
        return Promise.mapSeries(vc.config.routingKeys, function(routingKey) {
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
        var _ctx = (msg.properties.replyTo) ? { queueName: msg.properties.replyTo } : undefined;
        if (hasFeedback.call(self, _ctx)) {
          var properties = lodash.defaultsDeep({headers: {error: false, status: 'started'}}, msg.properties);
          enqueueFeedback.call(self, stringify({}), properties, _ctx);
        }
        callback(msg.content, lodash.pick(msg, ['fields', 'properties']), function done(err, result, headers) {
          if (hasFeedback.call(self, _ctx)) {
            debugx.enabled && debugx('consume() - Feedback queue is available');
            var properties;
            if (err) {
              properties = lodash.defaultsDeep({headers: {error: true, status: 'failed'}}, {headers: headers}, msg.properties);
              debugx.enabled && debugx('consume() - processing failed: %s', stringify(err));
            } else {
              properties = lodash.defaultsDeep({headers: {error: false, status: 'completed'}}, {headers: headers}, msg.properties);
              debugx.enabled && debugx('consume() - processing successful');
            }
            var stqDone = enqueueFeedback.call(self, stringify(result || err || {}), properties, _ctx);
            debugx.enabled && debugx('consume() - enqueue Feedback send message back');
            if (self.config.waitSendToQueueDone === false) {
              ch.ack(msg);
            } else {
              stqDone.finally(function() { ch.ack(msg); });
            }
          } else {
            debugx.enabled && debugx('consume() - Feedback queue not found');
            if (vc.config.noAck !== false) return;
            if (hasRecycler.call(self)) {
              debugx.enabled && debugx('consume() - Recycler queue is available');
              var strDone;
              if (err) {
                var rcfg = self.config.recycler;
                var props = lodash.clone(msg.properties);
                props.headers = props.headers || {};
                props.headers[rcfg.redeliveredCountName] = (props.headers[rcfg.redeliveredCountName] || 0) + 1;
                if (props.headers[rcfg.redeliveredCountName] <= rcfg.redeliveredLimit) {
                  debugx.enabled && debugx('consume() - enqueueConsumer message');
                  strDone = enqueueConsumer.call(self, msg.content, props);
                } else {
                  debugx.enabled && debugx('consume() - enqueueRecycler message');
                  strDone = enqueueRecycler.call(self, msg.content, props);
                }
              }
              if (self.config.waitSendToQueueDone === false || !strDone) {
                ch.ack(msg);
              } else {
                strDone.finally(function() { ch.ack(msg); });
              }
            } else {
              debugx.enabled && debugx('consume() - Recycler queue not found');
              err ? ch.nack(msg) :ch.ack(msg);
            }
          }
          vc.store.count--;
        }, {
          progress: function(completed, total, data) {
            if (hasFeedback.call(self, _ctx)) {
              var percent = -1;
              if (lodash.isNumber(total) && total > 0 &&
                  lodash.isNumber(completed) && completed >= 0 &&
                  completed <= total) {
                percent = (total === 100) ? completed : lodash.round((completed * 100) / total);
              }
              var result = { percent: percent, data: data };
              var properties = lodash.defaultsDeep({headers: {error: false, status: 'progress'}}, msg.properties);
              enqueueFeedback.call(self, stringify(result), properties, _ctx);
            }
          }
        });
      }, {noAck: vc.config.noAck});
    });

    return ok.then(function(result) {
      debugx.enabled && debugx('consume() - consumerTag: %s. CTRL+C to exit', result.consumerTag);
      vc.store.consumerTag = result.consumerTag;
      return result;
    });
  });
}

var getFeedbackConfig = function(ctx) {
  if (ctx && ctx.queueName) {
    this.feedbackExtra = this.feedbackExtra || {};
    this.feedbackExtra[ctx.queueName] = this.feedbackExtra[ctx.queueName] || {};
    this.feedbackExtra[ctx.queueName].config = this.feedbackExtra[ctx.queueName].config || {
      uri: config.consumer.uri,
      queueName: ctx.queueName
    }
    return this.feedbackExtra[ctx.queueName].config;
  }
  return (this.config.feedback);
}

var getFeedbackState = function(ctx) {
  if (ctx && ctx.queueName) {
    this.feedbackExtra = this.feedbackExtra || {};
    this.feedbackExtra[ctx.queueName] = this.feedbackExtra[ctx.queueName] || {};
    this.feedbackExtra[ctx.queueName].state = this.feedbackExtra[ctx.queueName].state || {}
    return this.feedbackExtra[ctx.queueName].state;
  }
  return (this.feedbackState = this.feedbackState || {});
}

var hasFeedback = function(ctx) {
  if (ctx && ctx.queueName) return true;
  return this.config.feedback && (this.config.feedback.enabled !== false);
}

var assertFeedback = function(ctx) {
  if (!hasFeedback.call(this, ctx)) return Promise.reject({
    message: 'The feedback is unavailable'
  });
  return Promise.resolve({ config: getFeedbackConfig.call(this, ctx), store: getFeedbackState.call(this, ctx) });
}

var enqueueFeedback = function(data, opts, ctx) {
  return assertFeedback.call(this, ctx).then(function(vr) {
    vr.name = 'enqueueFeedback';
    return sendToQueue.call(vr, data, opts);
  });
}

Handler.prototype.checkFeedback = function() {
  return assertFeedback.call(this).then(function(vr) {
    return getChannel.call(vr).then(function(ch) {
      return checkQueue.call({ config: vr.config }, ch);
    });
  });
}

Handler.prototype.purgeFeedback = function() {
  return assertFeedback.call(this).then(function(vr) {
    return getChannel.call(vr).then(function(ch) {
      return assertQueue.call({ config: vr.config }, ch).then(function() {
        return purgeQueue.call({ config: vr.config }, ch);
      });
    });
  });
}

Handler.prototype.pullout = function(callback) {
  var self = this;

  assert.ok(lodash.isFunction(callback), 'callback should be a function');

  return assertFeedback.call(self).then(function(vr) {
    vr.store.count = vr.store.count || 0;

    debugx.enabled && debugx('pullout() - get an object from Feedback');
    return getChannel.call(vr).then(function(ch) {

      var ok = assertSubscriber.call({ config: vr.config }, ch).then(function(qok) {
        debugx.enabled && debugx('pullout() - queue info: %s', JSON.stringify(qok));
        return Promise.promisify(ch.consume, {context: ch})(qok.queue, function(msg) {
          vr.store.count++;
          debugx.enabled && debugx('pullout() - received message: %s, fields: %s, amount: %s', 
            msg.content, JSON.stringify(msg.fields), vr.store.count);
          callback(msg.content, lodash.pick(msg, ['fields', 'properties']), function done(err, result) {
            if (vr.config.noAck !== false) return;
            if (hasRecycler.call(self)) {
              debugx.enabled && debugx('pullout() - Recycler queue is available');
              var strDone;
              if (err) {
                var rcfg = self.config.recycler;
                var headers = msg.properties && msg.properties.headers || {};
                headers[rcfg.redeliveredCountName] = (headers[rcfg.redeliveredCountName] || 0) + 1;
                if (headers[rcfg.redeliveredCountName] <= rcfg.redeliveredLimit) {
                  debugx.enabled && debugx('pullout() - enqueueFeedback message');
                  strDone = enqueueFeedback.call(self, msg.content, {headers: headers});
                } else {
                  debugx.enabled && debugx('pullout() - enqueueRecycler message');
                  strDone = enqueueRecycler.call(self, msg.content, {headers: headers});
                }
              }
              if (self.config.waitSendToQueueDone === false || !strDone) {
                ch.ack(msg);
              } else {
                strDone.finally(function() { ch.ack(msg); });
              }
            } else {
              debugx.enabled && debugx('pullout() - Recycler queue not found');
              err ? ch.nack(msg) :ch.ack(msg);
            }
            vr.store.count--;
          });
        }, {noAck: vr.config.noAck});
      });

      return ok.then(function(result) {
        debugx.enabled && debugx('pullout() - consumerTag: %s. CTRL+C to exit', result.consumerTag);
        vr.store.consumerTag = result.consumerTag;
        return result;
      });
    });
  });
}

var getRecyclerState = function() {
  return (this.recyclerState = this.recyclerState || {});
}

var hasRecycler = function() {
  return this.config.recycler && (this.config.recycler.enabled !== false);
}

var assertRecycler = function() {
  if (!hasRecycler.call(this)) return Promise.reject({
    message: 'The recycler is unavailable'
  });
  return Promise.resolve({ config: this.config.recycler, store: getRecyclerState.call(this) });
}

var enqueueRecycler = function(data, opts) {
  return assertRecycler.call(this).then(function(vr) {
    vr.name = 'enqueueRecycler';
    return sendToQueue.call(vr, data, opts);
  });
}

Handler.prototype.checkTrash = function() {
  return assertRecycler.call(this).then(function(vr) {
    return getChannel.call(vr).then(function(ch) {
      return checkQueue.call({ config: vr.config }, ch);
    });
  });
}

Handler.prototype.purgeTrash = function() {
  return assertRecycler.call(this).then(function(vr) {
    return getChannel.call(vr).then(function(ch) {
      return assertQueue.call({ config: vr.config }, ch).then(function() {
        return purgeQueue.call({ config: vr.config }, ch);
      });
    });
  });
}

Handler.prototype.recycle = function(callback) {
  var self = this;

  assert.ok(lodash.isFunction(callback), 'callback should be a function');

  return assertRecycler.call(self).then(function(vr) {
    vr.store.count = vr.store.count || 0;

    debugx.enabled && debugx('recycle() - get an object from Trash');
    return getChannel.call(vr).then(function(ch) {

      var ok = assertSubscriber.call({ config: vr.config }, ch).then(function(qok) {
        debugx.enabled && debugx('recycle() - queue info: %s', JSON.stringify(qok));
        return Promise.promisify(ch.consume, {context: ch})(qok.queue, function(msg) {
          vr.store.count++;
          debugx.enabled && debugx('recycle() - received message: %s, fields: %s, amount: %s', 
            msg.content, JSON.stringify(msg.fields), vr.store.count);
          callback(msg.content, lodash.pick(msg, ['fields', 'properties']), function done(err) {
            if (vr.config.noAck === false) {
              err ? ch.nack(msg) :ch.ack(msg);
            }
            vr.store.count--;
          });
        }, {noAck: vr.config.noAck});
      });

      return ok.then(function(result) {
        debugx.enabled && debugx('recycle() - consumerTag: %s. CTRL+C to exit', result.consumerTag);
        vr.store.consumerTag = result.consumerTag;
        return result;
      });
    });
  });
}

Handler.prototype.purge = function() {
  var self = this;
  return Promise.resolve().then(function() {
    return self.purgeChain();
  }).then(function() {
    if (!hasFeedback.call(self)) return Promise.resolve();
    return self.purgeFeedback();
  }).then(function() {
    if (!hasRecycler.call(self)) return Promise.resolve();
    return self.purgeTrash();
  });
}

var garbageAction = {};

var discardGarbage = garbageAction['discard'] = function() {
  var self = this;
  return assertRecycler.call(self).then(function(vr) {
    if (!vr.store.garbage) return false;
    return getChannel.call(vr).then(function(ch) {
      debugx.enabled && debugx('discardGarbage() - nack()');
      ch.nack(vr.store.garbage, false, false);
      vr.store.garbage = undefined;
      return true;
    });
  });
}

var restoreGarbage = garbageAction['restore'] = function() {
  var self = this;
  return assertRecycler.call(self).then(function(vr) {
    if (!vr.store.garbage) return false;
    return getChannel.call(vr).then(function(ch) {
      debugx.enabled && debugx('restoreGarbage() - nack()');
      ch.nack(vr.store.garbage);
      vr.store.garbage = undefined;
      return true;
    });
  });
}

var recoverGarbage = garbageAction['recover'] = function() {
  var self = this;
  return assertRecycler.call(self).then(function(vr) {
    if (!vr.store.garbage) return false;
    return getChannel.call(vr).then(function(ch) {
      var msg = vr.store.garbage;
      return enqueueConsumer.call(self, msg.content, msg.properties).then(function(result) {
        debugx.enabled && debugx('recoverGarbage() - ack()');
        ch.ack(vr.store.garbage);
        vr.store.garbage = undefined;
        return true;
      });
    });
  });
}

var requeueGarbage = garbageAction['requeue'] = function() {
  var self = this;
  return assertRecycler.call(self).then(function(vr) {
    if (!vr.store.garbage) return false;
    return getChannel.call(vr).then(function(ch) {
      var msg = vr.store.garbage;
      return enqueueRecycler.call(self, msg.content, msg.properties).then(function(result) {
        debugx.enabled && debugx('requeueGarbage() - ack()');
        ch.ack(vr.store.garbage);
        vr.store.garbage = undefined;
        return true;
      });
    });
  });
}

var examineGarbage = function() {
  var self = this;
  return assertRecycler.call(self).then(function(vr) {
    if (vr.store.garbage) return vr.store.garbage;
    return getChannel.call(vr).then(function(ch) {
      return assertQueue.call({ config: vr.config }, ch).then(function(qok) {
        return Promise.promisify(ch.get, {context: ch})(qok.queue, {});
      }).then(function(msgOrFalse) {
        debugx.enabled && debugx('examineGarbage() - msg: %s', JSON.stringify(msgOrFalse));
        if (msgOrFalse !== false) vr.store.garbage = msgOrFalse;
        return vr.store.garbage;
      });
    });
  });
}

Handler.prototype.examine = function(callback) {
  var self = this;
  var result = { obtained: 0 };
  result.callback = lodash.isFunction(callback);
  return examineGarbage.call(self).then(function(msg) {
    if (msg) {
      result.obtained = 1;
      if (!result.callback) return result;
      return new Promise(function(resolved, rejected) {
        var copied = lodash.pick(msg, ['content', 'fields', 'properties']);
        callback(copied, function update(action, data) {
          if (data && lodash.isObject(data)) {
            if(data.content instanceof Buffer) {
              msg.content = data.content;
            } else if (typeof(data.content) === 'string') {
              msg.content = bufferify(data.content);
            } else if (typeof(data.content) === 'object') {
              msg.content = bufferify(data.content);
            }
            if (data.properties && lodash.isObject(data.properties)) {
              lodash.merge(msg.properties, data.properties);
            }
          }
          if (lodash.isFunction(garbageAction[action])) {
            result.nextAction = true;
            garbageAction[action].call(self).then(function() {
              resolved(result);
            }).catch(function(err) {
              result.actionError = err;
              rejected(result);
            });
          } else {
            result.nextAction = false;
            resolved(result);
          }
        });
      });
    } else {
      return result;
    }
  });
}

Handler.prototype.destroy = function() {
  var self = this;
  return Promise.resolve().then(function() {
    return stopPublisher.call({store: getProducerState.call(self) });
  }).then(function() {
    return stopSubscriber.call({store: getConsumerState.call(self) });
  }).then(function() {
    return stopSubscriber.call({store: getRecyclerState.call(self) });
  }).then(function() {
    return stopSubscriber.call({store: getFeedbackState.call(self) });
  }).then(function() {
    return Promise.mapSeries(lodash.values(self.feedbackExtra), function(extra) {
      return stopSubscriber.call({store: extra.state });
    }).then(function() {
      delete self.feedbackExtra;
    });
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
      debugx.enabled && debugx('destroy() has been invoked: %s', self.store.consumerTag);
      var ch_cancel = Promise.promisify(self.store.channel.cancel, {
        context: self.store.channel
      });
      return ch_cancel(self.store.consumerTag).then(function(ok) {
        delete self.store.consumerTag;
        debugx.enabled && debugx('destroy() - consumer is cancelled: %s', JSON.stringify(ok));
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

module.exports = Handler;
