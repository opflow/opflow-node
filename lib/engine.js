'use strict';

var assert = require('assert');
var Promise = require('bluebird');
var amqp = require('amqplib/callback_api');
var events = require('events');
var lodash = require('lodash');
var locks = require('locks');
var Readable = require('stream').Readable;
var misc = require('./util');
var PayloadReader = require('./task').PayloadReader;
var PayloadWriter = require('./task').PayloadWriter;
var TimeoutHandler = require('./task').TimeoutHandler;
var LogTracer = require('logolite').LogTracer;
var LogAdapter = require('logolite').LogAdapter;
var LX = LogAdapter.getLogger({ scope: 'opflow:engine' });

var Engine = function(params) {
  params = params || {};
  var self = this;

  var engineId = params.engineId || misc.getLogID();
  var engineTrail = LogTracer.ROOT.branch({ key: 'engineId', value: engineId });
  LX.isEnabledFor('info') && LX.log('info', engineTrail.toMessage({
    text: 'Engine.new()'
  }));
  var engineState = 'suspended';
  var engineWatcher = new events.EventEmitter();

  var config = {};
  config.uri = params.uri;
  if (lodash.isEmpty(config.uri)) {
    var conargs = misc.buildURI(params);
    config.uri = conargs.uri;
  }
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

  config.confirmation = null;
  if (params.confirmation && typeof(params.confirmation) === 'object') {
    config.confirmation = lodash.pick(params.confirmation, [
      'enabled', 'interval', 'timeout', 'redeliveredLimit'
    ]);
  }

  var consumerRefs = [];

  var probateConfig = config.confirmation || {};
  probateConfig.interval = probateConfig.interval || 1000;
  probateConfig.timeout = probateConfig.timeout || 60000;
  if (probateConfig.enabled !== false && !lodash.isNumber(probateConfig.redeliveredLimit)) {
    probateConfig.redeliveredLimit = 1;
  }

  var probateTasks = {};
  var probateTimeoutHandler = null;
  var createProbateTimeoutHandler = function() {
    var timeoutHandler = new TimeoutHandler({
      monitorId: engineId,
      monitorType: 'confirmation',
      interval: probateConfig.interval,
      timeout: probateConfig.timeout,
      tasks: probateTasks,
      raiseTimeout: function(done) {
        var task = this;
        var props = task.props || {};
        props.headers = props.headers || {};
        props.headers.redeliveredCount = (props.headers.redeliveredCount || 0) + 1;
        var requestId = misc.getRequestId(props.headers);
        var timeoutTrail = engineTrail.branch({ key: 'requestId', value: requestId });
        if (props.headers.redeliveredCount <= probateConfig.redeliveredLimit) {
          self.produce(task.body, task.props, task.override);
          LX.isEnabledFor('info') && LX.log('info', timeoutTrail.add({
            'redeliveredCount': props.headers.redeliveredCount,
            'redeliveredLimit': probateConfig.redeliveredLimit
          }).toMessage({
            text: 'raiseTimeout() resend message'
          }));
        } else {
          LX.isEnabledFor('error') && LX.log('error', timeoutTrail.add({
            'task': lodash.omit(task, ['body']),
            'redeliveredCount': props.headers.redeliveredCount,
            'redeliveredLimit': probateConfig.redeliveredLimit
          }).toMessage({
            text: 'raiseTimeout() discard message'
          }));
        }
        done && done();
      },
      handlerType: 'callback'
    });
    timeoutHandler.start();
    return timeoutHandler;
  }

  var payloadConfig = config.payload || {};
  payloadConfig.interval = payloadConfig.interval || 1000;
  payloadConfig.timeout = payloadConfig.timeout || 60000;

  var payloadTasks = {};
  var payloadTimeoutHandler = null;
  var createPayloadTimeoutHandler = function() {
    var timeoutHandler = new TimeoutHandler({
      monitorId: engineId,
      monitorType: 'payload',
      interval: payloadConfig.interval,
      timeout: payloadConfig.timeout,
      tasks: payloadTasks,
      raiseTimeout: function(done) {
        var task = this;
        task.raiseError({ message: "segment has been timeout" });
        done && done();
      },
      handlerType: 'callback'
    });
    timeoutHandler.start();
    return timeoutHandler;
  }

  LX.isEnabledFor('debug') && LX.log('debug', engineTrail.add({
    config: config
  }).toMessage({
    text: 'configuration object'
  }));

  this.ready = function() {
    var readyTrail = engineTrail.copy();
    LX.isEnabledFor('info') && LX.log('info', readyTrail.toMessage({
      text: 'ready() is invoked'
    }));
    var readyPromise = null;
    if (config.exchangeName) {
      readyPromise = self.obtainSandbox(assertExchange);
    } else {
      readyPromise = Promise.resolve();
    }
    return readyPromise.then(function(result) {
      engineState = 'running';
      LX.isEnabledFor('info') && LX.log('info', readyTrail.toMessage({
        text: 'ready() has been done'
      }));
      return result;
    });
  }

  this.produce = function(bodyOrBuffer, props, override) {
    override = override || {};
    var routingKey = override.routingKey || config.routingKey;
    var hasPayload = override.payloadStream instanceof Readable;
    props = props || {};
    props.appId = props.appId || config.applicationId;
    props.headers = props.headers || {};
    var requestId = props.headers.requestId = props.headers.requestId || misc.getLogID();
    var requestTrail = engineTrail.branch({ key: 'requestId', value: requestId });
    var ok = obtainProducerSandbox().then(function(sandbox) {
      LX.isEnabledFor('info') && LX.log('info', requestTrail.add({
        producerId: sandbox.id
      }).toMessage({
        text: 'produce() a message to exchange/queue'
      }));
      if (sandbox.confirmable) {
        probateTimeoutHandler = probateTimeoutHandler || createProbateTimeoutHandler();
      }
      var probateId = misc.getLogID();
      var probateTrail = requestTrail.branch({ key: 'probateId', value: probateId });
      var sendTo = function(body, index, options) {
        options = options || {};
        var segmentId = (index === null) ? probateId : misc.getLogID();
        var segmentTrail = (index === null) ? probateTrail : requestTrail.branch({ key: 'segmentId', value: segmentId });
        if (index !== null) {
          props = lodash.cloneDeep(props);
          props.headers['payloadId'] = probateId;
          props.headers['segmentId'] = segmentId;
          props.headers['segmentIndex'] = index;
        }
        if (options.total !== undefined) {
          props.headers['segmentTotal'] = options.total;
        }
        if (sandbox.confirmable) {
          probateTimeoutHandler.add(segmentId, { body: body, props: props, override: override });
          LX.isEnabledFor('debug') && LX.log('debug', segmentTrail.toMessage({
            tags: ['engine.produce', 'add new object to confirmation list'],
            text: 'produce() add new object to confirmation list'
          }));
        }
        return sandbox.channel.publish(config.exchangeName, routingKey, 
            misc.bufferify(body), props, function(err, ok) {
          if (err) {
            LX.isEnabledFor('debug') && LX.log('debug', segmentTrail.toMessage({
              tags: ['engine.produce', 'confirmation has failed'],
              text: 'produce() confirmation has failed'
            }));
          } else {
            probateTimeoutHandler && probateTimeoutHandler.remove(segmentId);
            LX.isEnabledFor('debug') && LX.log('debug', segmentTrail.toMessage({
              tags: ['engine.produce', 'confirmation has completed'],
              text: 'produce() confirmation has completed'
            }));
          }
        });
      }
      var doSend = function(body, index, options) {
        var waiting = true;
        if (sandbox.sendable !== false) {
          sandbox.sendable = sendTo(body, index, options);
          waiting = false;
          LX.isEnabledFor('conlog') && LX.log('conlog', probateTrail.add({
            producerId: sandbox.id
          }).toMessage({
            tags: ['engine.produce', 'channel is writable'],
            text: 'produce() channel is writable, msg has been sent'
          }));
          return Promise.resolve(sandbox.sendable);
        } else {
          LX.isEnabledFor('info') && LX.log('info', probateTrail.add({
            producerId: sandbox.id
          }).toMessage({
            tags: ['engine.produce', 'channel is overflowed'],
            text: 'produce() channel is overflowed, waiting'
          }));
          return new Promise(function(onResolved, onRejected) {
            sandbox.channel.once('drain', function() {
              sandbox.sendable = sendTo(body, index, options);
              waiting = false;
              LX.isEnabledFor('info') && LX.log('info', probateTrail.add({
                producerId: sandbox.id
              }).toMessage({
                tags: ['engine.produce', 'channel is drained'],
                text: 'produce() channel is drained, flushed'
              }));
              onResolved(sandbox.sendable);
            });
            sandbox.channel.once('close', function() {
              if (!waiting) return;
              onRejected({ message: 'produce() channel is closed, msg has not been sent' });
            });
          }).timeout(probateConfig.timeout);
        }
      }
      if (hasPayload) return doSend(bodyOrBuffer, -3).then(function() {
        var payloadWriter = new PayloadWriter(function next(body, index) {
          return doSend(body, index);
        }, { payloadId: probateId });

        engineWatcher.once('close', function(opts) {
          payloadWriter && payloadWriter.forceClose(opts);
        })

        return new Promise(function(onResolved, onRejected) {
          LX.isEnabledFor('debug') && LX.log('debug', requestTrail.toMessage({
            text: 'produce() streaming begin'
          }));
          payloadWriter.on('finish', function() {
            var chunkCount = payloadWriter.count();
            LX.isEnabledFor('debug') && LX.log('debug', requestTrail.add({
              total: chunkCount
            }).toMessage({
              text: 'produce() streaming end'
            }));
            payloadWriter = null;
            doSend(null, -1, { total: chunkCount }).then(function() {
              onResolved();
            });
          });
          payloadWriter.on('error', function(err) {
            LX.isEnabledFor('error') && LX.log('error', requestTrail.add({
              errorMessage: err.message
            }).toMessage({
              text: 'produce() streaming error'
            }));
            payloadWriter = null;
            doSend(null, -2).then(function() {
              onRejected(err);
            });
          });
          override.payloadStream.pipe(payloadWriter, { end: true });
        });
      });
      return doSend(bodyOrBuffer);
    });
    if (config.delayTime > 0) {
      ok = ok.delay(config.delayTime);
    }
    return ok;
  }

  this.consume = function(callback, options) {
    assert.ok(lodash.isFunction(callback), 'callback should be a function');
    options = options || {};

    return obtainConsumerSandbox(options).then(function(sandbox) {
      var consumeTrail = engineTrail.branch({ key: 'consumerId', value: sandbox.id });

      sandbox.replyToName = options.replyToName || options.replyTo;

      LX.isEnabledFor('conlog') && LX.log('conlog', consumeTrail.toMessage({
        text: 'consume() - create a consumer'
      }));

      var ch = sandbox.channel;
      var ok = assertSubscriber(sandbox, options);

      ok = ok.then(function(qok) {
        if (options.binding === false) {
          LX.isEnabledFor('debug') && LX.log('debug', consumeTrail.add({
            queueName: qok.queue
          }).toMessage({
            text: 'consume() - queue is keep standalone'
          }));
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
            queueName: qok.queue
          }).toMessage({
            text: 'consume() - queue has been bound'
          }));
          return qok;
        });
      });

      ok = ok.then(function(qok) {
        LX.isEnabledFor('debug') && LX.log('debug', consumeTrail.add({
          queueInfo: qok
        }).toMessage({
          text: 'consume() - queue ready to consume'
        }));

        var ch_consume = Promise.promisify(ch.consume, {context: ch});
        return ch_consume(qok.queue, function(msg) {
          var props = msg && msg.properties || {};
          var requestId = misc.getRequestId(props.headers);

          LX.isEnabledFor('info') && LX.log('info', consumeTrail.add({
            requestId: requestId,
            appId: props.appId,
            bodySize: Buffer.byteLength(msg.content)
          }).toMessage({
            text: 'consume() receive new message'
          }));

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
              requestId: requestId,
              appId: msg.properties.appId,
              applicationId: config.applicationId
            }).toMessage({
              text: 'Mismatched applicationId'
            }));
            return;
          }

          var extension = sandbox;
          var payloadRoot = false;
          var payloadId = misc.getHeaderField(props.headers, 'payloadId');
          var segmentId = misc.getHeaderField(props.headers, 'segmentId');
          var segmentIndex = misc.getHeaderField(props.headers, 'segmentIndex');
          var segmentTotal = misc.getHeaderField(props.headers, 'segmentTotal');
          LX.isEnabledFor('info') && LX.log('info', consumeTrail.add({
              payloadId: payloadId,
              segmentId: segmentId,
              segmentIndex: segmentIndex,
              segmentTotal: segmentTotal
            }).toMessage({
              text: 'Payload information'
            }));
          if (options.payloadEnabled !== false && payloadId && segmentId && (segmentIndex >= -3)) {
            payloadTimeoutHandler = payloadTimeoutHandler || createPayloadTimeoutHandler();
            var payloadReader = payloadTimeoutHandler.touch(payloadId);
            if (!payloadReader) {
              payloadReader = payloadTimeoutHandler.add(payloadId, new PayloadReader({
                payloadId: payloadId
              })).get(payloadId);
            }
            if (segmentIndex >= 0) {
              payloadReader.addChunk(segmentIndex, msg.content, function() {
                forceAck();
              });
            } else {
              if (segmentIndex === -3) payloadRoot = true;
              if (segmentIndex === -1) payloadReader.raiseFinal(segmentTotal);
              if (segmentIndex === -2) payloadReader.raiseError({
                error: true,
                message: 'Error from Producer'
              });
            }
            if (payloadRoot) {
              extension = lodash.clone(sandbox);
              extension.payloadStream = payloadReader;
            } else {
              return;
            }
          }

          try {
            callback(msg, extension, function(err, result) {
              LX.isEnabledFor('info') && LX.log('info', consumeTrail.add({
                requestId: requestId
              }).toMessage({
                text: 'Processing finish'
              }));
              if (payloadId && options.payloadEnabled !== false) {
                payloadTimeoutHandler && payloadTimeoutHandler.remove(payloadId);
              }
              forceAck(err);
            });
          } catch (exception) {
            misc.isConsoleLogEnabled() && console.log('consume() - exception: ', exception);
            LX.isEnabledFor('info') && LX.log('info', consumeTrail.add({
              requestId: requestId
            }).toMessage({
              text: 'Unmanaged exception'
            }));
            forceAck(exception);
          }
        }, {noAck: options.noAck});
      });

      return ok.then(function(result) {
        LX.isEnabledFor('info') && LX.log('info', consumeTrail.add({
          consumerTag: result.consumerTag
        }).toMessage({
          text: 'consume() is running. CTRL+C to exit'
        }));
        sandbox.consumerTag = result.consumerTag;
        if (!config.mode || config.mode == 'engine') consumerRefs.push(sandbox);
        return sandbox;
      });
    });
  }

  this.cancelConsumer = function(sandbox) {
    sandbox = sandbox || {};
    var cancelTrail = engineTrail.branch({key:'consumerId', value:sandbox.id});
    return Promise.resolve().then(function() {
      if (sandbox.consumerTag && sandbox.channel) {
        LX.isEnabledFor('conlog') && LX.log('conlog', cancelTrail.add({
          consumerTag: sandbox.consumerTag
        }).toMessage({
          text: 'cancelConsumer() subscriber has been invoked'
        }));
        var ch_cancel = Promise.promisify(sandbox.channel.cancel, {
          context: sandbox.channel
        });
        return ch_cancel(sandbox.consumerTag).then(function(ok) {
          delete sandbox.consumerTag;
          LX.isEnabledFor('conlog') && LX.log('conlog', cancelTrail.add({
            consumerTag: sandbox.consumerTag
          }).toMessage({
            text: 'cancelConsumer() - subscriber is cancelled'
          }));
          return true;
        }).delay(100);
      }
      return true;
    }).then(function() {
      var sharedSandbox = getConsumerSandbox();
      return Promise.resolve().then(function() {
        if (sandbox.forceNewChannel || 
            sandbox.forceNewConnection || 
            sandbox.channel !== sharedSandbox.channel) 
          return closeChannel(sandbox);
        LX.isEnabledFor('conlog') && LX.log('conlog', cancelTrail.toMessage({
          text: 'cancelConsumer() - skip close channel'
        }));
        return true;
      }).then(function() {
        if (sandbox.forceNewConnection || 
            sandbox.connection !== sharedSandbox.connection)
          return closeConnection(sandbox);
        LX.isEnabledFor('conlog') && LX.log('conlog', cancelTrail.toMessage({
          text: 'cancelConsumer() - skip close connection'
        }));
        return true;
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

  this.obtainSandbox = function(callback, sandbox) {
    if (!lodash.isFunction(callback)) return Promise.resolve();
    var selfManaged = !lodash.isObject(sandbox);
    sandbox = sandbox || {};
    var p = self.openSession(sandbox).then(lodash.wrap(sandbox, callback));
    if (selfManaged) p = p.then(function(ok) {
      return self.closeSession(sandbox).then(lodash.wrap(ok));
    }).catch(function(err) {
      return self.closeSession(sandbox).then(lodash.wrap(Promise.reject(err)));
    });
    return p;
  }

  this.close = function() {
    var closeTrail = engineTrail.copy();
    engineState = 'suspended';
    engineWatcher.emit('close', {});
    return Promise.resolve().then(function() {
      LX.isEnabledFor('info') && LX.log('info', closeTrail.toMessage({
        text: 'close() - stop probateTimeoutHandler'
      }));
      if (probateTimeoutHandler === null) return true;
      return probateTimeoutHandler.stop();
    }).then(function() {
      LX.isEnabledFor('info') && LX.log('info', closeTrail.toMessage({
        text: 'close() - stop payloadTimeoutHandler'
      }));
      if (payloadTimeoutHandler === null) return true;
      return payloadTimeoutHandler.stop();
    }).then(function() {
      LX.isEnabledFor('info') && LX.log('info', closeTrail.toMessage({
        text: 'close() - cancel producer'
      }));
      return cancelProducer(getProducerSandbox());
    }).then(function() {
      if (config.mode && config.mode != 'engine') return true;
      LX.isEnabledFor('info') && LX.log('info', closeTrail.toMessage({
        text: 'close() - cancel consumers'
      }));
      return Promise.mapSeries(consumerRefs, function(consumerRef) {
        return self.cancelConsumer(consumerRef);
      });
    }).then(function() {
      LX.isEnabledFor('info') && LX.log('info', closeTrail.toMessage({
        text: 'close() - close default consumer channel'
      }));
      return closeChannel(getConsumerSandbox());
    }).then(function() {
      LX.isEnabledFor('info') && LX.log('info', closeTrail.toMessage({
        text: 'close() - close default consumer connection'
      }));
      return closeConnection(getConsumerSandbox());
    }).catch(function(err) {
      LX.isEnabledFor('info') && LX.log('info', closeTrail.toMessage({
        text: 'close() - catched error, reject it'
      }));
      misc.isConsoleLogEnabled() && console.log('Exception: %s', err);
      return Promise.reject(err);
    });
  }

  var getConnection = function(sandbox) {
    sandbox = sandbox || {};
    var connectTrail = (sandbox.id) ? engineTrail.branch({
      key: 'sandboxId', value:sandbox.id
    }) : engineTrail.copy();
    LX.isEnabledFor('conlog') && LX.log('conlog', connectTrail.toMessage({
      text: 'getConnection() - get a connection'
    }));
    if (config.connectIsCached !== false && sandbox.connection) {
      LX.isEnabledFor('conlog') && LX.log('conlog', connectTrail.toMessage({
        text: 'getConnection() - connection has been available'
      }));
      return Promise.resolve(sandbox.connection);
    } else {
      LX.isEnabledFor('conlog') && LX.log('conlog', connectTrail.toMessage({
        text: 'getConnection() - make a new connection'
      }));
      var amqp_connect = Promise.promisify(amqp.connect, {context: amqp});
      return amqp_connect(config.uri, {}).then(function(conn) {
        conn.on('close', function() {
          sandbox.connection = null;
          sandbox.channel = null;
          LX.isEnabledFor('conlog') && LX.log('conlog', connectTrail.toMessage({
            text: 'connection has been closed'
          }));
        });
        conn.on('error', function(error) {
          sandbox.connection = null;
          sandbox.channel = null;
          LX.isEnabledFor('conlog') && LX.log('conlog', connectTrail.toMessage({
            text: 'connection has been failed'
          }));
        })
        LX.isEnabledFor('conlog') && LX.log('conlog', connectTrail.toMessage({
          tags: ['engine.getConnection', 'connection_is_created'],
          text: 'getConnection() - connection is created'
        }));
        return (sandbox.connection = conn);
      }).catch(function(err) {
        LX.isEnabledFor('error') && LX.log('error', connectTrail.add({
          errorCode: err.code,
          errorMessage: err.message
        }).toMessage({
          text: 'getConnection() - throw an error object'
        }));
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
    LX.isEnabledFor('conlog') && LX.log('conlog', connectTrail.toMessage({
      text: 'closeConnection() - closing'
    }));
    if (sandbox.connection) {
      var connection_close = Promise.promisify(sandbox.connection.close, {
        context: sandbox.connection
      });
      return connection_close().then(function() {
        LX.isEnabledFor('conlog') && LX.log('conlog', connectTrail.toMessage({
          tags: ['engine.closeConnection', 'connection_is_closed'],
          text: 'closeConnection() - connection is closed'
        }));
        return true;
      }).catch(function(err) {
        LX.isEnabledFor('conlog') && LX.log('conlog', connectTrail.toMessage({
          text: 'closeConnection() - failed'
        }));
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
      LX.isEnabledFor('conlog') && LX.log('conlog', channelTrail.toMessage({
        text: 'getChannel() - channel has been available'
      }));
      return Promise.resolve(sandbox.channel);
    } else {
      LX.isEnabledFor('conlog') && LX.log('conlog', channelTrail.toMessage({
        tags: ['engine.getChannel', 'make_a_new_channel'],
        text: 'getChannel() - make a new channel'
      }));
      return getConnection(sandbox).then(function(conn) {
        LX.isEnabledFor('conlog') && LX.log('conlog', channelTrail.toMessage({
          text: 'getChannel() - connection is available'
        }));
        var createChannel = Promise.promisify(conn.createChannel, {context: conn});
        if (sandbox.confirmable) {
          LX.isEnabledFor('conlog') && LX.log('conlog', channelTrail.toMessage({
            tags: ['engine.getChannel', 'create_confirm_channel'],
            text: 'getChannel() - createConfirmChannel'
          }));
          createChannel = Promise.promisify(conn.createConfirmChannel, {context: conn});
        }
        return createChannel().then(function(ch) {
          ch.on('close', function() {
            sandbox.channel = null;
            LX.isEnabledFor('conlog') && LX.log('conlog', channelTrail.toMessage({
              text: 'channel has been closed'
            }));
          });
          ch.on('error', function() {
            sandbox.channel = null;
            LX.isEnabledFor('conlog') && LX.log('conlog', channelTrail.toMessage({
              text: 'channel has been failed'
            }));
          });
          LX.isEnabledFor('conlog') && LX.log('conlog', channelTrail.toMessage({
            text: 'getChannel() - channel is created'
          }));
          return (sandbox.channel = ch);
        }).catch(function(err) {
          LX.isEnabledFor('error') && LX.log('error', channelTrail.add({
            errorCode: err.code,
            errorMessage: err.message
          }).toMessage({
            text: 'getChannel() - failed, code: {errorCode}, message: {errorMessage}'
          }));
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
    LX.isEnabledFor('conlog') && LX.log('conlog', channelTrail.toMessage({
      text: 'closeChannel() - closing'
    }));
    if (sandbox.channel) {
      var ch_close = Promise.promisify(sandbox.channel.close, {
        context: sandbox.channel
      });
      return ch_close().then(function() {
        LX.isEnabledFor('conlog') && LX.log('conlog', channelTrail.toMessage({
          tags: ['engine.closeChannel', 'channel_is_closed'],
          text: 'closeChannel() - channel is closed'
        }));
        return true;
      }).catch(function(err) {
        LX.isEnabledFor('conlog') && LX.log('conlog', channelTrail.toMessage({
          text: 'closeChannel() - failed'
        }));
        return false;
      }).finally(function() {
        delete sandbox.channel;
      });
    }
    return Promise.resolve(true);
  }

  var initSandbox = function(sandbox) {
    if (!sandbox || typeof(sandbox) !== 'object') return Promise.resolve(sandbox);
    return getChannel(sandbox).then(lodash.wrap(sandbox));
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
        prefetch: options.prefetch
      }).toMessage({
        text: 'assertSubscriber() - set channel prefetch: {prefetch}'
      }));
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
          error: error
        }).toMessage({
          text: 'assertSubscriber() - Error'
        }));
        return Promise.reject(error);
      } else {
        LX.isEnabledFor('conlog') && LX.log('conlog', assertTrail.add({
          queue: qok
        }).toMessage({
          text: 'assertSubscriber() - Queue'
        }));
        return qok;
      }
    });
  }

  var getProducerSandbox = function() {
    self.producerSandbox = self.producerSandbox || {};
    if (!self.producerSandbox.id) {
      self.producerSandbox.id = misc.getLogID();
      LX.isEnabledFor('info') && LX.log('info', engineTrail.add({
        producerId: self.producerSandbox.id
      }).toMessage({
        tags: ['engine.getProducerSandbox', 'create_producer_sandbox'],
        text: 'getProducerSandbox() - create producer sandbox'
      }));
    }
    self.producerSandbox.name = self.producerSandbox.name || 'producerSandbox';
    self.producerSandbox.mutex = self.producerSandbox.mutex || locks.createMutex();
    if (config.exchangeQuota > 0 && !self.producerSandbox.fence) {
      self.producerSandbox.fence = self.producerSandbox.fence || locks.createSemaphore(config.exchangeQuota);
    }
    self.producerSandbox.confirmable = config.confirmation && config.confirmation.enabled !== false;
    return (self.producerSandbox);
  }

  var obtainProducerSandbox = function() {
    if (engineState !== 'running') {
      return Promise.reject({
        engineState: engineState,
        message: 'Engine is not ready yet'
      });
    }
    var producerSandbox = getProducerSandbox();
    var producerPromise = null;
    if (!producerSandbox.channel || !producerSandbox.connection) {
      producerPromise = new Promise(function(onResolved) {
        producerSandbox.mutex.lock(function() {
          LX.isEnabledFor('info') && LX.log('info', engineTrail.toMessage({
            tags: ['engine.obtainProducerSandbox', 'obtain_mutex'],
            text: 'lockProducer() - obtain mutex'
          }));
          onResolved();
        });
      }).then(function() {
        return getChannel(producerSandbox).then(function(ch) {
          return assertExchange(producerSandbox);
        }).then(function(eok) {
          return producerSandbox;
        });
      }).finally(function() {
        LX.isEnabledFor('info') && LX.log('info', engineTrail.toMessage({
          tags: ['engine.obtainProducerSandbox', 'release_mutex'],
          text: 'lockProducer() - release mutex'
        }));
        producerSandbox.mutex.unlock();
      });
    } else {
      producerPromise = lockProducer().finally(function() {
        unlockProducer();
      });
    }
    return producerPromise;
  }

  var lockProducer = function(producerSandbox) {
    producerSandbox = producerSandbox || getProducerSandbox();
    return new Promise(function(onResolved, onRejected) {
      if (producerSandbox.fence) {
        producerSandbox.fence.wait(function whenResourceAvailable() {
          LX.isEnabledFor('conlog') && LX.log('conlog', engineTrail.toMessage({
            tags: ['engine.lockProducer', 'obtain_semaphore'],
            text: 'lockProducer() - obtain semaphore'
          }));
          onResolved(producerSandbox);
        });
      } else {
        onResolved(producerSandbox);
        LX.isEnabledFor('conlog') && LX.log('conlog', engineTrail.toMessage({
          text: 'lockProducer() - skipped'
        }));
      }
    });
  }

  var unlockProducer = function(producerSandbox) {
    producerSandbox = producerSandbox || getProducerSandbox();
    if (producerSandbox.fence) {
      LX.isEnabledFor('conlog') && LX.log('conlog', engineTrail.toMessage({
        tags: ['engine.unlockProducer', 'release_semaphore'],
        text: 'unlockProducer() - release semaphore'
      }));
      producerSandbox.fence.signal();
    } else {
      LX.isEnabledFor('conlog') && LX.log('conlog', engineTrail.toMessage({
        text: 'unlockProducer() - skipped'
      }));
    }
  }

  var cancelProducer = function(sandbox) {
    return Promise.resolve().then(function() {
      return closeChannel(sandbox);
    }).then(function(result) {
      return closeConnection(sandbox);
    });
  }

  var getConsumerSandbox = function() {
    self.consumerSandbox = self.consumerSandbox || {};
    if (!self.consumerSandbox.id) {
      self.consumerSandbox.id = misc.getLogID();
      LX.isEnabledFor('conlog') && LX.log('conlog', engineTrail.add({
        consumerId: self.consumerSandbox.id
      }).toMessage({
        tags: ['engine.getConsumerSandbox', 'create_consumer_sandbox'],
        text: 'getConsumerSandbox() - create consumer sandbox'
      }));
    }
    self.consumerSandbox.name = self.consumerSandbox.name || 'consumerSandbox';
    self.consumerSandbox.mutex = self.consumerSandbox.mutex || locks.createMutex();

    return self.consumerSandbox;
  }

  var obtainConsumerSandbox = function(opts) {
    if (engineState !== 'running') {
      return Promise.reject({
        engineState: engineState,
        message: 'Engine is not ready yet'
      });
    }
    var consumerSandbox = getConsumerSandbox();
    var consumeTrail = engineTrail.copy();
    return lockConsumer(consumerSandbox).then(function() {
      if (!opts) {
        LX.isEnabledFor('info') && LX.log('info', consumeTrail.add({
          sandboxId: consumerSandbox.id
        }).toMessage({
          text: 'getConsumerSandbox() - return origin sandbox'
        }));
        return initSandbox(consumerSandbox);
      }
      if (opts.forceNewConnection) {
        var sandboxId1 = misc.getLogID();
        LX.isEnabledFor('info') && LX.log('info', consumeTrail.add({
          sandboxId: sandboxId1
        }).toMessage({
          text: 'getConsumerSandbox() - forced Connection and forced Channel'
        }));
        return initSandbox({
          id: sandboxId1,
          name: getSandboxNameOf(consumerSandbox, 1),
          forceNewConnection: true
        });
      }
      if (opts.forceNewChannel) {
        var sandboxId2 = misc.getLogID();
        return getConnection(consumerSandbox).then(function(conn) {
          LX.isEnabledFor('info') && LX.log('info', consumeTrail.add({
            sandboxId: sandboxId2
          }).toMessage({
            text: 'getConsumerSandbox() - shared Connection and forced Channel'
          }));
          return initSandbox({
            id: sandboxId2,
            name: getSandboxNameOf(consumerSandbox, 2),
            forceNewChannel: true,
            connection: conn
          })
        });
      }
      var sandboxId3 = misc.getLogID();
      return getChannel(consumerSandbox).then(function(ch) {
        LX.isEnabledFor('info') && LX.log('info', consumeTrail.add({
          sandboxId: sandboxId3
        }).toMessage({
          text: 'getConsumerSandbox() - shared Connection and shared Channel'
        }));
        return initSandbox({
          id: sandboxId3,
          name: getSandboxNameOf(consumerSandbox, 3),
          connection: consumerSandbox.connection,
          channel: ch
        })
      });
    }).finally(function() {
      unlockConsumer(consumerSandbox);
    });
  }

  var lockConsumer = function(consumerSandbox) {
    return new Promise(function(onResolved, onRejected) {
      if (consumerSandbox.mutex) {
        consumerSandbox.mutex.lock(function() {
          onResolved();
          LX.isEnabledFor('info') && LX.log('info', engineTrail.toMessage({
            tags: ['engine.lockConsumer', 'obtain_mutex'],
            text: 'lockConsumer() - obtain mutex'
          }));
        });
      } else {
        onResolved();
        LX.isEnabledFor('info') && LX.log('info', engineTrail.toMessage({
          text: 'lockConsumer() - skipped'
        }));
      }
    });
  }

  var unlockConsumer = function(consumerSandbox) {
    if (consumerSandbox.mutex) {
      consumerSandbox.mutex.unlock();
      LX.isEnabledFor('info') && LX.log('info', engineTrail.toMessage({
        tags: ['engine.unlockConsumer', 'release_mutex'],
        text: 'unlockConsumer() - release mutex'
      }));
    } else {
      LX.isEnabledFor('info') && LX.log('info', engineTrail.toMessage({
        text: 'unlockConsumer() - skipped'
      }));
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

  LX.isEnabledFor('info') && LX.log('info', engineTrail.toMessage({
    text: 'Engine.new() end!'
  }));
};

module.exports = Engine;
