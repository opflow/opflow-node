'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var assert = require('chai').assert;
var expect = require('chai').expect;
var debugx = require('debug')('bdd:opflow:engine');
var OpflowEngine = require('../../lib/engine');
var OpflowExecutor = require('../../lib/executor');
var LogTracer = require('../../lib/log_tracer');
var appCfg = require('../lab/app-configuration');
var bogen = require('../lab/big-object-generator');
var Loadsync = require('loadsync');

describe('opflow-engine:', function() {
	this.timeout(1000 * 60 * 60);

	var counter = {};
	before(function() {
		LogTracer.clearStringifyInterceptors();
		LogTracer.addStringifyInterceptor(function(logobj) {
			appCfg.updateCounter(counter, [{
				message: 'getConnection() - connection is created',
				fieldName: 'connectionCreated'
			}, {
				message: 'closeConnection() - connection is closed',
				fieldName: 'connectionDestroyed'
			}, {
				message: 'getChannel() - make a new channel',
				fieldName: 'channelCreated'
			}, {
				message: 'closeChannel() - channel is closed',
				fieldName: 'channelDestroyed'
			}, {
				message: 'getChannel() - createConfirmChannel',
				fieldName: 'confirmChannel'
			}, {
				message: 'getProducerSandbox() - create producer sandbox',
				fieldName: 'producerSandbox'
			}, {
				message: 'lockProducer() - obtain mutex',
				fieldName: 'producerLocked'
			}, {
				message: 'lockProducer() - release mutex',
				fieldName: 'producerUnlocked'
			}, {
				message: 'lockProducer() - obtain semaphore',
				fieldName: 'producerLocked'
			}, {
				message: 'unlockProducer() - release semaphore',
				fieldName: 'producerUnlocked'
			}, {
				message: 'getConsumerSandbox() - create consumer sandbox',
				fieldName: 'consumerSandbox'
			}, {
				message: 'lockConsumer() - obtain mutex',
				fieldName: 'consumerLocked'
			}, {
				message: 'unlockConsumer() - release mutex',
				fieldName: 'consumerUnlocked'
			}, {
				message: 'produce() confirmation has failed',
				fieldName: 'confirmationFailed'
			}, {
				message: 'produce() confirmation has completed',
				fieldName: 'confirmationCompleted'
			}, {
				message: 'exhaust() confirmation has failed',
				fieldName: 'confirmStreamFailed'
			}, {
				message: 'exhaust() confirmation has completed',
				fieldName: 'confirmStreamCompleted'
			}], logobj);
		});
	});

	after(function() {
		LogTracer.clearStringifyInterceptors();
	})

	describe('consume() method:', function() {
		var handler;
		var executor;
		var queue = {
			queueName: 'tdd-opflow-queue',
			durable: true,
			noAck: true,
			binding: true
		};

		before(function(done) {
			handler = new OpflowEngine(appCfg.extend());
			executor = new OpflowExecutor({ engine: handler });
			executor.purgeQueue(queue).then(lodash.ary(done, 0));
		});

		beforeEach(function(done) {
			appCfg.checkSkip.call(this);
			counter = {};
			handler.ready().then(function() {
				return executor.purgeQueue(queue);
			}).then(lodash.ary(done, 0));
		});

		afterEach(function(done) {
			handler.close().then(function() {
				debugx.enabled && debugx('COUNTER: ' + JSON.stringify(counter));
				assert.equal(counter.connectionCreated, counter.connectionDestroyed);
				done();
			});
		});

		it('preserve the order of elements', function(done) {
			var total = 10;
			var index = 0;
			handler.consume(function(msg, info, finish) {
				var message_code = parseInt(msg.properties.correlationId);
				var message = JSON.parse(msg.content);
				assert(message.code === index++);
				assert.equal(msg.properties.appId, 'engine-operator-tdd');
				assert.equal(msg.properties.messageId, 'message#' + message_code);
				assert.deepInclude(msg.properties.headers, {
					key1: 'test ' + message_code,
					key2: 'test ' + (message_code + 1)
				});
				assert.isTrue(Object.keys(msg.properties.headers).length >= 2);
				finish();
				if (index >= total) {
					handler.cancelConsumer(info).then(lodash.ary(done, 0));
				}
			}, queue).then(function() {
				Promise.mapSeries(lodash.range(total), function(count) {
					return handler.produce({
						code: count, msg: 'Hello world'
					}, {
						appId: 'engine-operator-tdd',
						messageId: 'message#' + count,
						correlationId: JSON.stringify(count),
						headers: {
							key1: 'test ' + count,
							key2: 'test ' + (count + 1)
						}
					});
				});
			});
		});

		it('push elements to queue massively', function(done) {
			var max = 1000;
			var idx = lodash.range(max);
			var n0to9 = lodash.range(10);
			var count = 0;
			handler.consume(function(msg, info, finish) {
				var message = JSON.parse(msg.content.toString());
				var pos = idx.indexOf(message.code);
				if (pos >= 0) idx.splice(pos, 1);
				finish();
				count++;
				if (count >= max * 10) {
					debugx.enabled && debugx('All of messages have been processed');
					assert(idx.length === 0);
					handler.cancelConsumer(info).then(lodash.ary(done, 0));
				}
			}, queue).then(function() {
				Promise.reduce(lodash.range(max), function(state, n) {
					return Promise.each(n0to9, function(k) {
						handler.produce({ code: (10*n + k), msg: 'Hello world' });
					}).delay(1);
				}, {});
			});
		});

		it('push large elements to queue', function(done) {
			var total = 10;
			var index = 0;
			var bog = new bogen.BigObjectGenerator({numberOfFields: 1000, max: total});
			handler.consume(function(msg, info, finish) {
				var message = JSON.parse(msg.content);
				assert(message.code === index++);
				finish();
				if (index >= total) {
					handler.cancelConsumer(info).then(lodash.ary(done, 0));
				}
			}, queue).then(function() {
				Promise.mapSeries(lodash.range(total), function(count) {
					return bog.next().then(function(randobj) {
						return handler.produce(randobj);
					});
				});
			});
		});
	});

	describe('produce() customize routingKey', function() {
		var handler0;
		var handler1;
		var queue0 = {
			queueName: 'tdd-opflow-queue',
			durable: true,
			noAck: true,
			binding: true
		}
		var queue1 = {
			queueName: 'tdd-opflow-clone',
			durable: true,
			noAck: true,
			binding: true
		}
		var executor0;
		var executor1;

		before(function() {
			handler0 = new OpflowEngine(appCfg.extend());
			handler1 = new OpflowEngine(appCfg.extend({
				routingKey: 'tdd-opflow-backup',
				consumer: undefined,
				exchangeQuota: undefined
			}));
			executor0 = new OpflowExecutor({
				engine: handler0
			});
			executor1 = new OpflowExecutor({
				engine: handler1
			});
		});

		beforeEach(function(done) {
			appCfg.checkSkip.call(this);
			Promise.all([
				handler0.ready(), executor0.purgeQueue(queue0),
				handler1.ready(), executor1.purgeQueue(queue1)
			]).then(lodash.ary(done, 0));
		});

		afterEach(function(done) {
			Promise.all([
				handler0.close(),
				handler1.close()
			]).then(lodash.ary(done, 0));
		});

		it('copy message to another queue (CC)', function(done) {
			var total = 10;

			var loadsync = new Loadsync([{
				name: 'testsync',
				cards: ['handler0', 'handler1']
			}]);

			var index0 = 0;
			var ok0 = handler0.consume(function(msg, info, finish) {
				var message = JSON.parse(msg.content.toString());
				assert(message.code === index0++);
				finish();
				if (index0 >= total) {
					handler0.cancelConsumer(info).then(function() {
						loadsync.check('handler0', 'testsync');
					});
				}
			}, queue0);

			var index1 = 0;
			var ok1 = handler1.consume(function(msg, info, finish) {
				var message = JSON.parse(msg.content.toString());
				assert(message.code === index1++);
				finish();
				if (index1 >= total) {
					handler1.cancelConsumer(info).then(function() {
						loadsync.check('handler1', 'testsync');	
					});
				}
			}, queue1);

			loadsync.ready(function(info) {
				done();
			}, 'testsync');

			Promise.all([ok0, ok1]).then(function() {
				lodash.range(total).forEach(function(count) {
					handler0.produce({ code: count, msg: 'Hello world' }, {CC: 'tdd-opflow-backup'});
				});
			});
		});

		it('redirect to another queue by changing routingKey', function(done) {
			var total = 10;
			var index = 0;
			var ok1 = handler1.consume(function(msg, info, finish) {
				var message = JSON.parse(msg.content.toString());
				assert(message.code === index++);
				finish();
				if (index >= total) {
					handler1.cancelConsumer(info).then(lodash.ary(done, 0));
				}
			}, queue1);
			ok1.then(function() {
				lodash.range(total).forEach(function(count) {
					handler0.produce({ code: count, msg: 'Hello world' }, {}, {
						routingKey: 'tdd-opflow-backup'
					});
				});
			});
		});
	});

	describe('ConfirmChannel:', function() {
		var handler;
		var executor;
		var queue = {
			queueName: 'tdd-opflow-queue',
			durable: true,
			noAck: false,
			binding: true
		};

		before(function(done) {
			handler = new OpflowEngine(appCfg.extend({
				confirmation: {}
			}));
			executor = new OpflowExecutor({ engine: handler });
			executor.purgeQueue(queue).then(lodash.ary(done, 0));
		});

		beforeEach(function(done) {
			appCfg.checkSkip.call(this);
			counter = {};
			handler.ready().then(function() {
				return executor.purgeQueue(queue);
			}).then(lodash.ary(done, 0));
		});

		afterEach(function(done) {
			handler.close().then(function() {
				debugx.enabled && debugx('COUNTER: ' + JSON.stringify(counter));
				assert.equal(counter.connectionCreated, counter.connectionDestroyed);
				assert.equal(counter.channelCreated, counter.channelDestroyed);
				assert.equal(counter.confirmChannel, 1);
				assert.equal(counter.producerLocked, counter.producerUnlocked);
				done();
			});
		});

		it('Confirm that server has dispatched messages', function(done) {
			var total = 100;
			var index = 0;
			Promise.mapSeries(lodash.range(total), function(count) {
				return handler.produce({
					code: count, msg: 'Hello world'
				}, {
					appId: 'engine-operator-tdd',
					messageId: 'message#' + count,
					correlationId: JSON.stringify(count),
					headers: {
						key1: 'test ' + count,
						key2: 'test ' + (count + 1)
					}
				});
			}).then(lodash.ary(done, 0));
		});

		it('Resend messages that have not confirmed', function(done) {
			var total = 10;
			var index = 0;
			handler.consume(function(msg, info, finish) {
				var message_code = parseInt(msg.properties.correlationId);
				var message = JSON.parse(msg.content);
				assert(message.code === index++);
				assert.equal(msg.properties.appId, 'engine-operator-tdd');
				assert.equal(msg.properties.messageId, 'message#' + message_code);
				assert.deepInclude(msg.properties.headers, {
					key1: 'test ' + message_code,
					key2: 'test ' + (message_code + 1)
				});
				assert.isTrue(Object.keys(msg.properties.headers).length >= 2);
				finish();
				if (index >= total) {
					handler.cancelConsumer(info).then(lodash.ary(done, 0));
				}
			}, queue).then(function() {
				Promise.mapSeries(lodash.range(total), function(count) {
					return handler.produce({
						code: count, msg: 'Hello world'
					}, {
						appId: 'engine-operator-tdd',
						messageId: 'message#' + count,
						correlationId: JSON.stringify(count),
						headers: {
							key1: 'test ' + count,
							key2: 'test ' + (count + 1)
						}
					});
				});
			});
		});
	});

	describe('exhaust():', function() {
		var FIELDS = bogen.FIELDS || 10000;
		var TOTAL = bogen.TOTAL || 1000;
		var TIMEOUT = bogen.TIMEOUT || 0;
		var handler, executor;
		var queue = {
			queueName: 'tdd-opflow-queue',
			durable: true,
			noAck: false,
			binding: true
		};

		before(function(done) {
			handler = new OpflowEngine(appCfg.extend({
				confirmation: {}
			}));
			executor = new OpflowExecutor({ engine: handler });
			executor.purgeQueue(queue).then(lodash.ary(done, 0));
		});

		beforeEach(function(done) {
			appCfg.checkSkip.call(this);
			counter = {};
			handler.ready().then(function() {
				return executor.purgeQueue(queue);
			}).then(lodash.ary(done, 0));
		});

		afterEach(function(done) {
			handler.close().then(function() {
				debugx.enabled && debugx('COUNTER: ' + JSON.stringify(counter));
				assert.equal(counter.connectionCreated, counter.connectionDestroyed);
				assert.equal(counter.channelCreated, counter.channelDestroyed);
				assert.equal(counter.confirmChannel, 1);
				assert.equal(counter.confirmStreamCompleted, TOTAL);
				assert.equal(counter.producerLocked, counter.producerUnlocked);
				assert.equal(counter.consumerLocked, counter.consumerUnlocked);
				done();
			});
		});

		it('emit drain event if the exhaust() is overflowed', function(done) {
			var index = 0;
			var check = lodash.range(TOTAL);
			var bog = new bogen.BigObjectGenerator({numberOfFields: FIELDS, max: TOTAL, timeout: TIMEOUT});
			handler.consume(function(msg, info, finish) {
				var message = JSON.parse(msg.content);
				check.splice(check.indexOf(message.code), 1);
				finish();
				if (++index >= TOTAL) {
					handler.cancelConsumer(info).then(lodash.ary(done, 0));
				}
			}, queue).then(function() {
				var bos = new bogen.BigObjectStreamify(bog, {objectMode: true});
				return handler.exhaust(bos);
			}).then(function() {
				debugx.enabled && debugx('exhaust() - done');
			}).catch(function(err) {
				debugx.enabled && debugx('exhaust() - error');
				done(err);
			});
		});

		it('insert some messages to working stream', function(done) {
			var count = 0;
			var check = lodash.range(TOTAL);
			var bog = new bogen.BigObjectGenerator({numberOfFields: FIELDS, max: TOTAL, timeout: TIMEOUT});
			var bo9 = new bogen.BigObjectGenerator({numberOfFields: FIELDS, min: TOTAL, max: TOTAL+1, timeout: 0});
			var successive = true;
			var ok = handler.consume(function(msg, info, finish) {
				var message = JSON.parse(msg.content);
				if (message.code !== count) successive = false;
				check.splice(check.indexOf(message.code), 1);
				finish();
				if (++count >= (TOTAL+1)) {
					assert.equal(counter.confirmationCompleted, 1);
					handler.cancelConsumer(info).then(lodash.ary(done, 0));
				}
			});
			ok.then(function() {
				var bos = new bogen.BigObjectStreamify(bog, {objectMode: true});
				setTimeout(function() {
					bo9.next().then(function(data) {
						debugx.enabled && debugx('produce() - inserting data');
						handler.produce(data).then(function() {
							debugx.enabled && debugx('produce() - data inserted');
						});
					});
				}, Math.round(100 + TIMEOUT * TOTAL / 2));
				debugx.enabled && debugx('exhaust() - start');
				return handler.exhaust(bos);
			}).then(function() {
				debugx.enabled && debugx('exhaust() - done');
			}).catch(function(err) {
				debugx.enabled && debugx('exhaust() - error');
				done(err);
			});
		});
	});
});
