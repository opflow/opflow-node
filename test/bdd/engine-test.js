'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var assert = require('chai').assert;
var expect = require('chai').expect;
var debugx = require('debug')('bdd:opflow:engine');
var OpflowEngine = require('../../lib/engine');
var OpflowExecutor = require('../../lib/executor');
var appCfg = require('../lab/app-configuration');
var bogen = require('../lab/big-object-generator');
var Loadsync = require('loadsync');

describe('opflow-engine:', function() {
	this.timeout(1000 * 60 * 60);

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
			executor = new OpflowExecutor({
				engine: handler
			});
			executor.purgeQueue(queue).then(lodash.ary(done, 0));
		});

		beforeEach(function(done) {
			handler.ready().then(function() {
				return executor.purgeQueue(queue);
			}).then(lodash.ary(done, 0));
		});

		afterEach(function(done) {
			handler.close().then(lodash.ary(done, 0));
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
});
