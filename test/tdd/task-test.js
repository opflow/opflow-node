'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var assert = require('chai').assert;
var expect = require('chai').expect;
var streamBuffers = require('stream-buffers');
var miss = require('mississippi');
var debugx = require('debug')('tdd:opflow:task');
var PayloadReader = require('../../lib/task').PayloadReader;
var PayloadWriter = require('../../lib/task').PayloadWriter;
var TimeoutHandler = require('../../lib/task').TimeoutHandler;
var LogAdapter = require('../../lib/log_adapter');
var LogTracer = require('../../lib/log_tracer');
var misc = require('../../lib/util');
var appCfg = require('../lab/app-configuration');

describe('opflow.task:', function() {
	this.timeout(1000 * 60 * 60);

	var logCounter = {};

	describe('TimeoutHandler:', function() {
		before(function() {
			LogTracer.clearStringifyInterceptors();
			LogTracer.addStringifyInterceptor(function(logobj) {
				appCfg.updateCounter(logCounter, [{
					message: 'TimeoutHandler checking loop is invoked',
					fieldName: 'checkingCount'
				}, {
					message: 'TimeoutHandler will be stopped, SAVE UNFINISHED TASKS',
					fieldName: 'unfinishedTasks'
				}, {
					message: 'TimeoutHandler task is timeout, event will be raised',
					fieldName: 'raiseTimeoutCount'
				}], logobj);
			});
		});

		after(function() {
			LogTracer.clearStringifyInterceptors();
		});

		beforeEach(function() {
			appCfg.checkSkip.call(this);
		});

		it('all of timeout tasks will be save when TimeoutHandler is stopping', function(done) {
			logCounter = {};
			var tasks = {};
			var th = new TimeoutHandler({
				monitorId: misc.getUUID(),
				interval: 10,
				timeout: 2000,
				tasks: tasks,
				raiseTimeout: function(done) {
					done();
				}
			});
			th.start();
			Promise.resolve().then(function() {
				lodash.range(10).forEach(function(i) {
					th.add('task' + i, {});
				});
				return Promise.resolve().delay(700);
			}).then(function() {
				return th.stop({ timeout: 1000 });
			}).then(function() {
				debugx.enabled && debugx('logCounter: %s', JSON.stringify(logCounter));
				if (LogTracer.isInterceptorEnabled) {
					assert.equal(logCounter.unfinishedTasks, 10);
				}
				done();
			})
		});

		it('3 latest tasks will be unfinished and save when TimeoutHandler stopped', function(done) {
			logCounter = {};
			var timeoutCount = 0;
			var tasks = {};
			var th = new TimeoutHandler({
				monitorId: misc.getUUID(),
				interval: 10,
				timeout: 800,
				tasks: tasks,
				raiseTimeout: function(done) {
					timeoutCount += 1;
					done();
				}
			});
			th.start();
			Promise.resolve().then(function() {
				return Promise.map(lodash.range(10), function(i) {
					return Promise.resolve().delay(300 * (i + 1)).then(function() {
						debugx.enabled && debugx('#: %s', i);
						th.add('task' + i, { number: i });
						return true;
					});
				});
			}).then(function() {
				return th.stop({ timeout: 1000 });
			}).then(function() {
				debugx.enabled && debugx('logCounter: %s', JSON.stringify(logCounter));
				if (LogTracer.isInterceptorEnabled) {
					assert.equal(logCounter.raiseTimeoutCount, 7);
					assert.equal(logCounter.unfinishedTasks, 3); // 2400, 2700, 3000 (> 2200 ~ 3000 - 800)
				}
				done();
			})
		});

		it('the latest task will be unfinished and save when TimeoutHandler stopped', function(done) {
			logCounter = {};
			var timeoutCount = 0;
			var tasks = {};
			var th = new TimeoutHandler({
				monitorId: misc.getUUID(),
				interval: 10,
				timeout: 800,
				tasks: tasks,
				raiseTimeout: function(done) {
					timeoutCount += 1;
					done();
				}
			});
			th.start();
			Promise.resolve().then(function() {
				return Promise.map(lodash.range(10), function(i) {
					return Promise.resolve().delay(300 * (i + 1)).then(function() {
						debugx.enabled && debugx('#: %s', i);
						th.add('task' + i, { number: i });
						return true;
					});
				});
			}).then(function() {
				setTimeout(function() { th.remove('task7') }, 500);
				setTimeout(function() { th.remove('task8') }, 800);
				setTimeout(function() { th.remove('task9') }, 1100);
				return th.stop({ timeout: 1000 });
			}).then(function() {
				debugx.enabled && debugx('logCounter: %s', JSON.stringify(logCounter));
				if (LogTracer.isInterceptorEnabled) {
					assert.equal(logCounter.raiseTimeoutCount, 7);
					assert.equal(logCounter.unfinishedTasks, 1); // 2400, 2700, 3000 (> 2200 ~ 3000 - 800)
				}
				done();
			})
		});
	});

	describe('PayloadReader:', function() {
		var chunkState = null;

		before(function() {
			LogTracer.clearStringifyInterceptors();
			LogTracer.addStringifyInterceptor(function(logobj) {
				appCfg.updateCounter(logCounter, [{
					message: 'addChunk() - inserted',
					fieldName: 'chunkInserted'
				}, {
					message: 'addChunk() - skipped',
					fieldName: 'chunkSkipped'
				}, {
					message: 'doRead() - pushed',
					fieldName: 'chunkPushed'
				}, {
					message: 'doRead() - waiting',
					fieldName: 'chunkWaiting'
				}, {
					message: 'raiseFinal() - total',
					fieldName: 'chunkFinish'
				}, {
					message: 'raiseError() - error',
					fieldName: 'payloadError'
				}], logobj);
				if (logobj.message === 'doRead() - status') {
					debugx.enabled && debugx('Chunk: %s', JSON.stringify(logobj.state));
					chunkState = logobj.state;
				}
				if (logobj.message === 'doRead() - pushed') {
					logCounter.indices = logCounter.indices || [];
					logCounter.indices.push(logobj.index);
				}
			});
		});

		after(function() {
			LogTracer.clearStringifyInterceptors();
		});

		beforeEach(function() {
			appCfg.checkSkip.call(this);
		});

		it('PayloadReader waiting for the lack chunks', function(done) {
			logCounter = {};
			var readableStream = new PayloadReader();
			var writableStream = new streamBuffers.WritableStreamBuffer();
			readableStream.pipe(writableStream).on('finish', function() {
				var text = writableStream.getContentsAsString();
				assert.equal(text, 'Hello World!');
				if (LogTracer.isInterceptorEnabled) {
					debugx.enabled && debugx('logCounter: %s', JSON.stringify(logCounter));
					assert.equal(logCounter.chunkWaiting, 1);
					assert.equal(logCounter.chunkPushed, logCounter.chunkInserted + 1);
				}
				done();
			});
			readableStream.addChunk(0, 'Hello');
			readableStream.addChunk(2, 'World!');
			setTimeout(function() {
				readableStream.addChunk(1, ' ');
			}, 2000);
			readableStream.raiseFinal();
		});

		it('invoke callback when the chunk has been pushed', function(done) {
			logCounter = {};
			var orders = [];
			var updateOrders = function(data, index) {
				orders.push(index);
			}
			var readableStream = new PayloadReader();
			var writableStream = new streamBuffers.WritableStreamBuffer();
			readableStream.pipe(writableStream).on('finish', function() {
				var text = writableStream.getContentsAsString();
				assert.equal(text, 'Hello World!');
				debugx.enabled && debugx('orders: %s', JSON.stringify(orders));
				assert.sameOrderedMembers(orders, [0, 1, 2]);
				if (LogTracer.isInterceptorEnabled) {
					debugx.enabled && debugx('logCounter: %s', JSON.stringify(logCounter));
					assert.equal(logCounter.chunkWaiting, 1);
					assert.equal(logCounter.chunkPushed, logCounter.chunkInserted + 1);
				}
				done();
			});
			readableStream.addChunk(0, 'Hello', updateOrders);
			readableStream.addChunk(2, 'World!', updateOrders);
			setTimeout(function() {
				readableStream.addChunk(1, ' ', updateOrders);
			}, 2000);
			readableStream.raiseFinal();
		});

		it('emit an "error" event after raiseError()', function(done) {
			logCounter = {};
			var readableStream = new PayloadReader();
			var writableStream = new streamBuffers.WritableStreamBuffer();
			readableStream
				.on('error', function(error) {
					debugx.enabled && debugx('raiseError(): %s', JSON.stringify(error));
					done();
				})
				.pipe(writableStream)
				.on('finish', function() {
					done({
						message: 'PayloadReader.raiseError() should raise an error event'
					});
				});
			readableStream.addChunk(0, 'Hello');
			readableStream.addChunk(2, 'World!');
			setTimeout(function() {
				readableStream.addChunk(1, ' ');
			}, 2000);
			readableStream.raiseError({ message: 'Suddenly a random error occurred' });
		});
	});

	describe('PayloadWriter:', function() {
		
		before(function() {
			LogTracer.clearStringifyInterceptors();
			LogTracer.addStringifyInterceptor(function(logobj) {
				appCfg.updateCounter(logCounter, [{
					message: 'addChunk() - inserted',
					fieldName: 'chunkInserted'
				}], logobj);
			});
		});

		after(function() {
			LogTracer.clearStringifyInterceptors();
		});

		beforeEach(function() {
			appCfg.checkSkip.call(this);
		});

		it('emit an "error" event if next() is failed', function(done) {
			var payloadStream = new streamBuffers.ReadableStreamBuffer();
			var writableStream = new PayloadWriter(function (chunk, count) {
				return Promise.reject({
					message: 'Suddenly a random error occurred'
				}).delay(1000);
			});
			payloadStream.pipe(writableStream).on('error', function(error) {
				debugx.enabled && debugx('next() error: %s', JSON.stringify(error));
				done();
			});
			payloadStream.put('Hello world');
			payloadStream.stop();
		});

		it('emit an "error" event after forceClose()', function(done) {
			var payloadStream = new streamBuffers.ReadableStreamBuffer();
			var writableStream = new PayloadWriter(function (chunk, count) {
				return Promise.resolve().delay(1000);
			});
			payloadStream.pipe(writableStream).on('finish', function() {
				done({ message: 'PayloadWriter.raiseError() should raise an error event' });
			}).on('error', function(error) {
				debugx.enabled && debugx('next() error: %s', JSON.stringify(error));
				done();
			});
			payloadStream.put('Hello world');
			writableStream.forceClose();
			payloadStream.stop();
		});
	});
});
