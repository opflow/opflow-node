'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var assert = require('chai').assert;
var expect = require('chai').expect;
var faker = require('faker');
var util = require('util');
var debugx = require('debug')('opflow:master:test');
var opflow = require('../../index');
var appCfg = require('../lab/app-configuration');
var bogen = require('../lab/big-object-generator');
var Loadsync = require('loadsync');

var Fibonacci = require('../lab/fibonacci').Fibonacci;
var fibonacci = require('../lab/fibonacci').fibonacci;

describe('opflow-master:', function() {

	describe('one master - one worker:', function() {
		var master, worker;

		before(function() {
			var cfg = appCfg.extend({
				feedback: {
					queueName: 'tdd-opflow-feedback',
					durable: true,
					noAck: false
				}
			});
			master = new opflow.RpcMaster(cfg);
			worker = new opflow.RpcWorker(cfg);
		});

		beforeEach(function(done) {
			done();
		});

		afterEach(function(done) {
			Promise.all([
				master.purge(), master.destroy(),
				worker.purge(), worker.destroy()
			]).then(lodash.ary(done, 0));
		});

		it('master request, worker process and response', function(done) {
			this.timeout(100000);
			var input = { number: 20 };
			Promise.all([worker.process(taskWorker)]).then(function() {
				return master.execute(input, {
					requestId: 'one-master-single-worker-' + (new Date()).toISOString()
				}).then(function(job) {
					return processTask(job);
				}).then(function(trail) {
					return {input, trail};
				});
			}).then(function(result) {
				validateResult(result);
				done(null);
			}).catch(function(error) {
				done(error);
			});
		});
	});

	describe('one master - multiple workers:', function() {
		var master, worker1, worker2;

		before(function() {
			var cfg = appCfg.extend({
				feedback: {
					queueName: 'tdd-opflow-feedback',
					durable: true,
					noAck: false
				}
			});
			master = new opflow.RpcMaster(cfg);
			worker1 = new opflow.RpcWorker(cfg);
			worker2 = new opflow.RpcWorker(cfg);
		});

		beforeEach(function(done) {
			done();
		});

		afterEach(function(done) {
			Promise.all([
				master.purge(), master.destroy(),
				worker1.purge(), worker1.destroy(),
				worker2.purge(), worker2.destroy()
			]).then(lodash.ary(done, 0));
		});

		it('master request to multiple workers, it should return correct results', function(done) {
			this.timeout(100000);
			var data = [10, 8, 20, 15, 11, 19, 25, 12, 16, 35, 34, 28].map(function(n) { return { number: n }});
			Promise.all([
				worker1.process(taskWorker),
				worker2.process(taskWorker)
			]).then(function() {
				return Promise.map(data, function(input) {
					return master.execute(input).then(function(job) {
						return processTask(job).then(function(trail) {
							return { input: input, trail: trail }
						});
					});
				}, {concurrency: 4});
			}).then(function(results) {
				lodash.forEach(results, validateResult);
				done(null);
			}).catch(function(error) {
				done(error);
			});
		});
	});
});

var taskWorker = function(data, info, done, notifier) {
	debugx.enabled && debugx('Request[%s] worker receives data: %s', info.requestId, data);
	var fibonacci = new Fibonacci(JSON.parse(data));
	while(fibonacci.next()) {
		var r = fibonacci.result();
		notifier.progress(r.step, r.number);
	};
	done(null, fibonacci.result());
};

var processTask = function(job) {
	var requestID = job.requestId;
	return new Promise(function(onResolved, onRejected) {
		var stepTracer = [];
		job.on('started', function(info) {
			stepTracer.push({ event: 'started', data: info});
			debugx.enabled && debugx('Request[%s] started', requestID);
		}).on('progress', function(percent, data) {
			stepTracer.push({ event: 'progress', data: {percent: percent}});
			debugx.enabled && debugx('Request[%s] progress: %s', requestID, percent);
		}).on('failed', function(error) {
			stepTracer.push({ event: 'failed', data: error});
			debugx.enabled && debugx('Request[%s] failed, error: %s', requestID, JSON.stringify(error));
			onRejected(error);
		}).on('completed', function(result) {
			stepTracer.push({ event: 'completed', data: result});
			debugx.enabled && debugx('Request[%s] done, result: %s', requestID, JSON.stringify(result));
			onResolved(stepTracer);
		});
	});
}

var validateResult = function(result) {
	var input = result.input, trail = result.trail;
	assert.equal(trail.length, 1 + input.number + 1);
	assert.equal(trail[0].event, 'started');
	for(var i=1; i<=input.number; i++) {
		assert.equal(trail[i].event, 'progress');
	}
	assert.equal(trail[input.number + 1].event, 'completed');
	assert.equal(trail[input.number + 1].data.number, input.number);
	assert.equal(trail[input.number + 1].data.value, fibonacci(input.number));
}