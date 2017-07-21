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
			master = new opflow.Master(cfg);
			worker = new opflow.Worker(cfg);
		});

		beforeEach(function(done) {
			done();
		});

		afterEach(function(done) {
			done();
		});

		it('master request, worker process and response', function(done) {
			this.timeout(100000);
			worker.process(function(data, done, notifier) {
				debugx.enabled && debugx('Input data: %s', data);
				var fibonacci = new Fibonacci(JSON.parse(data));
				while(fibonacci.next()) {
					var r = fibonacci.result();
					notifier.progress(r.step, r.number);
				};
				done(null, fibonacci.result());
			});
			var input = { number: 20 };
			master.execute(input).then(function(job) {
				var requestID = job.requestId;
				return new Promise(function(onResolved, onRejected) {
					var stepTracer = [];
					job.on('started', function(info) {
						stepTracer.push({ event: 'started', data: info});
						debugx.enabled && debugx('Task[%s] started', requestID);
					}).on('progress', function(percent, data) {
						stepTracer.push({ event: 'progress', data: {percent: percent}});
						debugx.enabled && debugx('Task[%s] progress: %s', requestID, percent);
					}).on('failed', function(error) {
						stepTracer.push({ event: 'failed', data: error});
						debugx.enabled && debugx('Task[%s] failed, error: %s', requestID, JSON.stringify(error));
						onRejected(error);
					}).on('completed', function(result) {
						stepTracer.push({ event: 'completed', data: result});
						debugx.enabled && debugx('Task[%s] done, result: %s', requestID, JSON.stringify(result));
						onResolved(stepTracer);
					});
				});
			}).then(function(trail) {
				assert.equal(trail.length, 1 + input.number + 1);
				assert.equal(trail[0].event, 'started');
				for(var i=1; i<=input.number; i++) {
					assert.equal(trail[i].event, 'progress');
				}
				assert.equal(trail[input.number + 1].event, 'completed');
				assert.equal(trail[input.number + 1].data.number, input.number);
				assert.equal(trail[input.number + 1].data.value, fibonacci(input.number));
				done(null);
			}).catch(function(error) {
				done(error);
			});
		});
	});
});

var Fibonacci = function Fibonacci(P) {
	var n = P && P.number && P.number >= 0 ? P.number : null;
	var c = 0;
	var f = 0, f_1 = 0, f_2 = 0;

	this.next = function() {
		if (c >= n) return false;
		if (++c < 2) {
			f = c;
		} else {
			f_2 = f_1; f_1 = f; f = f_1 + f_2;
		}
		return true;
	}

	this.result = function() {
		return { value: f, step: c, number: n };
	}
}

var fibonacci = function fibonacci(n) {
	if (n == 0 || n == 1) return n;
	else return fibonacci(n - 1) + fibonacci(n - 2);
}