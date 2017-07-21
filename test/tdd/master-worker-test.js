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
				console.log('Data: %s', data);
				data = data || {};
				data = JSON.parse(data);
				var number = data.number;
				console.log('Number: %s', number);
				var result = fibonacci(number, number, {
					update: function(completed, total) {
						notifier.progress(completed, total);
					}
				});
				setTimeout(function() {
					done(null, {result: result});
				}, 1000);
			});
			var input = { number: 10 };
			master.execute(input).then(function(job) {
				job.on('started', function(info) {
					console.log('Job started');
				}).on('progress', function(percent, data) {
					console.log('Job progress: %s', percent);
				}).on('failed', function(error) {
					console.log('Job error');
					done(error);
				}).on('completed', function(result) {
					console.log('Job done, result: %s', JSON.stringify(result));
					done();
				})
			})
		});
	});
});

function fibonacci(n, max, progressMeter) {
  if (progressMeter) {
    progressMeter.update(max - n + 1, max);
  }
  if (n == 0 || n == 1) {
    return n;
  } else {
    return fibonacci(n - 1, max, progressMeter) + fibonacci(n - 2);
  }
}