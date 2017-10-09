'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var assert = require('chai').assert;
var expect = require('chai').expect;
var debugx = require('debug')('bdd:opflow:limited');
var OpflowEngine = require('../../lib/engine');
var OpflowExecutor = require('../../lib/executor');
var appCfg = require('../lab/app-configuration');
var bogen = require('../lab/big-object-generator');
var Loadsync = require('loadsync');

describe('opflow-engine:', function() {
	this.timeout(1000 * 60 * 60);

	describe('no limit of consumers if maxSubscribers is undefined', function() {
		var handler, executor;

		before(function() {
			handler = new OpflowEngine(appCfg.extend());
			executor = new OpflowExecutor({ engine: handler });
		});

		beforeEach(function(done) {
			appCfg.checkSkip.call(this);
			Promise.all([
				handler.ready()
			]).then(lodash.ary(done, 0));
		});

		afterEach(function(done) {
			handler.close().then(lodash.ary(done, 0));
		});

		it('no limit of consumers', function(done) {
			var total = 10;
			var index = 0;
			Promise.mapSeries(lodash.range(total), function(count) {
				return handler.consume(function(message, sandbox, finish) {
					setTimeout(finish, 10);
				}, {
					queueName: 'tdd-opflow-queue'
				}).then(function(result) {
					assert.isNotNull(result.consumerTag);
					return result;
				});
			}).then(function() {
				executor.checkQueue({
					queueName: 'tdd-opflow-queue'
				}).then(function(info) {
					assert.equal(info.consumerCount, total, 'no limit of consumers');
					done();
				});
			});
		});
	});

	describe('exceeding quota limits of consumers', function() {
		var handler, executor;
		var total = 10;
		var limit = 7;
		var consumerOpts = {
			queueName: 'tdd-opflow-queue',
			consumerLimit: limit
		}

		before(function() {
			handler = new OpflowEngine(appCfg.extend());
			executor = new OpflowExecutor({ engine: handler });
		});

		beforeEach(function(done) {
			appCfg.checkSkip.call(this);
			handler.ready().then(lodash.ary(done, 0));
		});

		afterEach(function(done) {
			handler.close().then(lodash.ary(done, 0));
		});

		it('limit of consumers to ' + limit, function(done) {
			var index = 0;
			Promise.mapSeries(lodash.range(total), function(count) {
				return handler.consume(function(message, info, finish) {
					setTimeout(finish, 10);
				}, consumerOpts).then(function(success) {
					assert.isNotNull(success.consumerTag);
					return success;
				}).catch(function(failure) {
					debugx.enabled && debugx('received exception: %s', JSON.stringify(failure));
					assert.equal(failure.maxSubscribers, limit);
					return failure;
				});
			}).then(function() {
				executor.checkQueue(consumerOpts).then(function(info) {
					assert.equal(info.consumerCount, limit, 'limit of consumers to ' + limit);
					done();
				});
			});
		});
	});
});
