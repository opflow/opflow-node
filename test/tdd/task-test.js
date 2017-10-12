'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var assert = require('chai').assert;
var expect = require('chai').expect;
var debugx = require('debug')('tdd:opflow:task');
var TimeoutHandler = require('../../lib/task').TimeoutHandler;
var LogAdapter = require('../../lib/log_adapter');
var LogTracer = require('../../lib/log_tracer');
var misc = require('../../lib/util');
var appCfg = require('../lab/app-configuration');

describe('opflow.task:', function() {
	this.timeout(1000 * 60 * 60);

	var logCounter = {};
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
			assert.equal(logCounter.unfinishedTasks, 10);
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
			assert.equal(logCounter.raiseTimeoutCount, 7);
			assert.equal(logCounter.unfinishedTasks, 3); // 2400, 2700, 3000 (> 2200 ~ 3000 - 800)
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
			assert.equal(logCounter.raiseTimeoutCount, 7);
			assert.equal(logCounter.unfinishedTasks, 1); // 2400, 2700, 3000 (> 2200 ~ 3000 - 800)
			done();
		})
	});
});
