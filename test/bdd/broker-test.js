'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var assert = require('chai').assert;
var expect = require('chai').expect;
var path = require('path');
var util = require('util');
var debugx = require('debug')('bdd:opflow:broker');
var OpflowBuilder = require('../../lib/builder');
var OpflowCommander = require('../../lib/commander');
var OpflowServerlet = require('../../lib/serverlet');
var LogTracer = require('../../lib/log_tracer');
var appCfg = require('../lab/app-configuration');
var bogen = require('../lab/big-object-generator');
var Loadsync = require('loadsync');
var Fibonacci = require('../lab/fibonacci').Fibonacci;

describe('opflow-broker:', function() {
	this.timeout(1000 * 60 * 60);

	describe('registerMethod() method:', function() {
		var commander = null;
		var serverlet = null;

		beforeEach(function(done) {
			appCfg.checkSkip.call(this);
			Promise.all([
				OpflowBuilder.instance.createCommander({
					autoinit: false,
					configDir: path.join(__dirname, '../cfg'),
					configName: 'builder-commander-test.conf',
					verbose: false
				}).then(function(handler) {
					commander = handler;
					return handler;
				}),
				OpflowBuilder.instance.createServerlet({
					configurer: function(body, headers, finish) {},
					rpcWorker: [{
						routineId: 'fibonacci',
						handler: function(body, headers, response) {
							response.emitStarted();
							var fibonacci = new Fibonacci(JSON.parse(body)[0]);
							while(fibonacci.next()) {
								var r = fibonacci.result();
								response.emitProgress(r.step, r.number);
							};
							response.emitCompleted(fibonacci.result());
						}
					}],
					subscriber: function(body, headers, finish) {}
				}, {
					autoinit: false,
					configDir: path.join(__dirname, '../cfg'),
					configName: 'builder-serverlet-test.conf',
					verbose: false
				}).then(function(handler) {
					serverlet = handler;
					return handler;
				})
			]).then(lodash.ary(done, 0)).catch(function(error) {
				done(error);
			});;
		});

		afterEach(function(done) {
			Promise.all([
				commander.close(),
				serverlet.close()
			]).then(lodash.ary(done, 0)).finally(function() {
				commander = null;
				serverlet = null;
			});
		});

		it('should return object with registered method', function(done) {
			var handler = commander.registerMethod({
				name: "fibonacci",
				schema: {
					"type": "array",
					"items": {
						"type": [{
							"type": "object",
							"properties": {
								"number": { "type": "number" }
							}
						}]
					}
				}
			});
			handler.fibonacci({number: 40}).then(function(value) {
				console.log('XXXXXXXXXXX: %s', JSON.stringify(value));
				done(null);
			}).catch(function(error) {
				done(error);
			});
		})
	});
});