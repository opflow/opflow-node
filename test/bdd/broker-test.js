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
var LogTracer = require('logolite').LogTracer;
var appCfg = require('../lab/app-configuration');
var bogen = require('../lab/big-object-generator');
var Loadsync = require('loadsync');
var Fibonacci = require('../lab/fibonacci').Fibonacci;

process.env.LOGOLITE_ALWAYS_ENABLED='all';
require('logolite').LogConfig.reset();

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
					rpcWorker: [],
					subscriber: function(body, headers, finish) {}
				}, {
					autoinit: false,
					configDir: path.join(__dirname, '../cfg'),
					configName: 'builder-serverlet-test.conf',
					verbose: false
				}).then(function(handler) {
					serverlet = handler;
					return handler.start();
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
			var routineDescriptor = {
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
			}

			serverlet.registerRoutine(lodash.defaults({
				handler: function(input) {
					return new Fibonacci(input).calc();
				}
			}, routineDescriptor));

			var routine = commander.registerRoutine(routineDescriptor);

			routine.fibonacci({number: 40}).then(function(value) {
				console.log('Fibonacci output: %s', JSON.stringify(value));
				done(null);
			}).catch(function(error) {
				done(error);
			});
		})
	});
});