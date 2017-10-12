'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var assert = require('chai').assert;
var expect = require('chai').expect;
var path = require('path');
var util = require('util');
var debugx = require('debug')('tdd:opflow:Builder');
var OpflowBuilder = require('../../lib/builder');
var Loadsync = require('loadsync');

describe('opflow-builder:', function() {
	this.timeout(1000 * 60 * 60);

	describe('loadConfig() method:', function() {
		it('should return the configuration from default config file', function() {
			var cfg = OpflowBuilder.instance.loadConfig({
				configDir: path.join(__dirname, '../cfg'),
				default: {
					default1: 'value 1',
					default2: 'value 2'
				},
				verbose: true
			});
			var expected = {
				"default1": "value 1",
				"default2": "value 2",
				"json1": "String",
				"json2": 17779,
				"json3": true,
				"json4": {
					"msg": "This is an object"
				},
				"js1": 100,
				"js2": 101,
				"jsx": {
					"key": "hello",
					"value": [
						"world"
					]
				}
			};
			assert.deepEqual(cfg, expected);
			debugx.enabled && debugx('Configuration: %s', JSON.stringify(cfg));
		});

		it('should return the configuration object only by default', function() {
			var cfg = OpflowBuilder.instance.loadConfig({
				default: {
					default1: 'value 1',
					default2: 'value 2'
				},
				skipConfigFile: true,
				verbose: true
			});
			var expected = {
				"default1": "value 1",
				"default2": "value 2"
			};
			assert.deepEqual(cfg, expected);
			debugx.enabled && debugx('Configuration: %s', JSON.stringify(cfg));
		});

		it('should return the configuration object only if useDefaultConfigName is false', function() {
			var cfg = OpflowBuilder.instance.loadConfig({
				default: {
					default1: 'value 1',
					default2: 'value 2'
				},
				configDir: path.join(__dirname, '../cfg'),
				skipConfigFile: true,
				verbose: true
			});
			var expected = {
				"default1": "value 1",
				"default2": "value 2"
			};
			assert.deepEqual(cfg, expected);
			debugx.enabled && debugx('Configuration: %s', JSON.stringify(cfg));
		});

		it('should return the configuration from config files in OPFLOW_CONFIG_DIR', function() {
			process.env['OPFLOW_CONFIG_DIR'] = path.join(__dirname, '../cfg');
			var cfg = OpflowBuilder.instance.loadConfig({
				default: {
					default1: 'value 1',
					default2: 'value 2'
				},
				verbose: true
			});
			var expected = {
				"default1": "value 1",
				"default2": "value 2",
				"json1": "String",
				"json2": 17779,
				"json3": true,
				"json4": {
					"msg": "This is an object"
				},
				"js1": 100,
				"js2": 101,
				"jsx": {
					"key": "hello",
					"value": [
						"world"
					]
				}
			};
			assert.deepEqual(cfg, expected);
			debugx.enabled && debugx('Configuration: %s', JSON.stringify(cfg));
		});

		it('should return the configuration from OPFLOW_CONFIG_NAME files in OPFLOW_CONFIG_DIR', function() {
			process.env['OPFLOW_CONFIG_DIR'] = path.join(__dirname, '../cfg');
			process.env['OPFLOW_CONFIG_NAME'] = 'opflow.copy';
			var cfg = OpflowBuilder.instance.loadConfig({
				default: {
					default1: 'value 1',
					default2: 'value 2'
				},
				verbose: true
			});
			var expected = {
				"default1": "value 1",
				"default2": "value 2",
				"json1": "String",
				"json2": 17779,
				"json3": true,
				"json4": {
					"msg": "This is an opflow"
				},
				"js1": 200,
				"js2": 201,
				"jsx": {
					"key": "hello",
					"value": [
						"opflow"
					]
				}
			};
			assert.deepEqual(cfg, expected);
			debugx.enabled && debugx('Configuration: %s', JSON.stringify(cfg));
		});

		it('should return the configuration object from YAML file', function() {
			var cfg = OpflowBuilder.instance.loadConfig({
				default: {
					default1: 'value 1',
					default2: 'value 2'
				},
				configDir: path.join(__dirname, '../cfg'),
				configName: 'opflow.test.yml',
				verbose: true
			});
			var expected = {
				"default1": "value 1",
				"default2": "value 2",
				"opflow": {
					"yaml1": {
						"field1": "Hello world",
						"field2": 1024,
						"field3": 17.7
					},
					"yaml2": {
						"data1": {
							"field1": "salary",
							"field2": 100
						}
					}
				}
			};
			assert.deepEqual(cfg, expected);
			debugx.enabled && debugx('Configuration: %s', JSON.stringify(cfg));
		});

		it('should return the configuration object from both JSON and YAML files', function() {
			var cfg = OpflowBuilder.instance.loadConfig({
				default: {
					default1: 'value 1',
					default2: 'value 2'
				},
				configDir: path.join(__dirname, '../cfg'),
				configName: 'opflow.test',
				verbose: true
			});
			var expected = {
				"default1": "value 1",
				"default2": "value 2",
				"opflow": {
					"yaml1": {
						"field1": "Hello world",
						"field2": 1024,
						"field3": 17.7
					},
					"yaml2": {
						"data1": {
							"field1": "salary",
							"field2": 100
						},
						"data2": {
							"field1": "tax",
							"field2": 7
						}
					}
				}
			};
			assert.deepEqual(cfg, expected);
			debugx.enabled && debugx('Configuration: %s', JSON.stringify(cfg));
		});
	});

	describe('createPubsubHandler() method:', function() {
		var pubsubHandler;

		it('should return the PubsubHandler object if provided a correct configuration file', function(done) {
			OpflowBuilder.instance.createPubsubHandler({
				configDir: path.join(__dirname, '../cfg'),
				configName: 'builder-pubsub-test.conf',
				verbose: false
			}).then(function(handler) {
				pubsubHandler = handler;
				done();
			}).catch(function(error) {
				done(error);
			});
		});

		afterEach(function(done) {
			pubsubHandler && pubsubHandler.close().then(lodash.ary(done, 0));
		});
	});

	describe('createRpcMaster() method:', function() {
		var rpcMaster;

		it('should return the RpcMaster object if provided a correct configuration file', function(done) {
			OpflowBuilder.instance.createRpcMaster({
				configDir: path.join(__dirname, '../cfg'),
				configName: 'builder-rpc_master-test.conf',
				verbose: false
			}).then(function(handler) {
				rpcMaster = handler;
				done();
			}).catch(function(error) {
				done(error);
			});
		});

		afterEach(function(done) {
			rpcMaster && rpcMaster.close().then(lodash.ary(done, 0));
		});
	});

	describe('createRpcWorker() method:', function() {
		var rpcWorker;

		it('should return the RpcWorker object if provided a correct configuration file', function(done) {
			OpflowBuilder.instance.createRpcWorker({
				configDir: path.join(__dirname, '../cfg'),
				configName: 'builder-rpc_worker-test.conf',
				verbose: false
			}).then(function(handler) {
				rpcWorker = handler;
				done();
			}).catch(function(error) {
				done(error);
			});
		});

		afterEach(function(done) {
			rpcWorker && rpcWorker.close().then(lodash.ary(done, 0));
		});
	});

	describe('createRecycler() method:', function() {
		var recycler;

		it('should return the Recycler object if provided a correct configuration file', function(done) {
			OpflowBuilder.instance.createRecycler({
				configDir: path.join(__dirname, '../cfg'),
				configName: 'builder-recycler-test.conf',
				verbose: false
			}).then(function(handler) {
				recycler = handler;
				done();
			}).catch(function(error) {
				done(error);
			});
		});

		afterEach(function(done) {
			recycler && recycler.close().then(lodash.ary(done, 0));
		});
	});

	describe('createCommander() method:', function() {
		var commander;

		it('should return the Commander object if provided a correct configuration file', function(done) {
			OpflowBuilder.instance.createCommander({
				autoinit: false,
				configDir: path.join(__dirname, '../cfg'),
				configName: 'builder-commander-test.conf',
				verbose: false
			}).then(function(handler) {
				commander = handler;
				done();
			}).catch(function(error) {
				done(error);
			});
		});

		afterEach(function(done) {
			commander && commander.close().then(lodash.ary(done, 0));
		});
	});

	describe('createServerlet() method:', function() {
		var serverlet;

		it('should return the Serverlet object if provided a correct configuration file', function(done) {
			OpflowBuilder.instance.createServerlet({
				configurer: function(body, headers, finish) {},
				rpcWorker: [{
					routineId: 'fibonacci',
					handler: function(body, headers, response) {}
				}],
				subscriber: function(body, headers, finish) {}
			}, {
				autoinit: false,
				configDir: path.join(__dirname, '../cfg'),
				configName: 'builder-serverlet-test.conf',
				verbose: false
			}).then(function(handler) {
				serverlet = handler;
				done();
			}).catch(function(error) {
				done(error);
			});
		});

		afterEach(function(done) {
			serverlet && serverlet.close().then(lodash.ary(done, 0));
		});
	});
});