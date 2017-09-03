'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var assert = require('chai').assert;
var expect = require('chai').expect;
var path = require('path');
var util = require('util');
var debugx = require('debug')('opflow:loader:test');
var OpflowLoader = require('../../lib/loader');
var Loadsync = require('loadsync');

describe('opflow-loader:', function() {
	describe('loadConfig() method:', function() {
		it('should return the configuration from default config file', function() {
			var cfg = OpflowLoader.instance.loadConfig({
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
		})
	});

	describe('createPubsubHandler() method:', function() {
		var pubsubHandler;

		it('should return the PubsubHandler object if provided a correct configuration file', function() {
			pubsubHandler = OpflowLoader.instance.createPubsubHandler({
				configDir: path.join(__dirname, '../cfg'),
				configName: 'loader-pubsub-test.conf',
				verbose: false
			});
		});

		afterEach(function(done) {
			pubsubHandler && pubsubHandler.close().then(lodash.ary(done, 0));
		});
	});

	describe('createRpcMaster() method:', function() {
		var rpcMaster;

		it('should return the RpcMaster object if provided a correct configuration file', function() {
			rpcMaster = OpflowLoader.instance.createRpcMaster({
				configDir: path.join(__dirname, '../cfg'),
				configName: 'loader-rpc_master-test.conf',
				verbose: false
			});
		});

		afterEach(function(done) {
			rpcMaster && rpcMaster.close().then(lodash.ary(done, 0));
		});
	});

	describe('createRpcWorker() method:', function() {
		var rpcWorker;

		it('should return the RpcWorker object if provided a correct configuration file', function() {
			rpcWorker = OpflowLoader.instance.createRpcWorker({
				configDir: path.join(__dirname, '../cfg'),
				configName: 'loader-rpc_worker-test.conf',
				verbose: false
			});
		});

		afterEach(function(done) {
			rpcWorker && rpcWorker.close().then(lodash.ary(done, 0));
		});
	});

	describe('createRecycler() method:', function() {
		var recycler;

		it('should return the Recyclere object if provided a correct configuration file', function() {
			recycler = OpflowLoader.instance.createRecycler({
				configDir: path.join(__dirname, '../cfg'),
				configName: 'loader-recycler-test.conf',
				verbose: false
			});
		});

		afterEach(function(done) {
			recycler && recycler.close().then(lodash.ary(done, 0));
		});
	});
});