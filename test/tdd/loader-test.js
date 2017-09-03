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
				basename: path.join(__dirname, 'opflow.conf'),
				default: {
					default1: 'value 1',
					default2: 'value 2'
				}
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
});