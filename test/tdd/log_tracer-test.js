'use strict';

var lodash = require('lodash');
var assert = require('chai').assert;
var expect = require('chai').expect;
var debugx = require('debug')('tdd:opflow:LogTracer');
var freshy = require('freshy');

var OpflowLogTracer = freshy.reload('../../lib/log_tracer');

describe('opflow.LogTracer:', function() {
	describe('libraryInfo:', function() {
		it('should return library information when get libraryInfo', function() {
			var libinfo = OpflowLogTracer.libraryInfo;
			assert.equal(libinfo.lib_name, 'opflow-nodejs');
			assert.property(libinfo, 'lib_version');
			assert.property(libinfo, 'os_name');
			assert.property(libinfo, 'os_version');
			assert.property(libinfo, 'os_arch');
		})
	});
	describe('branch() method:', function() {
		it('should create new child logTracer object', function() {
			process.env.OPFLOW_INSTANCE_ID = 'node1';

			var LT1 = OpflowLogTracer.ROOT;
			assert.deepEqual(JSON.parse(LT1.toString()), {
				"message": null,
				"instanceId": "node1"
			});

			var LT2 = LT1.branch({key: 'engineId', value: 'engine_123456'});
			var msg1 = '' + LT2;
			debugx.enabled && debugx('LT2-1: %s', msg1);
			assert.deepEqual(JSON.parse(msg1), {
				"message": null,
				"instanceId": "node1",
				"engineId": "engine_123456"
			});

			LT1.put("instanceId", "node2");
			assert.deepEqual(JSON.parse(LT1.toString()), {
				"message": null,
				"instanceId": "node2"
			});
			assert.deepEqual(JSON.parse(LT1.reset().toString()), {
				"message": null,
				"instanceId": "node1"
			});

			LT2.put('message', 'Message #2')
				.put('integer', 100)
				.put('float', 123.456);
			var msg2 = '' + LT2;
			debugx.enabled && debugx('LT2-2: %s', msg2);
			assert.deepEqual(JSON.parse(msg2), {
				"instanceId": "node1",
				"engineId": "engine_123456",
				"message": "Message #2",
				"integer": 100,
				"float": 123.456
			});
			
			LT2.reset()
				.put('boolean', true)
				.put('message', 'Message renew #2');
			var msg3 = '' + LT2;
			debugx.enabled && debugx('LT2-3: %s', msg3);
			assert.deepEqual(JSON.parse(msg3), {
				"instanceId": "node1",
				"engineId": "engine_123456",
				"boolean": true,
				"message": "Message renew #2"
			});
		});
	});

	describe('copy() method:', function() {
		it('should clone new separated logTracer object', function() {
			process.env.OPFLOW_INSTANCE_ID = 'node1';

			var LT1 = OpflowLogTracer.ROOT;
			assert.deepEqual(JSON.parse(LT1.toString()), {
				"message": null,
				"instanceId": "node1"
			});

			var LT2 = LT1.copy();
			var msg1 = '' + LT2;
			debugx.enabled && debugx('LT2-1: %s', msg1);
			assert.deepEqual(JSON.parse(msg1), {
				"message": null,
				"instanceId": "node1"
			});

			LT1.put("instanceId", "node2");
			assert.deepEqual(JSON.parse(LT1.toString()), {
				"message": null,
				"instanceId": "node2"
			});

			LT2.put('message', 'Message #2')
				.put('integer', 100)
				.put('float', 123.456);
			var msg2 = '' + LT2;
			debugx.enabled && debugx('LT2-2: %s', msg2);
			assert.deepEqual(JSON.parse(msg2), {
				"instanceId": "node1",
				"message": "Message #2",
				"integer": 100,
				"float": 123.456
			});
			
			LT2.reset()
				.put('boolean', true)
				.put('message', 'Message renew #2');
			var msg3 = '' + LT2;
			debugx.enabled && debugx('LT2-3: %s', msg3);
			assert.deepEqual(JSON.parse(msg3), {
				"instanceId": "node1",
				"boolean": true,
				"message": "Message renew #2"
			});
		});
	});
});