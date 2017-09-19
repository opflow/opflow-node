'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var assert = require('chai').assert;
var expect = require('chai').expect;
var path = require('path');
var util = require('util');
var debugx = require('debug')('tdd:opflow:LogTracer');
var OpflowLogTracer = require('../../lib/log_tracer');
var Loadsync = require('loadsync');

describe('opflow.LogTracer:', function() {
	describe('branch() method:', function() {
		it('should create new child logTracer object', function() {
			var LT1 = new OpflowLogTracer({ key: 'systemId', value: 'node1' });
			assert.deepEqual(JSON.parse(LT1.toString()), {
				"systemId": "node1"
			});

			var LT2 = LT1.branch({key: 'engineId', value: 'engine_123456'});
			var msg1 = '' + LT2;
			debugx.enabled && debugx('LT2-1: %s', msg1);
			assert.deepEqual(JSON.parse(msg1), {
				"systemId": "node1",
				"engineId": "engine_123456"
			});

			LT1.put("systemId", "node2");
			assert.deepEqual(JSON.parse(LT1.toString()), {
				"systemId": "node2"
			});
			assert.deepEqual(JSON.parse(LT1.reset().toString()), {
				"systemId": "node1"
			});

			LT2.put('message', 'Message #2')
				.put('integer', 100)
				.put('float', 123.456);
			var msg2 = '' + LT2;
			debugx.enabled && debugx('LT2-2: %s', msg2);
			assert.deepEqual(JSON.parse(msg2), {
				"systemId": "node1",
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
				"systemId": "node1",
				"engineId": "engine_123456",
				"boolean": true,
				"message": "Message renew #2"
			});
		});
	});

	describe('copy() method:', function() {
		it('should clone new separated logTracer object', function() {
			var LT1 = new OpflowLogTracer({ key: 'systemId', value: 'node1' });
			assert.deepEqual(JSON.parse(LT1.toString()), {
				"systemId": "node1"
			});

			var LT2 = LT1.copy();
			var msg1 = '' + LT2;
			debugx.enabled && debugx('LT2-1: %s', msg1);
			assert.deepEqual(JSON.parse(msg1), {
				"systemId": "node1"
			});

			LT1.put("systemId", "node2");
			assert.deepEqual(JSON.parse(LT1.toString()), {
				"systemId": "node2"
			});

			LT2.put('message', 'Message #2')
				.put('integer', 100)
				.put('float', 123.456);
			var msg2 = '' + LT2;
			debugx.enabled && debugx('LT2-2: %s', msg2);
			assert.deepEqual(JSON.parse(msg2), {
				"systemId": "node1",
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
				"systemId": "node1",
				"boolean": true,
				"message": "Message renew #2"
			});
		});
	});
});