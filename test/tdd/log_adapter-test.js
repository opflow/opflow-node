'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var assert = require('chai').assert;
var expect = require('chai').expect;
var path = require('path');
var util = require('util');
var debugx = require('debug')('opflow:test:LogAdapter');
var misc = require('../../lib/util');
var LogAdapter = require('../../lib/log_adapter');
var MockLogger = require('../lab/mock-logger');

describe('opflow.LogAdapter:', function() {
	var consoleLog = process.env.OPFLOW_CONLOG;
	var logIdTreeEnabled = process.env.OPFLOW_LOGIDTREE;

	before(function() {
		delete process.env.OPFLOW_CONLOG;
		delete process.env.OPFLOW_LOGIDTREE;
	});

	it('should run in LogIdTreeEnabled mode', function() {
		var mock = (new MockLogger())._alter({ logLevel: 'info' });
		LogAdapter.connectTo(mock);
		var logger = LogAdapter.getLogger({
			logIdTreeEnabled: true
		});

		logger.log('trace', {'msg': 'This is trace level'});

		logger.log('debug', {'msg': 'This is debug level'});

		logger.isEnabledFor('info') && logger.log('info', {
			'instanceId': misc.instanceId, 
			'engineId': 'eef420ff-9eb7-474a-996a-f63b121100a8',
			'field1': 'Value 1',
			'field2': 'Value 2'
		});

		logger.isEnabledFor('info') && logger.log('info', {
			'engineId': 'eef420ff-9eb7-474a-996a-f63b121100a8',
			'consumerId': 'consumer#1'
		});

		logger.isEnabledFor('info') && logger.log('info', {
			'engineId': 'eef420ff-9eb7-474a-996a-f63b121100a8',
			'consumerId': 'consumer#2'
		});

		assert.equal(mock.messages.length, 4);
		assert.include(mock.messages[1][1], {
			'instanceId': misc.instanceId,
			'engineId': 'eef420ff-9eb7-474a-996a-f63b121100a8',
			'field1': 'Value 1',
			'field2': 'Value 2'
		});
		debugx.enabled && debugx('Log messages: %s', JSON.stringify(mock.messages, null, 2));
	});

	after(function() {
		consoleLog && (process.env.OPFLOW_CONLOG = consoleLog);
		logIdTreeEnabled && (process.env.OPFLOW_LOGIDTREE = logIdTreeEnabled);
	});
});
