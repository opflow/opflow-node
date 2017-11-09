'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var assert = require('chai').assert;
var expect = require('chai').expect;
var debugx = require('debug')('tdd:opflow:util');
var misc = require('../../lib/util');
var LogAdapter = require('logolite').LogAdapter;

describe('opflow.util:', function() {
	this.timeout(1000 * 60 * 60);

	it('should build default URI from empty parameters', function() {
		var args = misc.buildURI();
		assert.equal(args.uri, 'amqp://localhost');
		assert.equal(args.host, 'localhost');
		debugx.enabled && debugx('conargs (default) %s', JSON.stringify(args));
	});

	it('should build full URI from completed parameters', function() {
		var args = misc.buildURI({
			host: 'broker.example.com',
			port: 5670,
			username: 'nowhere',
			password: 'happy',
			virtualHost: 'opflow',
			channelMax: 10,
			frameMax: '0x1000',
			heartbeat: 30,
			locale: 'vi_VN'
		});
		assert.equal(args.uri, "amqp://nowhere:happy@broker.example.com:5670" +
			"/opflow?channelMax=10&frameMax=0x1000&heartbeat=30&locale=vi_VN");
		debugx.enabled && debugx('conargs (full) %s', JSON.stringify(args));
	});
});
