'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var assert = require('chai').assert;
var expect = require('chai').expect;
var faker = require('faker');
var util = require('util');
var debugx = require('debug')('opflow:engine:test');
var OpflowEngine = require('../../lib/engine');
var appCfg = require('../lab/app-configuration');
var bogen = require('../lab/big-object-generator');
var Loadsync = require('loadsync');

describe('opflow-engine:', function() {

	describe('feedback:', function() {
		var handler;

		before(function() {
			handler = new OpflowEngine(appCfg.extend({
				feedback: {
					queueName: 'tdd-opflow-feedback',
					durable: true,
					noAck: false
				}
			}));
		});

		beforeEach(function(done) {
			Promise.all([
				handler.ready(), handler.purgeChain(), handler.purgeFeedback()
			]).then(lodash.ary(done, 0));
		});

		afterEach(function(done) {
			handler.destroy().then(lodash.ary(done, 0));
		});

		it('Result of consume() should be send to Feedback queue', function(done) {
			var total = 1000;
			var count = 0;
			var index = 0;
			var codes = [11, 21, 31, 41, 51, 61, 71, 81, 91, 99];
			var hasDone = 0;
			var ok = handler.consume(function(message, info, finish) {
				message = JSON.parse(message);
				message.feedback = 'feedback_' + message.code;
				if (codes.indexOf(message.code) < 0) {
					finish(undefined, message);
				} else {
					finish({ error_code: message.code });
				}
			});
			ok.then(function() {
				return handler.pullout(function(message, info, finish) {
					var headers = info.properties && info.properties.headers;
					message = JSON.parse(message);
					if (headers.error === false) {
						assert.equal(message.feedback, 'feedback_' + message.code);
					} else {
						count++;
						assert.isTrue(codes.indexOf(message.error_code) >= 0);
					}
					if (++index >= total) {
						assert.equal(count, codes.length);
						handler.checkChain().then(function(info) {
							assert.equal(info.messageCount, 0, 'Chain should be empty');
							(hasDone++ === 0) && done();
						});
					}
				});
			})
			ok.then(function() {
				Promise.mapSeries(lodash.range(total), function(count) {
					return handler.produce({ code: count, msg: 'Hello world' });
				});
			})
			this.timeout(5*total);
		});
	});
});
