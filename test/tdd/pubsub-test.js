'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var assert = require('chai').assert;
var expect = require('chai').expect;
var faker = require('faker');
var util = require('util');
var debugx = require('debug')('opflow:pubsub:test');
var PubsubHandler = require('../../lib/pubsub');
var appCfg = require('../lab/app-configuration');
var bogen = require('../lab/big-object-generator');
var Loadsync = require('loadsync');

describe('opflow:', function() {

	describe('pubsub:', function() {
		var total = 4;
		var publisher;
		var subscribers;
		var loadsync;

		before(function() {
			subscribers = lodash.range(total).map(function(i) {
				return new PubsubHandler({
					uri: 'amqp://master:zaq123edcx@192.168.56.56?frameMax=0x1000',
					exchangeName: 'tdd-opflow-publisher',
					routingKey: 'tdd-opflow-pubsub-public',
					consumer: {
						queueName: 'tdd-opflow-subscriber#' + i,
						routingKeys: ['tdd-opflow-pubsub-group#' + (i - Math.floor(i/2)*2), 'tdd-opflow-pubsub-private#' + i],
						durable: true,
						noAck: false
					}
				});
			});
			publisher = new PubsubHandler({
				uri: 'amqp://master:zaq123edcx@192.168.56.56?frameMax=0x1000',
				exchangeName: 'tdd-opflow-publisher',
				routingKey: 'tdd-opflow-pubsub-public'
			});
		});

		beforeEach(function(done) {
			Promise.all(lodash.flatten(lodash.map(subscribers, function(subscriber) {
				return [ subscriber.ready(), subscriber.purge() ];
			}))).then(lodash.ary(done, 0));
		});

		afterEach(function(done) {
			Promise.all(lodash.map(subscribers, function(subscriber) {
				return subscriber.destroy();
			})).then(lodash.ary(done, 0));
		});

		it('Publish to public channel, all of subscribers should receive message', function(done) {
			var messageTotal = 5;
			var results = lodash.map(lodash.range(subscribers.length), function() {return new Array()});
			loadsync = new Loadsync([{
				name: 'testpubsub',
				cards: lodash.range(total)
			}]);
			loadsync.ready(function(info) {
				lodash.range(subscribers.length).forEach(function(i) {
					assert.sameDeepMembers(results[i], lodash.range(messageTotal).map(function(i) {
						return { count: i, type: 'public' }
					}));
				});
				done();
			}, 'testpubsub');
			Promise.resolve().then(function() {
				return Promise.map(subscribers, function(subscriber, i) {
					return subscriber.subscribe(function(content, info) {
						content = JSON.parse(content.toString());
						if (content.type === 'end') {
							loadsync.check(i, 'testpubsub');
						} else {
							results[i].push(content);
						}
					});
				})
			}).then(function() {
				return Promise.mapSeries(lodash.range(messageTotal), function(i) {
					return publisher.publish({ count: i, type: 'public'});
				});
			}).then(function() {
				return publisher.publish({ type: 'end' });
			});
			this.timeout(500*total);
		});

		it('Publish to specific channel, only corresponding subscribers will receive messages', function(done) {
			var messageTotal = 5;
			var groupTotal = 3;
			var privateTotal = 2;
			var results = lodash.map(lodash.range(subscribers.length), function() {return new Array()});
			loadsync = new Loadsync([{
				name: 'testpubsub',
				cards: lodash.range(total)
			}]);
			loadsync.ready(function(info) {
				assert.equal(results[0].length, messageTotal);
				assert.equal(results[1].length, messageTotal + groupTotal + privateTotal);
				assert.equal(results[2].length, messageTotal);
				assert.equal(results[3].length, messageTotal + groupTotal);
				lodash.range(subscribers.length).forEach(function(i) {
					assert.includeDeepMembers(results[i], lodash.range(messageTotal).map(function(i) {
						return { count: i, type: 'public' }
					}));
				});
				[1, 3].forEach(function(i) {
					assert.includeDeepMembers(results[i], lodash.range(groupTotal).map(function(i) {
						return { count: i, type: 'group' }
					}));
				});
				[1].forEach(function(i) {
					assert.includeDeepMembers(results[i], lodash.range(privateTotal).map(function(i) {
						return { count: i, type: 'private' }
					}));
				});
				done();
			}, 'testpubsub');
			Promise.resolve().then(function() {
				return Promise.map(subscribers, function(subscriber, i) {
					return subscriber.subscribe(function(content, info) {
						content = JSON.parse(content.toString());
						if (content.type === 'end') {
							loadsync.check(i, 'testpubsub');
						} else {
							results[i].push(content);
						}
					});
				})
			}).then(function() {
				return Promise.mapSeries(lodash.range(messageTotal), function(i) {
					return publisher.publish({ count: i, type: 'public'}, 'tdd-opflow-pubsub-public');
				});
			}).then(function() {
				return Promise.mapSeries(lodash.range(groupTotal), function(i) {
					return publisher.publish({ count: i, type: 'group'}, 'tdd-opflow-pubsub-group#1');
				});
			}).then(function() {
				return Promise.mapSeries(lodash.range(privateTotal), function(i) {
					return publisher.publish({ count: i, type: 'private'}, 'tdd-opflow-pubsub-private#1');
				});
			}).then(function() {
				return publisher.publish({ type: 'end' });
			});
			this.timeout(500*total);
		});
	});
});
