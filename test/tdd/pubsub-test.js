'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var assert = require('chai').assert;
var expect = require('chai').expect;
var faker = require('faker');
var util = require('util');
var debugx = require('debug')('opflow:pubsub:test');
var Pubsub = require('../../lib/pubsub');
var appCfg = require('../lab/app-configuration');
var bogen = require('../lab/big-object-generator');
var Loadsync = require('loadsync');

describe('opflow:', function() {

	describe('pubsub:', function() {
		var messageTotal = 5;
		var total = 4;
		var publisher;
		var subscribers;
		var loadsync;

		before(function() {
			subscribers = lodash.range(total).map(function(i) {
				return new Pubsub({
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
			publisher = new Pubsub({
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
			loadsync = new Loadsync([{
				name: 'testpubsub',
				cards: lodash.range(total)
			}]);
			var results = lodash.map(lodash.range(subscribers.length), function() {return new Array()});
			subscribers.forEach(function(subscriber, i) {
				subscriber.subscribe(function(content, info) {
					results[i].push(JSON.parse(content.toString()));
					if (results[i].length >= messageTotal) {
						loadsync.check(i, 'testpubsub');
					}
				});
			});
			Promise.mapSeries(lodash.range(messageTotal), function(i) {
				return publisher.publish({ count: i, type: 'public'});
			});
			loadsync.ready(lodash.ary(done, 0), 'testpubsub');
			this.timeout(500*total);
		});
	});
});
