'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var assert = require('chai').assert;
var expect = require('chai').expect;
var util = require('util');
var debugx = require('debug')('opflow:engine:test');
var PubsubHandler = require('../../lib/pubsub');
var Recycler = require('../../lib/recycler');
var appCfg = require('../lab/app-configuration');
var bogen = require('../lab/big-object-generator');
var Loadsync = require('loadsync');

describe('opflow-engine:', function() {

	describe('recycle() method:', function() {
		var handler;
		var recycler;

		before(function() {
			handler = new PubsubHandler(appCfg.extend({
				exchangeName: 'tdd-opflow-publisher',
				routingKey: 'tdd-opflow-recycle',
				subscriberName: 'tdd-opflow-subscriber',
				recyclebinName: 'tdd-opflow-recyclebin',
				redeliveredLimit: 3
			}));
			recycler = new Recycler(appCfg.extend({
				subscriberName: 'tdd-opflow-subscriber',
				recyclebinName: 'tdd-opflow-recyclebin'
			}));
		});

		beforeEach(function(done) {
			Promise.all([
				handler.ready(),
				recycler.purgeSubscriber(),
				recycler.purgeRecyclebin()
			]).then(lodash.ary(done, 0));
		})

		afterEach(function(done) {
			Promise.all([
				handler.close(),
				recycler.close()
			]).then(lodash.ary(done, 0));
		})

		it('filter the failed consumeing data to trash (recycle-bin)', function(done) {
			var total = 1000;
			var index = 0;
			var codes = [11, 21, 31, 41, 51, 61, 71, 81, 91, 99];
			var hasDone = 0;
			var ok = handler.subscribe(function(body, headers, finish) {
				body = JSON.parse(body);
				finish(codes.indexOf(body.code) < 0 ? undefined : 'error');
				if (++index >= (total + 3*codes.length)) {
					recycler.checkSubscriber().then(function(info) {
						assert.equal(info.messageCount, 0, 'Chain should be empty');
						(hasDone++ === 0) && done();
					});
				}
			});
			ok.then(function() {
				Promise.mapSeries(lodash.range(total), function(count) {
					return handler.publish({ code: count, msg: 'Hello world' });
				});
			})
			this.timeout(50*total);
		});

		it('assure the total of recovered items in trash (recycle-bin)', function(done) {
			var total = 1000;
			var index = 0;
			var loadsync = new Loadsync([{
				name: 'testsync',
				cards: ['consume', 'recycle']
			}]);

			var code1 = [11, 21, 31, 41, 51, 61, 71, 81, 91, 99];
			var ok1 = handler.subscribe(function(body, info, finish) {
				body = JSON.parse(body);
				finish(code1.indexOf(body.code) < 0 ? undefined : 'error');
				if (++index >= (total + 3*code1.length)) {
					recycler.checkSubscriber().then(function(info) {
						assert.equal(info.messageCount, 0, 'Chain should be empty');
						loadsync.check('consume', 'testsync');
					});
				}
			});

			var code2 = [];
			var ok2 = recycler.recycle(function(body, info, finish) {
				body = JSON.parse(body);
				code2.push(body.code);
				if (code2.length >= code1.length) {
					recycler.checkRecyclebin().then(function(info) {
						assert.equal(info.messageCount, 0, 'Trash should be empty');
						loadsync.check('recycle', 'testsync');
					});
				}
				finish();
			});

			loadsync.ready(function(info) {
				assert.sameMembers(code1, code2, 'There are exactly ' + code1.length + ' failed items');
				setTimeout(done, 100);
			}, 'testsync');

			Promise.all([ok1, ok2]).then(function() {
				Promise.mapSeries(lodash.range(total), function(count) {
					return handler.publish({ code: count, msg: 'Hello world' });
				});
			});
			this.timeout(5*total);
		});
	});

	describe('garbage recovery:', function() {
		var handler;
		var recycler;

		before(function() {
			handler = new PubsubHandler(appCfg.extend({
				exchangeName: 'tdd-opflow-publisher',
				routingKey: 'tdd-opflow-recycle',
				subscriberName: 'tdd-opflow-subscriber',
				recyclebinName: 'tdd-opflow-recyclebin',
				redeliveredLimit: 3
			}));
			recycler = new Recycler(appCfg.extend({
				subscriberName: 'tdd-opflow-subscriber',
				recyclebinName: 'tdd-opflow-recyclebin'
			}));
		});

		beforeEach(function(done) {
			Promise.all([
				handler.ready(),
				recycler.purgeSubscriber(),
				recycler.purgeRecyclebin()
			]).then(lodash.ary(done, 0));
		});

		afterEach(function(done) {
			Promise.all([
				handler.close(),
				recycler.close()
			]).then(lodash.ary(done, 0));
		});

		it('examine garbage items in trash (recycle-bin)', function(done) {
			var total = 1000;
			var index = 0;
			var loadsync = new Loadsync([{
				name: 'testsync',
				cards: ['consume']
			}]);

			var codes = [11, 21, 31, 41, 51, 61, 71, 81, 91, 99];
			handler.subscribe(function(body, headers, finish) {
				body = JSON.parse(body);
				finish(codes.indexOf(body.code) < 0 ? undefined : 'error');
				++index;
				if (index == (total + 3*codes.length)) {
					recycler.checkSubscriber().then(function(info) {
						assert.equal(info.messageCount, 0, 'Chain should be empty');
						loadsync.check('consume', 'testsync');
					});
				}
				if (index > (total + 3*codes.length)) {
					debugx.enabled && debugx('Recovery message: %s', JSON.stringify(body));
					assert.equal(body.code, total);
					recycler.checkSubscriber().then(function(info) {
						assert.equal(info.messageCount, 0, 'Chain should be empty');
					});
				}
			}).then(function() {
				return Promise.mapSeries(lodash.range(total), function(count) {
					return handler.publish({ code: count, msg: 'Hello world' });
				});
			});

			loadsync.ready(function(info) {
				var msgcode;
				Promise.resolve().delay(50 * codes.length).then(function() {
					return recycler.checkRecyclebin().then(function(info) {
						debugx.enabled && debugx('Trash info: %s', JSON.stringify(info));
						assert.equal(info.messageCount, (codes.length-0), 'Trash should has ' + (codes.length-0) + ' items');
						return true;
					})
				}).then(function() {
					return recycler.examine(function(msg, update) {
						var message = JSON.parse(msg.content.toString());
						debugx.enabled && debugx('Garbage message: %s', JSON.stringify(message));
						msgcode = message.code;
						assert.isTrue(codes.indexOf(msgcode) >= 0);
						assert.equal(codes[0], msgcode);
						update('nop');
					});
				}).then(function() {
					return recycler.checkRecyclebin().then(function(info) {
						debugx.enabled && debugx('Trash info: %s', JSON.stringify(info));
						assert.equal(info.messageCount, (codes.length-1), 'Trash should has ' + (codes.length-1) + ' items');
						return true;
					})
				}).then(function() {
					return recycler.examine(function(msg, update) {
						var message = JSON.parse(msg.content.toString());
						debugx.enabled && debugx('Garbage message: %s', JSON.stringify(message));
						assert.equal(message.code, msgcode);
						update('discard');
					});
				}).then(function() {
					return recycler.examine(function(msg, update) {
						update('discard');
					});
				}).then(function() {
					return recycler.checkRecyclebin().then(function(info) {
						debugx.enabled && debugx('Trash info: %s', JSON.stringify(info));
						assert.equal(info.messageCount, (codes.length-2), 'Trash should has ' + (codes.length-2) + ' items');
						return true;
					})
				}).then(function() {
					return recycler.examine(function(msg, update) {
						var message = JSON.parse(msg.content.toString());
						debugx.enabled && debugx('Garbage message: %s', JSON.stringify(message));
						assert.equal(message.code, codes[2]);
						message.msg = 'I will survive';
						update('restore', {
							content: JSON.stringify(message)
						});
					});
				}).then(function() {
					return recycler.checkRecyclebin().then(function(info) {
						debugx.enabled && debugx('Trash info: %s', JSON.stringify(info));
						assert.equal(info.messageCount, (codes.length-2), 'Trash should has ' + (codes.length-2) + ' items');
						return true;
					})
				}).then(function() {
					return recycler.examine(function(msg, update) {
						var message = JSON.parse(msg.content.toString());
						debugx.enabled && debugx('Garbage message: %s', JSON.stringify(message));
						assert.equal(message.code, codes[2]);
						message.code = total;
						update('recover', {
							content: JSON.stringify(message)
						});
					});
				}).then(function() {
					return recycler.checkRecyclebin().then(function(info) {
						debugx.enabled && debugx('Trash info: %s', JSON.stringify(info));
						assert.equal(info.messageCount, (codes.length-3), 'Trash should has ' + (codes.length-3) + ' items');
						return true;
					})
				}).then(function() {
					return recycler.examine(function(msg, update) {
						var message = JSON.parse(msg.content.toString());
						debugx.enabled && debugx('Garbage message: %s', JSON.stringify(message));
						assert.equal(message.code, codes[3]);
						message.code = total+1;
						update('requeue', {
							content: JSON.stringify(message)
						});
					});
				}).then(function() {
					return recycler.checkRecyclebin().then(function(info) {
						debugx.enabled && debugx('Trash info: %s', JSON.stringify(info));
						assert.equal(info.messageCount, (codes.length-3), 'Trash should has ' + (codes.length-3) + ' items');
						return true;
					})
				}).then(function() {
					return Promise.mapSeries(lodash.range(4, 10), function(count) {
						return recycler.examine(function(msg, update) {
							var message = JSON.parse(msg.content.toString());
							debugx.enabled && debugx('Garbage message: %s', JSON.stringify(message));
							assert.equal(message.code, codes[count]);
							update('discard');
						});
					});
				}).then(function() {
					return recycler.checkRecyclebin().then(function(info) {
						debugx.enabled && debugx('Trash info: %s', JSON.stringify(info));
						assert.equal(info.messageCount, (1), 'Trash should has ' + (1) + ' items');
						return true;
					})
				}).then(function() {
					return recycler.examine(function(msg, update) {
						var message = JSON.parse(msg.content.toString());
						debugx.enabled && debugx('Garbage message: %s', JSON.stringify(message));
						assert.equal(message.code, total + 1);
						update('nop');
					});
				}).then(function() {
					done();
				});
			}, 'testsync');

			this.timeout(100*total);
		});
	});
});
