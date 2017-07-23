var lodash = require('lodash');

var baseCfg = {
	uri: 'amqp://master:zaq123edcx@192.168.56.56?frameMax=0x1000',
	exchangeType: 'direct',
	exchangeName: 'tdd-opflow-exchange',
	routingKey: 'tdd-opflow-defaultkey',
	delayTime: 0,
	consumer: {
		queueName: 'tdd-opflow-queue',
		durable: true,
		noAck: false,
		binding: true
	}
};

module.exports = {
	extend: function(ext) {
		ext = ext || {};
		return lodash.merge({}, baseCfg, ext);
	},
	master: function(ext) {

	},
	worker: function(ext) {
		
	}
};
