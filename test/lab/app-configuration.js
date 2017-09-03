var lodash = require('lodash');

var baseCfg = {
	uri: process.env.OPFLOW_TDD_URI || 'amqp://localhost',
	exchangeName: 'tdd-opflow-exchange',
	routingKey: 'tdd-opflow-defaultkey',
	delayTime: 0
};

module.exports = {
	extend: function(ext) {
		ext = ext || {};
		return lodash.merge({}, baseCfg, ext);
	}
};
