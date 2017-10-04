var lodash = require('lodash');
var util = require('util');

var baseCfg = {
	uri: process.env.OPFLOW_TDD_URI || 'amqp://localhost',
	exchangeName: 'tdd-opflow-exchange',
	exchangeQuota: 3,
	routingKey: 'tdd-opflow-defaultkey',
	delayTime: 0
};

module.exports = {
	extend: function(ext) {
		ext = ext || {};
		return lodash.merge({}, baseCfg, ext);
	},
	timeout: function(ms) {
		return ms ? lodash.max([ms, 600000]) : 600000;
	},
	checkSkip: function() {
		var testTitle = this.currentTest.title || '';
		var selected = process.env.TDD_SELECTED;
		var selecteds = selected ? selected.split(',') : [];
		var skipped = selecteds.length > 0;
		for(var i=0; i<selecteds.length; i++) {
			if (selecteds[i].length > 0 && testTitle.indexOf(selecteds[i]) >= 0) {
				skipped = false;
				break;
			}
		}
		skipped && this.skip();
	}
};
