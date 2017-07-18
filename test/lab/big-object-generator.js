'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var util = require('util');
var faker = require('faker');

var helper = {
	FIELDS: parseInt(process.env.BOG_FIELDS),
	TOTAL: parseInt(process.env.BOG_TOTAL),
	TIMEOUT: parseInt(process.env.BOG_TIMEOUT)
};

var BigObjectGenerator = helper.BigObjectGenerator = function(params) {
	params = params || {};
	this.index = params.min || 0;
	this.total = params.max || 1000;
	this.fields = lodash.range(params.numberOfFields || 10).map(function(index) {
		return {
			name: 'field_' + index,
			type: 'string'
		}
	});
	this.next = function() {
		if (this.index >= this.total) return Promise.resolve(null);
		var obj = {};
		this.fields.forEach(function(field) {
			obj[field.name] = faker.lorem.sentence();
		});
		obj.code = this.index;
		this.index++;
		return Promise.resolve(obj).delay(params.timeout || 0);
	}
}

module.exports = helper;