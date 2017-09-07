'use strict';

var Promise = require('bluebird');
var lodash = require('lodash');
var assert = require('chai').assert;
var expect = require('chai').expect;
var debugx = require('debug')('opflow:util:test');
var misc = require('../../lib/util');

describe('opflow-util:', function() {
	it('should return library information when call getLibraryInfo', function() {
		var libinfo = misc.getLibraryInfo();
		assert.equal(libinfo.name, 'opflow-nodejs');
		assert.property(libinfo, 'version');
		assert.property(libinfo, 'os');
	})
});
