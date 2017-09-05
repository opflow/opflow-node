'use strict';

var util = require('util');

/**
 * 1. invoke super constructor
 * 2. include stack trace in error object
 * 3. set our function's name as error name.
 */

function BootstrapError() {
	Error.apply(this, arguments);
	Error.captureStackTrace(this, this.constructor);
	this.name = this.constructor.name;
}
util.inherits(BootstrapError, Error);


function OperationError() {
	Error.apply(this, arguments);
	Error.captureStackTrace(this, this.constructor);
	this.name = this.constructor.name;
}
util.inherits(OperationError, Error);


function ParameterError() {
	Error.apply(this, arguments);
	Error.captureStackTrace(this, this.constructor);
	this.name = this.constructor.name;
}
util.inherits(ParameterError, Error);


module.exports = {
	BootstrapError,
	OperationError,
	ParameterError
};
