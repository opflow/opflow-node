'use strict';

var Fibonacci = function Fibonacci(P) {
	var n = P && P.number && P.number >= 0 ? P.number : null;
	var c = 0;
	var f = 0, f_1 = 0, f_2 = 0;

	this.next = function() {
		if (c >= n) return false;
		if (++c < 2) {
			f = c;
		} else {
			f_2 = f_1; f_1 = f; f = f_1 + f_2;
		}
		return true;
	}

	this.result = function() {
		return { value: f, step: c, number: n };
	}
}

var fibonacci = function fibonacci(n) {
	if (n == 0 || n == 1) return n;
	else return fibonacci(n - 1) + fibonacci(n - 2);
}

module.exports = { Fibonacci, fibonacci }