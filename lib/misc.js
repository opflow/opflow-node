'use strict';

var uuidV4 = require('uuid/v4');
var misc = {}

misc.getUUID = function() {
  return uuidV4();
}

module.exports = misc;
