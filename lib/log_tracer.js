'use strict';

var misc = require('./util');

var LogTracer = function(params) {
  params = params || {
    key: 'instanceId',
    value: process.env.OPFLOW_INSTANCE_ID || misc.instanceId
  };
  var self = this;

  var __parent = params.parent;
  var __key = params.key;
  var __value = params.value;
  var __store = {};

  Object.defineProperties(this, {
    'parent': {
      get: function() { return __parent },
      set: function(val) {}
    },
    'key': {
      get: function() { return __key },
      set: function(val) {}
    },
    'value': {
      get: function() { return __value },
      set: function(val) {}
    }
  });

  self.reset = function(level) {
    __store = __clearMap(__store);
    level = level || 2;
    if (level > 0) {
      if (level == 1) {
        if (self.parent) {
          __store[self.parent.key] = self.parent.value;
        }
      } else {
        var ref = self.parent;
        while(ref) {
          __store[ref.key] = ref.value;
          ref = ref.parent;
        }
      }
    }
    __store[__key] = __value;
    return this;
  }

  self.branch = function(origin) {
    return new LogTracer({parent: this, key: origin.key, value: origin.value});
  }

  self.copy = function() {
    return new LogTracer(this);
  }

  self.get = function(key) {
    return __store[key];
  }

  self.put = function(key, value) {
    if (key && value) __store[key] = value;
    return this;
  }

  self.toString = function() {
    return JSON.stringify(__store);
  }

  var __clearMap = function(map) {
    Object.keys(map).forEach(function(key) {
      delete map[key];
    });
    return map;
  }

  self.reset();
}

var ROOT = null;

Object.defineProperty(LogTracer, 'ROOT', {
  get: function() { return ROOT = ROOT || new LogTracer() },
  set: function(val) {}
});

module.exports = LogTracer;

