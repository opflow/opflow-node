'use strict';

var misc = require('./util');

var OPFLOW_SAFEGUARD = process.env.OPFLOW_SAFEGUARD != 'false';

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
    __store['message'] = null;
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

  self.add = function(map) {
    if (map) {
      Object.keys(map).forEach(function(key) {
        __store[key] = map[key];
      });
    }
    return this;
  }

  self.toString = function(opts) {
    var json = null;
    if (OPFLOW_SAFEGUARD) {
      try {
        json = JSON.stringify(__store);
      } catch (error) {
        json = JSON.stringify({ safeguard: 'JSON.stringify() error' });
      }
    } else {
      json = JSON.stringify(__store);
    }
    opts && opts.reset && self.reset();
    return json;
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

