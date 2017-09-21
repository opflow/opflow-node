'use strict';

var lodash = require('lodash');
var fs = require('fs');
var os = require('os');
var misc = require('./util');

var OPFLOW_SAFEGUARD = process.env.OPFLOW_SAFEGUARD != 'false';

var LogTracer = function(params) {
  params = params || {
    key: 'instanceId',
    value: process.env.OPFLOW_INSTANCE_ID || misc.getUUID()
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
    if (INTERCEPTOR_ENABLED) {
      var cloned = lodash.clone(__store);
      _interceptors.forEach(function(interceptor) {
        interceptor(cloned, __key, __value, __parent, opts);
      });
    }
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

var INTERCEPTOR_ENABLED = process.env.OPFLOW_DEBUGLOG != 'false' && (
    process.env.OPFLOW_CONLOG || process.env.NODE_ENV == 'test');

var _interceptors = [];

Object.defineProperties(LogTracer, {
  'addStringifyInterceptor': {
    get: function() {
      return function(f) {
        if (typeof(f) === 'function' && _interceptors.indexOf(f) < 0) {
          _interceptors.push(f);
          return true;
        }
        return false;
      }
    },
    set: function(value) {}
  },
  'removeStringifyInterceptor': {
    get: function() {
      return function(f) {
        var pos = _interceptors.indexOf(f);
        if (pos >= 0) {
          _interceptors.splice(pos, 1);
          return true;
        }
        return false;
      }
    },
    set: function(value) {}
  },
  'stringifyInterceptorCount': {
    get: function() {
      return function() {
        return _interceptors.length;
      }
    },
    set: function(value) {}
  }
});

var loadPackageInfo = function() {
  return JSON.parse(fs.readFileSync(__dirname + '/../package.json', 'utf8'));
}

var libraryInfo = null;
var libraryInfoString = null;

Object.defineProperties(LogTracer, {
  'libraryInfo': {
    get: function() {
      return libraryInfo = libraryInfo || {
        message: 'Opflow Library Information',
        lib_name: 'opflow-nodejs',
        lib_version: loadPackageInfo().version,
        os_name: os.platform(),
        os_version: os.release(),
        os_arch: os.arch()
      }
    },
    set: function(value) {}
  },
  'libraryInfoString': {
    get: function() {
      return libraryInfoString = libraryInfoString || 
          new LogTracer().add(LogTracer.libraryInfo).toString();
    },
    set: function(value) {}
  }
});

var ROOT = null;

Object.defineProperty(LogTracer, 'ROOT', {
  get: function() { return ROOT = ROOT || new LogTracer() },
  set: function(val) {}
});

module.exports = LogTracer;
