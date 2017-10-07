# Testing

## Environment variables

* `OPFLOW_BASE64UUID`: Use Base64 format for UUID;
* `OPFLOW_CONLOG`: Enable console log (ie. debug module);
* `OPFLOW_LOGTRACER`: LogTracer level - enable LogTracer for this level;
* `OPFLOW_SELECTED_TEST`: list of test cases will be run;

Example:

```shell
export OPFLOW_BASE64UUID=true
export OPFLOW_LOGTRACER=conlog
export OPFLOW_CONLOG=true
export DEBUG=bdd*,opflow*
TDD_SELECTED="multiple workers" node_modules/.bin/mocha test/bdd/rpc-test.js
```

## TDD Environment Variables

```shell
export OPFLOW_TDD_URI=amqp://master:zaq123edcx@192.168.56.56
export OPFLOW_TDD_HOST=192.168.56.56
export OPFLOW_TDD_PORT=5672
export OPFLOW_TDD_USERNAME=master
export OPFLOW_TDD_PASSWORD=zaq123edcx
```
