module.exports = {
  opflow: {
    serverlet: {
      uri: process.env.OPFLOW_TDD_URI || 'amqp://localhost',
      applicationId: 'FibonacciGenerator',
      configurer: {
        exchangeName: 'opflow-tdd-publisher',
        routingKey: 'opflow-tdd-configurer'
      },
      rpcWorker: {
        exchangeName: 'opflow-tdd-exchange',
        routingKey: 'opflow-tdd-rpc',
        operatorName: 'opflow-tdd-operator',
        responseName: 'opflow-tdd-response'
      },
      subscriber: {
        exchangeName: 'opflow-tdd-publisher',
        routingKey: 'opflow-tdd-pubsub-public',
        subscriberName: 'opflow-tdd-subscriber',
        recyclebinName: 'opflow-tdd-recyclebin',
        consumerTotal: 2,
        enabled: false
      }
    }
  }
}