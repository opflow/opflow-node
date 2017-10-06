module.exports = {
  opflow: {
    commander: {
      uri: process.env.OPFLOW_TDD_URI || 'amqp://localhost',
      applicationId: 'FibonacciGenerator',
      configurer: {
        exchangeName: 'opflow-tdd-publisher',
        routingKey: 'opflow-tdd-configurer'
      },
      rpcMaster: {
        exchangeName: 'opflow-tdd-exchange',
        routingKey: 'opflow-tdd-rpc'
      },
      publisher: {
        exchangeName: 'opflow-tdd-publisher',
        routingKey: 'opflow-tdd-pubsub-public',
        enabled: false
      }
    }
  }
}