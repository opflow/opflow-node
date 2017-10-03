module.exports = {
	opflow: {
		rpc_master: {
			uri: process.env.OPFLOW_TDD_URI || 'amqp://localhost',
			exchangeName: 'tdd-opflow-exchange',
			routingKey: 'tdd-opflow-rpc',
			responseName: 'tdd-opflow-response'
		},
		verbose: false
	}
}