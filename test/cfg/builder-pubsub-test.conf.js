module.exports = {
	opflow: {
		uri: process.env.OPFLOW_TDD_URI || 'amqp://localhost',
		exchangeName: 'tdd-opflow-exchange',
		routingKey: 'tdd-opflow-defaultkey',
		pubsub: {
			exchangeName: 'tdd-opflow-publisher',
			routingKey: 'tdd-opflow-pubsub-public',
			otherKeys: ['tdd-opflow-pubsub-private'],
			subscriberName: 'tdd-opflow-subscriber',
			recyclebinName: 'tdd-opflow-recyclebin'
		}
	}
}