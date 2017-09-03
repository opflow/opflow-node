module.exports = {
	opflow: {
		uri: 'amqp://master:zaq123edcx@192.168.56.56?frameMax=0x1000',
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