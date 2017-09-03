module.exports = {
	opflow: {
		rpc_worker: {
			uri: 'amqp://master:zaq123edcx@192.168.56.56?frameMax=0x1000',
			exchangeName: 'tdd-opflow-exchange',
			routingKey: 'tdd-opflow-rpc',
			responseName: 'tdd-opflow-response',
			operatorName: 'tdd-opflow-operator'
		},
		verbose: false
	}
}