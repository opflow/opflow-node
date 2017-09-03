module.exports = {
	opflow: {
		uri: process.env.OPFLOW_TDD_URI || 'amqp://localhost',
		recycler: {
			subscriberName: 'tdd-opflow-subscriber',
			recyclebinName: 'tdd-opflow-recyclebin'
		},
		verbose: false
	}
}