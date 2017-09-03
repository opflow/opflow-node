module.exports = {
	opflow: {
		uri: 'amqp://master:zaq123edcx@192.168.56.56?frameMax=0x1000',
		recycler: {
			subscriberName: 'tdd-opflow-subscriber',
			recyclebinName: 'tdd-opflow-recyclebin'
		},
		verbose: false
	}
}