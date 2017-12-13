
const Spredis = require('../Spredis');

class Commander {

	constructor() {
		this.ns = Spredis.getInstance().defaultNamespace;
	}

	use(namespace, callback) {

	}

	async search(query, callback) {
		try {
			var res = await this.ns.search(query);
			callback(null, res);	
		} catch(e) {
			callback(e);
		}
		
	}
	exec(command, callback) {
		this.search(command.input, callback)
	} 
}

module.exports = Commander;