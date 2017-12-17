
const Spredis = require('../Spredis');
const _ = require('lodash');

class Commander {

	constructor() {
		this.spredis = Spredis.getInstance();
		this.ns = this.spredis.defaultNamespace;
	}

	use(namespace) {
		if (!namespace) {
			this.ns = this.spredis.defaultNamespace;
			return;
		}
		this.ns = this.spredis.useNamespace(namespace);
	}

	async namespaceConfig(query, callback) {
		try {
			var res = await this.ns.getNamespaceConfig();
			callback(null, res);	
		} catch(e) {
			callback(e);
		}
	}

	async indexDocuments(query, callback) {
		try {
			var res = await this.ns.indexDocuments(query);
			callback(null, res);	
		} catch(e) {
			callback(e);
		}
	}

	async getDocument(id, callback) {
		try {
			var res = await this.ns.getDocument(id);
			callback(null, res);	
		} catch(e) {
			callback(e);
		}
		
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
		this.use(command.ns);
		if (!this.ns) {
			callback(new Error('Could not find namespace'))
		}
		let action = this[command.action];
		if (!(action instanceof Function)) {
			callback(new Error(`Unknown or unsupported command "${command.action}"`))
		}
		this[command.action](command.input, callback)
	} 
}

module.exports = Commander;