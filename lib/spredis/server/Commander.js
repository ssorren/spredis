
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
			return callback(null, res);	
		} catch(e) {
			console.error(e.stack);
			return callback(e);
		}
	}

	async addDocuments(docs, callback) {
		try {
			var res = await this.ns.addDocuments(docs);
			return callback(null, res);	
		} catch(e) {
			console.error(e.stack);
			return callback(e);
		}
	}

	async deleteDocuments(docs, callback) {
		try {
			var res = await this.ns.deleteDocuments(docs);
			return callback(null, res);	
		} catch(e) {
			console.error(e.stack);
			return callback(e);
		}
	}

	async getDocument(id, callback) {
		try {
			// console.log('getting id', id)
			var res = await this.ns.getDocument(id);
			return callback(null, res);	
		} catch(e) {
			console.error(e.stack);
			return callback(e);
		}
		
	}

	async search(query, callback) {
		try {
			var res = await this.ns.search(query);
			return callback(null, res);	
		} catch(e) {
			console.error(e.stack);
			return callback(e);
		}
	}

	async createNamespace(query, callback) {
		try {
			var res = await this.spredis.createNamespace(query);
			return callback(null, res);	
		} catch(e) {
			console.error(e.stack);
			return callback(e);
		}
	}

	exec(command, callback) {
		this.use(command.ns);
		if (!this.ns) {
			return callback(new Error('Could not find namespace'))
		}
		let action = this[command.action];
		if (!(action instanceof Function)) {
			return callback(new Error(`Unknown or unsupported command "${command.action}"`))
		}
		this[command.action](command.input, callback)
	} 
}

module.exports = Commander;