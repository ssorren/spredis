'use strict';
var NS = require('./NS');
var db = require('./db');

var SINGLETON = null;

// var config = require('../../dev/sample-config');

const defaultNSName = process.env.SPREDIS_DEFAULT_NS || 'auctions';
const CONFIG_KEY = require('./constants').CONFIG_KEY;
const _ = require('lodash');

class Spredis {
	constructor(redisConfig) {
		this.redisConfig = redisConfig;
		this.namespaces = {};
		this.defaultNamespace = null;
	}

	async initialize() {
		this.db = db(this.redisConfig);
		SINGLETON = this;		
		let res = await this.db.indexServer().hgetall(CONFIG_KEY);
		// console.log(res)
		let self = this;
		_.forEach(res, (val, key) => {
			self.namespaces[key] = new NS(JSON.parse(val), self.db);
		});

		if (defaultNSName) {
			this.defaultNamespace = this.namespaces[defaultNSName];
		}
	}

	async createNamespace(opts) {
		var ns =  new NS(opts, this.db);
		this.namespaces[opts.name] = ns;
		await ns.saveNamespaceConfig()
		return ns;
	}

	useNamespace(name) {
		return this.namespaces[name];
	}

	quit() {
		this.db.quit();
	}
}

Spredis.getInstance = function() {
	return SINGLETON;
}
module.exports = Spredis;
