'use strict';


/*
	entry point into spredis logic. connects listener layer to namespaces
	namespaces are created/modified here 
*/
var NS = require('./NS');
var db = require('./db');

var SINGLETON = null;

// var config = require('../../dev/sample-config');

const defaultNSName = process.env.SPREDIS_DEFAULT_NS || 'auctions';
const CONFIG_KEY = require('./constants').CONFIG_KEY;
const _ = require('lodash');
const Expirer = require('./Expirer');

// var NSMAP = {};
class Spredis {
	constructor(redisConfig) {
		this.redisConfig = redisConfig || {};
		this.namespaces = {};
		this.defaultNamespace = null;
		this.disableExpirer = this.redisConfig.disableExpirer;
		delete this.redisConfig.disableExpirer;
	}

	static getInstance() {
		return SINGLETON;
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
		if (!this.disableExpirer) this.expirer = new Expirer(this);
	}

	async createNamespace(opts) {
		var ns =  new NS(opts, this.db);
		this.namespaces[opts.name] = ns;
		// console.log(ns.search);
		await ns.saveNamespaceConfig()
		return opts;
	}

	async useNamespace(name) {
		let ns = this.namespaces[name];
		if (!ns) {
			let val = await this.db.indexServer().hget(CONFIG_KEY, name);
			if (val) {
				this.namespaces[name] = new NS(JSON.parse(val), this.db);
				ns = this.namespaces[name];
			}
		}
		return ns;
	}

	async namespaceConfigs() {
		let configs = await this.db.indexServer().hgetall(CONFIG_KEY);
		let res = [];
		_.forEach(configs, (val, key) => {
			res.push(JSON.parse(val));
		})
		return res;
	}

	quit() {
		_.forEach(this.namespaces, (ns) => {
			try {
				ns.quit();
			} catch (e) {
				//eat it
				console.error(e);
			}
		});
		if (this.expirer) this.expirer.quit();
		// this.resorter && this.resorter.quit()
		this.db && this.db.quit();
	}

	
}

module.exports = Spredis;
