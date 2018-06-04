'use strict';
const _ = require('lodash');
const Redis = require('ioredis'),
	Definition = require('./Definition'),
	Indexer = require('./Indexer'),
	Searcher = require('./Searcher'),
	Compound = require('./strategy/Compound'),
	Prefixed = require('./Prefixed');

// var pack = JSON.stringify,
// 	unpack = JSON.parse;

// var multiSortScript = require('fs').readFileSync(require('path').normalize(__dirname + '/../lua/multi-key-sort.lua'), 'utf8')
// const pack = require('snappy').compressSync;
// const unpack = require('snappy').uncompressSync;
const db = require('./db');
const CONFIG_KEY = require('./constants').CONFIG_KEY;

module.exports = class NS extends Prefixed {
	constructor(opts, db) {
		super();
		opts = this.validate(opts);
		this.namespace = opts.name;
		this._raw = opts;
		_.assign(this, opts);
		let defs = {};
		let compounds = {};
		let lang = this.defaultLang;

		// console.log("DEFAULT LANG:", this.defaultLang);
		_.forEach(this.fields, (f, name) => {
			f.lang = f.lang || lang;
			f.fallBackLang = f.fallBackLang || lang;
			defs[name] = new Definition(name, opts.prefix, f);
		});
		let strats = {};
		_.forEach(defs, (v,k) => {
			strats[k] = v.strategy;
			v.strategy.namespace = this;
		})
		// console.log(strats);
		_.forEach(this.compounds, (c,k) => {
			compounds[k] = new Compound(opts.prefix, k, c, strats)
		})
		// this.configKey = CONFIG_KEY;
		this.indexer = new Indexer(opts.prefix, defs, compounds, this.defaultLang, this.minLangSupport);
		this.searcher = new Searcher(opts.prefix, defs, compounds, this.defaultLang);
		// this.valueKey = this.prefix + ':VALUES';
		this.valueKey = this.prefix + ':ID:';

		if (db) {
			this.attach(db);
		}
	}

	getStrategy(fieldName) {
		if (!this.searcher) return null;
		return this.searcher.getStrategy(fieldName);
	}

	get defaultLang() {
		return this._defaulLang || 'en';
	}

	set defaultLang(lang) {
		this._defaulLang = lang;
	}

	getValueKey(id) {
		return this.valueKey + String(id);
	}

	get redis() {
		return this._redis
	}

	set redis(redis) {
		this._redis = redis;
		if (this.indexer) {
			this.indexer.redis = redis;
		}
		if (this.searcher) {
			this.searcher.redis = redis;
		}
	}

	attach(db) {
		this.redis = db;
		this.connected = true;
		return this;
	}

	// async connect(dbConfig) {
		
		
	// 	this.redis = db(dbConfig);


		
		
	// 	this.connected = true;		
	// 	// this.saveNamespaceConfig.resolve()
	// 	await this.saveNamespaceConfig();
	// 	return this;
	// }
	// connect(redisConfig) {
	// 	return Promise.all(this._connect(redisConfig))
	// }
	async  checkForLeaks() { //for dev only, will produce false positives in production
		var tempPattern = this.prefix + ':TMP:*';
		var cursor =-1
		var leaks = [];
		var iterations = 0;
		while (cursor !== 0) {
			if (cursor === -1) cursor = 0;
			var keys = await this.redis.indexServer().scan(cursor, 'MATCH', tempPattern, 'COUNT', 10000);
			var cursor = parseInt(keys[0])
			iterations++;
			if (keys[1].length) {
				// total += keys[1].length
				leaks.push.apply(leaks, keys[1]);
				// await this.redis.del.apply(this.redis, keys[1]);
			}
			console.log("found %d so far (round %d)...", leaks.length, iterations);
		}
		console.log("found %d total", leaks.length);
		return leaks;
	}

	async  saveNamespaceConfig() {
		var existing = await this.getNamespaceConfig(CONFIG_KEY);
		if (existing) {
			//TODO: compare the configs for for tarcking re-indexing
			await this.redis.indexServer().hset(CONFIG_KEY, this.namespace, JSON.stringify(this._raw));
		} else {
			await this.redis.indexServer().hset(CONFIG_KEY, this.namespace, JSON.stringify(this._raw));
		}
	}

	saveDocsToFile(fname) {
		return this.indexer.saveDocsToFile(fname);
	}

	reIndex() {
		return this.indexer.reIndex();
	}
	fullReIndex() {
		return this.indexer.reIndex(true);
	}
	
	quit() {
		if (this.indexer) {
			this.indexer.cancelResorting();
		}
	}

	drop() {
		// var theKeys
		if (this.indexer) {
			this.indexer.cancelResorting();
		}
		let conn = this.redis.indexServer();
		let stream = conn.scanStream({match:this.prefix + '*', count:100});
		return new Promise((resolve, reject) => {
			var total = 0;
			
			stream.on('data', async (keys) => {
				total += keys.length
				if (keys.length) {
					for (var i = 0; i < keys.length; i++) {
						await conn.del(keys[i]);
					}
					// conn.del.apply(conn, keys);
				}
				console.log("deleted %d", total);
			});
			stream.on('end', () => {
				resolve(total)
			});
			stream.on('error', (err) => {
				reject(err)
			});
		});
		
		// var keys = await this.redis.indexServer().scan(0, 'MATCH', this.prefix + '*', 'COUNT', 100);
		// // console.log("keys", keys);
		
		// if (keys[1].length) {
		// 	total += keys[1].length
		// 	await this.redis.indexServer().del.apply(this.redis.indexServer(), keys[1]);
		// }
		// console.log("deleted %d", total);

		// while (parseInt(keys[0])) {

		// 	keys = await this.redis.indexServer().scan(keys[0], 'MATCH', this.prefix + '*', 'COUNT', 100);
		// 	if (keys[1].length) {
		// 		total += keys[1].length
		// 		await this.redis.indexServer().del.apply(this.redis.indexServer(), keys[1]);
		// 		console.log("deleted %d", total);
		// 	}
		// }
		// return total;
		// scan 0 MATCH "VEHICLE:*" COUNT 10000
	}

	async  getNamespaceConfig() {
		var res = await this.redis.indexServer().hget(CONFIG_KEY, this.namespace);
		// console.log(res);
		return res ? JSON.parse(res) : null;
	}

	getDocument(id) {
		return this.indexer.getDocument(id);
	}

	

	reindexDocuments(docs) {
		return this.indexer.addDocuments(docs, true);
	}

	addDocuments(docs) {
		return this.indexer.addDocuments(docs);
	}

	addDocument(doc) {
		return this.indexer.addDocument(doc);
	}

	deleteDocuments(docs) {
		return this.indexer.deleteDocuments(docs);
	}

	deleteDocument(doc) {
		return this.indexer.deleteDocument(doc);
	}

	search(query) {
		return  this.searcher.search(query);
	}

	validate(opts) {
		if (!opts.prefix) {
			throw new Error('Namespace must have a prefix!');
		}
		return opts;
	}
}