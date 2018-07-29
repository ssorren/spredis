'use strict';

/*
	defines and controls interaction with a namespace.
	all queries/commands are routed through this object.
	there will be one of these active for every defined namespace.
*/

const _ = require('lodash');
const Redis = require('ioredis'),
	Definition = require('./Definition'),
	Indexer = require('./Indexer'),
	Searcher = require('./Searcher'),
	// Compound = require('./strategy/Compound'),
	Composite = require('./strategy/Composite'),
	Prefixed = require('./Prefixed');

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
		// let compounds = {};
		let composites = {};
		let lang = this.defaultLang;
		this.fields["id"] = {type:"documentid" , "index": false, "facet":true};
		_.forEach(this.fields, (f, name) => {
			f.lang = f.lang || lang;
			f.fallBackLang = f.fallBackLang || lang;
			defs[name] = new Definition(name, opts.prefix, f);
		});
		this.strats = {};
		_.forEach(defs, (v,k) => {
			this.strats[k] = v.strategy;
			v.strategy.namespace = this;
		})
		// _.forEach(this.compounds, (c,k) => {
		// 	compounds[k] = new Compound(opts.prefix, k, c, strats)
		// })

		_.forEach(this.composites, (c,k) => {
			composites[k] = new Composite(opts.prefix, k, c, this.strats)
		})
		this.composites = composites;
		this.indexer = new Indexer(opts.prefix, defs, composites, this.defaultLang, this.minLangSupport);
		this.searcher = new Searcher(opts.prefix, defs, composites, this.defaultLang);
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

	/*
		checkForLeaks: unused...likely to be deleted
	*/
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
		let existing = await this.getNamespaceConfig(CONFIG_KEY);
		let modified = false;
		if (existing) {
			//TODO: compare the configs for for tarcking re-indexing
			modified = !_.isEqual(existing, this._raw);
			await this.redis.indexServer().hset(CONFIG_KEY, this.namespace, JSON.stringify(this._raw));
		} else {
			await this.redis.indexServer().hset(CONFIG_KEY, this.namespace, JSON.stringify(this._raw));
		}
		if (true || !existing || modified) {
			let command = [];
			let fields = this.fields ? _.keys(this.fields) : [];
			//we don't want to define the id field
			_.remove(fields, (f) => {return f === "id";});
			let composites = this.composites ? _.keys(this.composites) : [];
			let strat;
			_.forEach(fields, (f) => {
				strat = this.strats[f];
				command.push(...strat.toDefinitionCommand());
			});

			_.forEach(composites, (f) => {
				strat = this.composites[f];
				command.push(...strat.toDefinitionCommand());
			});
			// console.log('spredis.definenamespace', this.prefix, fields.length, composites.length, ...command);
			let start = new Date().getTime();
			let res = await this.redis.indexServer().call('spredis.definenamespace', this.prefix, this.defaultLang || 'en' ,fields.length, composites.length, ...command);
			console.log(`Redefinition took ${new Date().getTime() - start}ms`);
			console.log(JSON.parse(res));

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
		// delete this.strats;
		delete this.indexer;
		delete this.searcher;
		// delete this.defs;
		// if (this.indexer) {
		// 	this.indexer.cancelResorting();
		// }
	}

	drop() {
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