'use strict';

/*
	does what the name implies.
	responsible for CUD (CRUD - R) operations.
	connected directly to the namespace
	
	directly controls the document itself
	but talks to definitions/strategies to execute redis commands for indexing individual fields.
*/

const _ = require('lodash');
const _exists = (val) => {
	return val !== null && val !== undefined && val !== '';
}
const Redlock = require('redlock');
const utils = require('../utils');
const uuid = require('uuid').v4;
const Prefixed = require('./Prefixed');

module.exports = class Indexer extends Prefixed {
	constructor(prefix, defs, composites, defaultLang='en', minLangSupport=[]) {
		super();
		this.prefix = prefix;
		this.allIdsKey = prefix + ':ALLIDS';
		this.defs = defs;
		this.defNames = _.keys(defs);
		this.valueKey = prefix + ':DOCS'
		this.composites = composites;
		// console.log(this.composites);
		this.indexingLock = this.prefix + ':INDEXLOCK';
		this.allStrategies = {};
		this.defaultLang = defaultLang;
		this.minLangSupport = minLangSupport;
		
		let self = this;
		_.forEach(this.defs, (v,k) => {
			self.allStrategies[k] = v.strategy;
		});

		this.protectedKeys = [
			this.allIdsKey, this.valueKey,
			this.indexingLock
		]
	}

	getValueKey(id) {
		return this.valueKey;
	}
	validateDoc(doc) {
		if (!doc.id) {
			throw new Error('Document has no id!: %s', JSON.stringify(doc));
		}
	}
	cancelResorting() {
		this.resortingCanceled = true;
		if (this.resortHandle) {
			clearTimeout(this.resortHandle);
		}
	}
	
	async  getDocument(id) {
		let qs = await this._redis.grabQueryConnection();
		let res;
		try {
			res = await qs.callBuffer('spredis.docget', this.getValueKey(), id);	
		} catch (e) {
			throw e;
		} finally {
			this._redis.releaseQueryConnection(qs);
		}
		
		// console.log('doc:', res[2].length);

		if (res && res.length === 3)
			return JSON.parse(  res[2].toString() );
			// return JSON.parse( unpack( res[2] ).toString() );

		throw new Error(`Could not find document for ${id}`);
	}

	async  addDocument(doc, force=false) {
		return await this.indexDocuments([doc], force);
	}

	async  addDocuments(docs, force=false) {
		return await this.indexDocuments(docs, force);
	}


	async  deleteDocuments(docs) {
		return await this.unIndexDocuments(docs);
	}

	async  deleteDocument(doc) {
		return await this.unIndexDocuments([doc]);
	}

	get redis() {
		return this._redis
	}

	get conn() {
		return this._redis.indexServer();
	}

	set redis(redis) {
		this._redis = redis;
		for (var i = 0; i < this.defNames.length; i++) {
			this.defs[this.defNames[i]].redis = redis;
		}
		// if (!this.locker) {
			this.locker = new Redlock([this._redis.lockServer()]);
		// }

		// if (!this.sortLocker) {
			this.sortLocker = new Redlock([this._redis.lockServer()], {
				retryCount: Number.MAX_SAFE_INTEGER,
				retryDelay: 10000
			});
		// }
	}

	getValue(doc, name, strategy) {
		if (strategy.supportsSource && strategy.source) {
			// console.log("val =", strategy.getValFromSource(doc, this.allStrategies), strategy.name);
			return strategy.getValFromSource(doc, this.allStrategies);
		}
		// console.log("val =", doc[name] || strategy.defaultValue, strategy.name);
		let v = doc[name]; 
		return _exists(v) ? v : strategy.defaultValue;
	}

	async  indexField(pipe, docs, oldDocs, def, oldDef, full=false) {
		// console.log('full=', full);
		try {
			let name = def.name;
			// var dirtys = [];
			for (let i = 0; i < docs.length; i++) {
				let oldDoc = oldDocs[i];
				let oldVal = oldDoc ? oldDoc[name] : null;
				let doc = docs[i];
				let val = doc[name];
				
				if (!full && oldDoc && doc && _.isEqual(oldVal, val)) {
					//we've seen this value before, skip over it
					continue;
				}
				if (oldDoc) {
					oldVal = def.typeDef.array ? oldVal : [oldVal];
					oldVal = _.isArray(oldVal) ? oldVal : [oldVal];
					for (let k = 0; k < oldVal.length; k++) {
						def.strategy.unIndexField(pipe, oldDoc._id, oldVal[k], k);
					};
				}
				val = def.typeDef.array ? val : [val];
				val = _.isArray(val) ? val : [val];
				for (let k = 0; k < val.length; k++) {
					def.strategy.indexField(pipe, doc._id, val[k], k);
				};
			};
			return 1;
			
		} catch(e) {
			console.log(e.stack);
		}
		return 1
	}

	__removeNonIndex(keys) {
		return _.difference(keys, this.protectedKeys);
	}

	dropIndexes() {
		let self = this;
		let conn = this.conn;
		let stream = conn.scanStream({
			match: this.prefix +'*',
			count: 100
		})
		return new Promise( (resolve, reject) => {
			console.log("doing a full re-index, dropping index data...");
			var total = 0
			stream.on('data', async (keys) => {
				let indeces = self.__removeNonIndex(keys);

				await conn.del.apply(conn, indeces);
				total += indeces.length;
				console.log("deleted %d indeces so far...", total);
			});
			stream.on('error', (err) => {
				reject(err)
			});
			stream.on('end', () => {
				console.log('finsihed dropping %d indeces.', total);
				resolve(total)
			});
			
		})
		
	}

	async  reIndex(full) {
		//not yet implemented
	}


	async  saveDocsToFile(fname) {
		
		
	}
	async  rebuildSorts() {
		//not yet implemented, don't think we need it anymore
	}
	
	_longestArrayValue(doc) {
		let self = this;
		let max = 1;
		_.forEach(self.allStrategies, (strat, k) => {
			// console.log(k, strat)
			if (strat.type.index && strat.type.array) {
				let val = self.getValue(doc, strat.name, strat);
				if (_.isArray(val) && val.length > 0) {
					max = max < val.length ? val.length : max;	
				}
			}
		});
		return max;
	}

	toLex(id, pos) {
		return id;
	}

	applyFieldValues(doc) {
		let self = this;
		_.forEach(this.allStrategies, (strat,name) => {
			doc[name] = self.getValue(doc, name, strat);
		})
		return doc;
	}

	compositeDirty(composite, a, b) {
		if (!b) return true;
		for (let i = 0; i < composite.sortedFields.length; i++) {
			let f = composite.sortedFields[i];
			if (!_.isEqual(a[f], b[f])) return true;
		}
		return false;
	}

	async indexDocuments(docs, full) {
		//TODO: need to implemt locking, only 1 server should be indexing at a time
		var lock = await this.locker.lock(this.indexingLock, 10000);


		try {
				

			var oldDocs = [];
			for (let i = 0; i < docs.length; i++) {

				if (!docs[i].id) throw new Error('No id for document!' + (docs[i].id));

				let resA = await this.conn.callBuffer('spredis.docget', this.getValueKey(), docs[i].id);
				if (resA && resA.length === 3) {
					let doc = JSON.parse( resA[2].toString() );
					doc._lid = parseInt(resA[0].toString());
					doc._id = resA[1].toString();
					oldDocs.push(this.applyFieldValues(doc));
				} else {
					// console.log('couldnt find document');
					oldDocs.push(null);
				}
				let resB = await this.conn.call('spredis.docadd', this.getValueKey(), docs[i].id, JSON.stringify(docs[i]) );

				if (resB && _.isArray(resB) && resB.length === 2) {
					docs[i]._lid = resB[0];
					docs[i]._id = resB[1];
					await this.conn.call('spredis.sadd', this.allIdsKey, docs[i]._id);
				} else {
					throw new Error('Could not add document to index:', docs[i].id);
				}
			}

			
			

			for (let i = docs.length - 1; i >= 0; i--) { //validate id and store in all ids
				if (!docs[i]._id) throw new Error('No id for document!' + (docs[i].id));
				docs[i] = this.applyFieldValues(docs[i]);
			};

			let pipe = this.conn.multi();
			let reses;
			for (let i = 0; i < docs.length; i++) {
				let doc = docs[i];
				let oldDoc = oldDocs[i];
				_.forEach(this.composites,(c,name) => {
					if (this.compositeDirty(c, doc, oldDoc)) {
						c.unIndexDocument(pipe, oldDoc);
						c.indexDocument(pipe, doc);	
					}
					
				});
			}
			for (let i = 0; i < this.defNames.length; i++) {
				let def = this.defs[this.defNames[i]];
				if (def.typeDef.index) {
					this.indexField(pipe, docs, oldDocs, def, null, full);
				}
			};
			if (pipe.length) {
				reses = await pipe.exec();
			}

		} catch(e) {
			console.log(e.stack);
			// lock.unlock();
			// return 0
		} finally {
			lock.unlock();	
		}
		
		return 1;
	}

	async  unIndexField(pipe, docs, def) {
		try {
			let name = def.name;
			for (let i = 0; i < docs.length; i++) {
				let doc = docs[i];
				let val = doc[name];
				val = def.typeDef.array ? val : [val];
				val = _.isArray(val) ? val : [val];
				for (let k = 0; k < val.length; k++) {
					def.strategy.unIndexField(pipe, doc._id, val[k], k);
				};
			};
			return 1;
			
		} catch(e) {
			console.log(e.stack);
		}
		return 1
	}
	
	async unIndexDocuments(docs, full) {

		var lock = await this.locker.lock(this.indexingLock, 10000);


		try {

			_.remove(docs, (d) => {
				return d == null;
			});
			docs = _.map(docs, (doc) => {
				doc = _.isNumber(doc) ? String(doc) : doc;
				if (_.isString(doc)) {
					return {id:doc}
				}
				return {id:doc.id}
			});
			_.remove(docs, (d) => {
				return d.id == null;
			});
		
			let npipe = this.conn.pipeline();
			for (var i = docs.length - 1; i >= 0; i--) {
				if (!docs[i].id) throw new Error('No id for document!' + (docs[i].id));
				npipe.callBuffer('spredis.docget', this.getValueKey(), docs[i].id);
			}
			let nres = await npipe.exec();
			var oldDocs = [];
			for (var i = docs.length - 1; i >= 0; i--) {
				if (nres[i][1]){
					let res = nres[i][1];
					let doc =  this.applyFieldValues( JSON.parse(res[2].toString()) );
					doc._lid = parseInt(res[0].toString());
					doc._id = res[1].toString();
					oldDocs.push(doc);
				}
			}
			var pipe = this.conn.multi();
			
			_.remove(oldDocs, (d) => {
				return d == null;
			})
			for (var i = oldDocs.length - 1; i >= 0; i--) {
				let doc = oldDocs[i];
				if (doc && doc._id) {
					pipe.call('spredis.docrem', this.getValueKey(), doc.id);
					pipe.call('spredis.srem', this.allIdsKey, doc._id);
				}
				
			};
			for (var i = 0; i < this.defNames.length; i++) {
				var def = this.defs[this.defNames[i]];
				if (def.typeDef.index) {
					this.unIndexField(pipe, oldDocs, def);
				}
			};
			for (var i = 0; i < oldDocs.length; i++) {
				let doc = oldDocs[i]
				_.forEach(this.composites,(c,name) => {
					 c.unIndexDocument(pipe, doc);
				});
			}
			
			if (pipe.length) {
				await pipe.exec();
			}
		} catch(e) {
			console.log(e.stack);
		} finally {
			lock.unlock();	
		}
		
		return 1;
	}
}

