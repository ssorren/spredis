'use strict';
const _ = require('lodash');
const _exists = (val) => {
	return val !== null && val !== undefined && val !== '';
}

const pack = require('snappy').compressSync;
const unpack = require('snappy').uncompressSync;
const Redlock = require('redlock');
const utils = require('../utils');
const uuid = require('uuid').v4;
const Prefixed = require('./Prefixed');
const DIRTYSORTKEY = require('./constants').DIRTYSORTKEY;

module.exports = class Indexer extends Prefixed {
	constructor(prefix, defs, compounds) {
		super();
		this.prefix = prefix;
		this.allIdsKey = prefix + ':ALLIDS';
		// this.idsOnlyKey = prefix + ':IDS';
		this.defs = defs;
		this.defNames = _.keys(defs);
		this.dirtyIndexKey = prefix + ':D:I';
		this.valueKey = prefix + ':DOCS'
		// this.configKey = prefix + ':CONFIG';
		// this.dirtySortKey = this.prefix + ':DIRTYSORTDATA';
		this.compounds = compounds;
		this.rebuildSortLock = this.prefix + ':RESORTLOCK';
		this.indexingLock = this.prefix + ':INDEXLOCK';
		this.allStrategies = {};
		let self = this;
		_.forEach(this.defs, (v,k) => {
			self.allStrategies[k] = v.strategy;
		});

		this.idIncr = this.prefix + ':IDINCR'
		this.idMap = this.prefix + ':IDMAP'
		this.availableIds = this.prefix + ":IDSAVAIL"
		// this.sourceStrategies = _.filter(this.allStrategies, (s) => {return s.supportsSource && s.source;});
		// console.log('sourceStrategies:', this.sourceStrategies)
		this.protectedKeys = [
			this.allIdsKey, this.idMap, this.valueKey, this.idIncr, this.availableIds,
			this.indexingLock, this.rebuildSortLock
		]
		// this.resortHandle = setTimeout(() => {
		// 	self._reIndexASort()
		// }, 10000); //let's reindex a dirty sort every 10 seconds or so
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
	async _reIndexASort() {
		let lock = null;
		let lock2 = null;
		// let lock2 = await this.locker.lock(this.indexingLock, 10000);
		try {
			lock = await this.sortLocker.lock(this.rebuildSortLock, 10000);
			// lock2 = await this.locker.lock(this.indexingLock, 10000);
			let aDirtySort = await this.conn.spop(DIRTYSORTKEY);
			// console.log(aDirtySort);
			if (aDirtySort) {
				let a = aDirtySort.split(',');
				let tempStore = this.getTempIndexName();
				a.push(tempStore);
				console.log('a:', a);
				let start = new Date().getTime();
				await this.conn.reSort.apply(this.conn, a);
				let end = new Date().getTime();
				console.log(`Resorted in ${end - start}ms`);
				// lock2.unlock()
				// lock2 = null;
				await this.conn.del(tempStore);
				await utils.waitAwhile(3000); //let's wait awhile so we don't re-index too often
			} else {
				// console.log('nothing to re-sort');
			}
		} catch (e) {
			console.error(e.stack);
		} finally {
			// lock2.unlock();
			// lock2 && lock2.unlock();
			lock && lock.unlock();
			if (!this.resortingCanceled) {
				let self = this;
				this.resortHandle = setTimeout(() => {
					self._reIndexASort()
				}, 10000);
			}
		}
		
	}
	async  getDocument(id) {
		let qs = this._redis.queryServer();
		let res = await qs.callBuffer('spredis.docget', this.getValueKey(), id);
		console.log('doc:', res[2].length);

		if (res && res.length === 3)
			return JSON.parse(  res[2].toString() );
			// return JSON.parse( unpack( res[2] ).toString() );

		throw new Error(`Could not find document for ${id}`);
	}

	async  addDocuments(doc) {
		return await this.indexDocuments([doc]);
	}

	async  addDocuments(docs) {
		return await this.indexDocuments(docs);
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

	async  indexField(pipe, docs, oldDocs, def, oldDef) {
		try {
			let name = def.name;
			// var dirtys = [];
			for (let i = 0; i < docs.length; i++) {
				let oldDoc = oldDocs[i];
				let oldVal = oldDoc ? oldDoc[name] : null;
				let doc = docs[i];
				let val = doc[name];
				// if (name === 'vin') {
				// 	console.log(oldDoc && doc && _.isEqual(oldVal, val))
				// }
				// if (oldDoc) console.log('old:',oldVal, 'new:', val);
				if (oldDoc && doc && _.isEqual(oldVal, val)) {
					//we've seen this value before, skip over it

					continue;
				}
				if (oldDoc) {
					// let oldVal = oldDoc[name];
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
		// let allIds = this.allIdsKey;
		// // let idsOnly = this.idsOnlyKey;
		// let valueKey = this.valueKey
		// let protectedKeys = [
		// 	this.allIdsKey, this.idMap, this.valueKey, this.idIncr, this.availableIds
		// ]
		// let configKey = this.configKey;
		// _.remove(keys, (key) => {
		// 	return key === allIds || key === idsOnly || _.startsWith(key, valueKey);
		// });
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
				
				// for (var i = 0; i < indeces.length; i++) {
				// 	await conn.del(indeces[i]);
				// }

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
		// return pos ? String(id) + ';' + String(pos): String(id);
	}

	applyFieldValues(doc) {
		let self = this;
		_.forEach(this.allStrategies, (strat,name) => {
			doc[name] = self.getValue(doc, name, strat);
		})
		return doc;
	}

	async indexDocuments(docs, full) {
		//TODO: need to implemt locking, only 1 server should be indexing at a time
		var lock = await this.locker.lock(this.indexingLock, 10000);


		try {
				
			// var dirtys = [];
			// var s = new Date().getTime();
			// var res = await pipe.exec();
			var oldDocs = [];
			for (let i = 0; i < docs.length; i++) {

				if (!docs[i].id) throw new Error('No id for document!' + (docs[i].id));

				let resA = await this.conn.callBuffer('spredis.docget', this.getValueKey(), docs[i].id);
				if (resA && resA.length === 3) {
					// let doc = JSON.parse( unpack(resA[2]).toString() );
					let doc = JSON.parse( resA[2].toString() );
					doc._lid = parseInt(resA[0].toString());
					doc._id = resA[1].toString();
					oldDocs.push(this.applyFieldValues(doc));
				} else {
					// console.log('couldnt find document');
					oldDocs.push(null);
				}
				// let resB = await this.conn.call('spredis.docadd', this.getValueKey(), docs[i].id, pack( JSON.stringify(docs[i]) ));
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
				_.forEach(this.compounds,(c,name) => {
					c.unIndexDocument(pipe, oldDoc);
					c.indexDocument(pipe, doc);
				});
			}
			for (let i = 0; i < this.defNames.length; i++) {
				let def = this.defs[this.defNames[i]];
				if (def.typeDef.index) {
					this.indexField(pipe, docs, oldDocs, def);
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
			// var dirtys = [];
			for (let i = 0; i < docs.length; i++) {
				let doc = docs[i];
				let val = doc[name];
				val = def.typeDef.array ? val : [val];
				for (let k = 0; k < val.length; k++) {
					// console.log('unindexng', doc._id, val[k], def.name)
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
		//TODO: need to implemt locking, only 1 server should be indexing at a time
		var lock = await this.locker.lock(this.indexingLock, 10000);


		try {
			docs = _.map(docs, (doc) => {
				if (!doc) throw new Error(`Cannot delete document with id '${doc}'`)
				doc = _.isNumber(doc) ? String(doc) : doc;
				if (_.isString(doc)) {
					return {id:doc}
				}
				if (!doc.id) throw new Error(`Cannot delete document with id '${doc.id}'`)
				return {id:doc.id}
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
					// let theId = Number(nres[i][1])
					let res = nres[i][1];
					// let doc =  this.applyFieldValues( JSON.parse(unpack(res[2]).toString()) );
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
				_.forEach(this.compounds,(c,name) => {
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

