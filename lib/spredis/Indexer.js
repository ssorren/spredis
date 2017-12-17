'use strict';
var _ = require('lodash');
var _exists = (val) => {
	return val !== null && val !== undefined && val !== '';
}

var pack = require('snappy').compressSync;
var unpack = require('snappy').uncompressSync;
var Redlock = require('redlock');

module.exports = class Indexer {
	constructor(prefix, defs, compounds) {
		this.prefix = prefix;
		this.allIdsKey = prefix + ':ALLIDS';
		this.idsOnlyKey = prefix + ':IDS';
		this.defs = defs;
		this.defNames = _.keys(defs);
		this.dirtyIndexKey = prefix + ':D:I';
		this.valueKey = prefix + ':ID:'
		// this.configKey = prefix + ':CONFIG';
		this.dirtSortKey = this.prefix + ':DIRTYSORTDATA';
		this.compounds = compounds;
		this.rebuildSortLock = this.prefix + ':RESORTLOCK';
		this.indexingLock = this.prefix + ':INDEXLOCK';
		this.allStrategies = {};
		let self = this;
		_.forEach(this.defs, (v,k) => {
			self.allStrategies[k] = v.strategy;
		});
		// this.sourceStrategies = _.filter(this.allStrategies, (s) => {return s.supportsSource && s.source;});
		// console.log('sourceStrategies:', this.sourceStrategies)
	}

	getValueKey(id) {
		return this.valueKey + String(id);
	}
	validateDoc(doc) {
		if (!doc.id) {
			throw new Error('Document has no id!: %s', JSON.stringify(doc));
		}
	}
	async  getDocument(id) {
		var res = await this.conn.getBuffer(this.getValueKey(id));
		return res ? JSON.parse( unpack( res ).toString() ) : null;
	}

	async  addDocuments(doc) {
		return await this.indexDocuments([doc]);
	}

	async  addDocuments(docs) {
		return await this.indexDocuments(docs);
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
		if (!this.locker) {
			this.locker = new Redlock([this._redis.lockServer()]);
		}
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
			var name = def.name;
			// var dirtys = [];
			for (var i = 0; i < docs.length; i++) {
				var oldDoc = oldDocs[i];
				if (oldDoc) {
					var oldVal = oldDoc[name];
					oldVal = def.typeDef.array ? oldVal : [oldVal];
					for (var k = 0; k < oldVal.length; k++) {
						def.strategy.unIndexField(pipe, oldDoc.id, oldVal[k], k);
					};
				}

				var doc = docs[i];
				var val = doc[name];
				val = def.typeDef.array ? val : [val];
				for (var k = 0; k < val.length; k++) {
					def.strategy.indexField(pipe, doc.id, val[k], k);
				};
			};
			return 1;
			
		} catch(e) {
			console.log(e.stack);
		}
		return 1
	}

	__removeNonIndex(keys) {
		let allIds = this.allIdsKey;
		let idsOnly = this.idsOnlyKey;
		let valueKey = this.valueKey
		// let configKey = this.configKey;
		_.remove(keys, (key) => {
			return key === allIds || key === idsOnly || _.startsWith(key, valueKey);
		});
		return keys;
	}

	async  dropIndexes() {
		console.log("doing a full re-index, dropping index data...");
		var keys = await this.conn.scan(0, 'MATCH', this.prefix +'*', 'COUNT', 10000);
		// console.log("keys", keys);
		var total = 0
		var inedeces = this.__removeNonIndex(keys[1])
		if (inedeces.length) {
			total += inedeces.length
			await this.conn.del.apply(this.conn, inedeces);
			console.log("deleted %d indeces so far...", total);
		}

		while (parseInt(keys[0])) {
			keys = await this.conn.scan(keys[0], 'MATCH', this.prefix + '*', 'COUNT', 10000);
			inedeces = this.__removeNonIndex(keys[1])
			if (inedeces.length) {
				total += inedeces.length
				await this.conn.del.apply(this.conn, inedeces);
				console.log("deleted %d indeces so far...", total);
			}
		}
		console.log('finsihed dropping %d indeces.', total);
		return total;
	}
// 20264
	async  reIndex(full) {
		if (full) await this.dropIndexes();		
		var cursor = -1;
		var total = 0
		var startTime = new Date().getTime();
		while (cursor != 0) {
			if (cursor == -1) cursor = 0;
			var res = await this.conn.zscan(this.allIdsKey, cursor, 'COUNT', 100);
			cursor = parseInt(res[0]);

			var ids = res[1];
			// console.log(ids)
			total += ids.length
			
			var docs = [];
			for (var i = 0; i < ids.length; i++) {
				var d = await this.conn.getBuffer(this.getValueKey(ids[i]))
				// await this.conn.set(this.getValueKey(ids[i]), pack(d))
				if (d){
					// console.log(d) 
					docs.push( JSON.parse( unpack(d).toString() ) );	
				} else {
					console.log("can't find %s", this.getValueKey(String(ids[i])));
				}
				
			};
			await this.indexDocuments(docs, full);
			console.log('indexed %d docs so far', total)
		}
		var endTime = new Date().getTime();
		console.log('finsihed indexing %d docs in %d seconds (%d/sec)', total, (endTime - startTime) / 1000, total / ((endTime - startTime) / 1000))
	}


	async  saveDocsToFile(fname) {
		
		var cursor = -1;
		var total = 0
		while (cursor != 0) {
			if (cursor == -1) cursor = 0;
			var res = await this.conn.zscan(this.idsOnlyKey, cursor, 'COUNT', 100);
			cursor = parseInt(res[0]);
			var ids = res[1];
			total += ids.length
			console.log('%d so far', total)
			var docs = [];
			for (var i = 0; i < ids.length; i++) {
				var d = await this.conn.get(this.getValueKey(ids[i]))
				docs.push( JSON.parse(d) )
			};
			require('fs').writeFileSync(fname + '/' + String(cursor) + '.json', JSON.stringify(docs, null, 2));
		}
		
		console.log('wrote %d', total)
	}
	async  rebuildSorts() {
		// var lock = await this.locker.lock(this.rebuildSortLock, 20000);
		// try {
		// 	//TODO: this needs to be refined, need to do this in chunks. for millions of documents it will take way too long
		// 	//can't block redis that long
		// 	var dirtys = await this.conn.smembers(this.dirtSortKey)
		// 	for (var i = 0; i < dirtys.length; i++) {
		// 		var resortArgs = dirtys[i].split(',');
		// 		// console.log(resortArgs);
				

		// 		var sortSet = resortArgs[0];
		// 		var allValuesKey = resortArgs[1];
		// 		var weightPattern = resortArgs[2];
		// 		// var allIdsKey = parseInt(resortArgs[3]) ? 'ALPHA' : 'NUMERIC';
		// 		var alpha = parseInt(resortArgs[3]) ? 'ALPHA' : 'NUMERIC';
		// 		var finalSort = resortArgs[4];

		// 		var args = [
		// 			sortSet,
		// 			allValuesKey,
		// 			weightPattern,
		// 			this.allIdsKey,
		// 			finalSort,
		// 			alpha
		// 		]
		// 		var startTime = new Date().getTime();
		// 		var res = await this.conn.reSort.apply(this.conn, args);
		// 		var endTime = new Date().getTime();

		// 		console.log("resorted index in %dms", endTime - startTime);
		// 		await this.conn.del(this.dirtSortKey);
		// 		// if (alpha) {
		// 		// 	valueSort = await this.conn.sort(this.allIdsKey, 'BY', weightPattern, 'GET', weightPattern, 'ASC', 'ALPHA', 'STORE', allValuesKey)
		// 		// 	sorted = await this.conn.sort(this.allIdsKey, 'BY', weightPattern, 'GET', '#', 'GET', weightPattern, 'ASC', 'ALPHA', 'STORE', 'TEMPSORT')
		// 		// } else {
		// 		// 	valueSort = await this.conn.sort(this.allIdsKey, 'BY', weightPattern, 'GET', weightPattern, 'ASC', 'STORE', allValuesKey)
		// 		// 	sorted = await this.conn.sort(this.allIdsKey, 'BY', weightPattern, 'GET', '#', 'GET', weightPattern, 'ASC', 'STORE', 'TEMPSORT')
		// 		// }
				
		// 		// console.log("sorted:", allValuesKey);
		// 	};
		// } catch (e) {
		// 	console.log(e.stack);
		// 	lock.unlock();
		// 	return 0
		// }
		// lock.unlock();
		// return 1;
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
			if (!full) {
				// let multi = this.conn.multi();
				var readPipe = this.conn.pipeline();
				_.each(docs, (doc) =>{
					readPipe.getBuffer( this.getValueKey(doc.id) );
				});
				oldDocs = await readPipe.exec();
				// await multi.exec();
				oldDocs = _.map(oldDocs, (old) => {
					if (old && old[1]) {
						return JSON.parse( unpack(old[1]).toString() );
					}
					return null;
				});
			}
			
			// console.log(oldDocs.length, docs.length);
			var pipe = this.conn.multi();
			// for (var i = 0; i < docs.length; i++) {
			// 	var doc = docs[i];
			// 	var oldDoc = oldDocs[i];

			// };

			for (var i = docs.length - 1; i >= 0; i--) { //validate id and store in all ids
				if (!docs[i].id) throw new Error('No id for document!' + (docs[i].id));
				let doc = this.applyFieldValues(docs[i]);


				pipe.set( this.getValueKey(doc.id), pack( JSON.stringify(doc) ) );
				pipe.zadd(this.allIdsKey, 0, doc.id);

				// if (oldDocs[i]) {
				// 	let olongest = this._longestArrayValue(oldDocs[i]);
				// 	for (let k = 0; k < olongest; k++) {
				// 		pipe.zrem(this.allIdsKey, 0, this.toLex(oldDocs[i].id, k));
				// 	}	
				// }
				// let longest = this._longestArrayValue(docs[i]);
				// for (let k = 0; k < longest; k++) {
				// 	pipe.zadd(this.allIdsKey, 0, this.toLex(docs[i].id, k));
				// }
			};
			for (var i = 0; i < this.defNames.length; i++) {
				var def = this.defs[this.defNames[i]];
				if (def.typeDef.index) {
					this.indexField(pipe, docs, oldDocs, def);
				}
			};
			let self = this;
			for (var i = 0; i < docs.length; i++) {
				let doc = docs[i]
				let oldDoc = oldDocs[i];
				_.forEach(this.compounds,(c,name) => {
					if (oldDoc) c.unIndexDocument(pipe, oldDoc);
					c.indexDocument(pipe, doc);
				});
			}
			
			
			// dirtys = _.uniq(dirtys);
			// if (dirtys.length) {
			// 	dirtys.unshift(this.dirtyIndexKey);
			// 	// console.log("dirtys:", dirtys);
			// 	pipe.sadd.apply(pipe, dirtys);	
			// }
			var reses = await pipe.exec();
			// var e = new Date().getTime();
			// console.log("%d redis commands executed in %dms.", reses.length, e - s);

			// var reses = await pipe.exec();
			// console.log(dirtys);


		} catch(e) {
			console.log(e.stack);
			// lock.unlock();
			// return 0
		} finally {
			lock.unlock();	
		}
		
		return 1;
	}
}

