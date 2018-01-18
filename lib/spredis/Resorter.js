const Prefixed = require('./Prefixed');
// var SINGLETON = null;
const constants = require('./constants');
const DIRTYSORTKEY = constants.DIRTYSORTKEY;
const Redlock = require('redlock');
const GetIdFromLex = /;(.+)/;

class Resorter extends Prefixed {
	constructor(db) {
		super();
		this.prefix = '_XX:SPREDIS:RESORT:'
		this.db = db;
		// console.log(db)
		// SINGLETON = this;
		this.lockPrefix = '_XX:SPREDIS:SORTING:'
		this.scheduleSort();
		this.locker = new Redlock([db.lockServer()], {
			retryCount: 0,
			// retryDelay: 10000
		});
	}

	get conn() {
		return this.db ? this.db.indexServer() : null;
	}
	// static getInstance(db) {
	// 	if (!SINGLETON && db) {
	// 		new Resorter(db)	
	// 	}
	// 	return SINGLETON;
	// }
	lockName(key) {
		return this.lockPrefix + key;
	}
	_timeIt(start) {
		return new Date().getTime() - start;
	}

	async _getSortLock(source) {
		let lock = null;
		try {
			lock = await this.locker.lock(this.lockName(source), 10000);
		} catch (e) {
			console.warn(`Somebody else is sorting '${source}', will try again later.`)
			lock = null;
		}
		return lock;
	}



	async sortUnsorted(source, finalSort, unsorted) {
		let lock = null;
		try {
			lock = await this._getSortLock(source);
			if (lock == null) return;
			/*
				we don't want to block indexing as this may take a while..
				but we run the risk of new items being added while we resort
				let's just move the unsorted items to a new key so we can process them without 
				worrying about colissions or race conditions
				this works because of the single-threaded nature of redis
			*/
			let tempUnsorted = this.getTempIndexName();
			await this.conn.rename(unsorted, tempUnsorted);
			unsorted = tempUnsorted; 
			/* 1 command and all done */

			let sourceCount = await this.conn.zcard(source);
			let unsortedCount = await this.conn.zcard(unsorted);
			if (
				sourceCount == unsortedCount //first sort
				|| sourceCount < 5000 //let's keep doing full resoirts until we have a fair amount of values in the index
				) {
				await this.fullResort(source, finalSort, unsorted);
			} else {
				console.log(`Attempting individual sorts ${sourceCount} total, ${unsortedCount} unsortedCount`);
				//TODO: duplicate the logic from literal sorters (store-sort.lua)
				//if the floating point accuracy gets out of control then do a full sort
				await this.fullResort(source, finalSort, unsorted);
			}


		} catch (e) {
			console.error(e.stack);
		} finally {
			lock && lock.unlock();
			await this.conn.del(unsorted); //no need to keep this, doing a full resort
		}
	}

	async fullResort(source, finalSort, unsorted) {
		console.log(`Resorting all in '${source}`)
		let tempStore = this.getTempIndexName();
		let s = new Date().getTime();
		let conn = this.wrapPipe(this.conn);
		await conn.sort(source, 'BY', '#', 'store', tempStore);
		console.log(`Sort values took ${this._timeIt(s)}ms`);

		let chunk = await conn.lrange(tempStore, 0, 99);
		s = new Date().getTime();
		let count = 0;
		while (chunk && chunk.length) {
			let command = ['spredis.dhashset', finalSort];
			for (var i = 0; i < chunk.length; i++) {
				let res = chunk[i];
				if (res) {
					res = GetIdFromLex.exec(res)
					// res = res.match(/:([^:]+)/);
					
					if (res && res[1]) {
						//TODO: ony set if exists, copy functionality of zadd XX
						
						command.push(res[1], ++count);
					}
				}
			}
			if (command.length > 2) {
				await conn.callBuffer.apply(conn, command);
				// conn.zadd.apply(conn, command);
			}
			await conn.ltrim(tempStore, 100, -1);
			chunk = await conn.lrange(tempStore, 0, 99);
		}
		console.log(`Applying sort to id's took ${this._timeIt(s)}ms`);
		await conn.del(tempStore);
	}

	async checkForSort() {
		// console.log('Checking for dirty sorts...');
		// let aDirtySort = await this.conn.spop(DIRTYSORTKEY);
		// if (aDirtySort) {
		// 	let a = aDirtySort.split(',');
		// 	if (a[0] && a[1] && a[2]) {
		// 		await this.sortUnsorted(a[0], a[1], a[2]);	
		// 	} else {
		// 		console.warn(`Bad dirty sort entry '${aDirtySort}'...check your code!`)
		// 	}
		// }
		// this.scheduleSort();
	}

	scheduleSort() {
		if (this.quitting) return;
		let self = this;
		this.checkInterval = setTimeout( () => {
			self.checkForSort()
		}, 20000);
	}
	quit() {
		this.quitting = true;
		if (this.checkInterval) {
			clearTimeout(this.checkInterval);
		}
	}
}

module.exports = Resorter