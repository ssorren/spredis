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
	// lockName(key) {
	// 	return this.lockPrefix + key;
	// }
	_timeIt(start) {
		return new Date().getTime() - start;
	}

	// async _getSortLock(source) {
	// 	let lock = null;
	// 	try {
	// 		lock = await this.locker.lock(this.lockName(source), 10000);
	// 	} catch (e) {
	// 		console.warn(`Somebody else is sorting '${source}', will try again later.`)
	// 		lock = null;
	// 	}
	// 	return lock;
	// }


	async checkForSort() {
		// console.log('Checking for dirty sorts...');
		let aDirtySort = await this.conn.spop(DIRTYSORTKEY);
		if (aDirtySort) {

			console.log(`Found a dirty sort key! ${aDirtySort}`);
			let startTime = new Date().getTime();

			await this.conn.call('spredis.zlapplyscores', aDirtySort);

			console.log(`Applying sort took ${new Date().getTime() - startTime}ms`);
		}
		this.scheduleSort();
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