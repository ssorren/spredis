/*
	automatic purging of expired documents (as defined in the namespace)
	polls every 5 minutes and deletes expired documents.

	possible improvements:
		break off into a separate process so as not to hinder queries
*/

const Prefixed = require('./Prefixed');
const constants = require('./constants');
const Redlock = require('redlock');
// const GetIdFromLex = /;(.+)/;
const utils = require('../../lib/utils');
const EXPIRE_INTERVAL = 60 * 5 * 1000;

class Expirer extends Prefixed {
	constructor(spredis) {
		super();
		this.spredis = spredis;
		
		this.schedule();
		this.locker = new Redlock([this.spredis.db.lockServer()], {
			retryCount: 0,
			// retryDelay: 10000
		});
	}
	_timeIt(start) {
		return new Date().getTime() - start;
	}


	async expire(c, lock) {
		let expField = c.expirationField;
		let query  = {};
		query[c.expirationField] = {min: null, max:new Date()};
		let r = {
			start: 0,
			count: 25,
			idsOnly:true,
			query: query
		};
		let found = null;
		let purged = 0;
		do {
			try {
				let ns = await this.spredis.useNamespace(c.name);

				if (ns) {
					
					let results = await ns.search(r);
					if (results && results.items && results.items.length) {
						found = results.items;
						found = found.map( (doc) => {return JSON.parse(doc);} );

						await ns.deleteDocuments(found);
						lock = await lock.extend(1000);
						purged += found.length;
						//pausing...let some queries run through unhindered. deletes create write locks on index data
						await utils.waitAwhile(100);
					} else {
						found = null;
					}
				
				}
			} catch (e) {
				console.error(e.stack);
				found = null;
			}
		} while(this.spredis && found && found.length);
		if (purged) console.info(`Purged ${purged} expired documents from '${c.name}.'`);
	}

	async checkForExpired() {
		if (this.spredis) {
			let configs = await this.spredis.namespaceConfigs();
			for (var i = 0; i < configs.length; i++) {
				let c = configs[i];
				if (c.expirationField) {
					let nsLock = null;
					try {
						nsLock = await this.locker.lock('SP:EXPIRER:' + c.name, 1000);
						if (nsLock) await this.expire(c, nsLock);		
					} catch (e) {
						// console.error(e.stack);
						// let's not bother with this error. there may be a lot of contention if we're running multiple servers
						// we'll catch up next time this runs
					} finally {
						if (nsLock) nsLock.unlock();
					}
				}
			}
		}
		this.schedule();
	}

	schedule() {
		if (this.quitting) return;
		let self = this;
		this.checkInterval = setTimeout( () => {
			self.checkForExpired().catch(e=>{console.error(e.stack);})
		}, EXPIRE_INTERVAL);
	}
	
	quit() {
		this.quitting = true;
		if (this.checkInterval) {
			clearTimeout(this.checkInterval);
		}
		this.spredis = null;
	}
}

module.exports = Expirer