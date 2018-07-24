const Redis = require('ioredis');
Redis.Promise = Promise;
const pooler = require('generic-pool');

const _ = require('lodash');
let RetryStrategy = (times) => {
   return Math.min(times * 50, 2000);
}



class RedisFactory {
	constructor(url, opts) {
		// opts = opts || {};
		this.url = url;
		let poolOpts = opts.pool || {}
		this.poolOptions = opts;
	}

	async create() {
		return new Redis(this.url);
	}

	destroy(client) {
		client.disconnect();
	}
}


class DB {
	constructor(config) {
		config = config || {};
		
		let indexServer = 	config.indexServer 
							|| process.env.SPREDIS_INDEX_SERVER 
							|| 'redis://localhost:6379';
							// || '/tmp/redis.sock';

		let lockServer = 	config.lockServer 
							|| process.env.SPREDIS_LOCK_SERVER 
							|| indexServer
							|| 'redis://localhost:6379';
							// || '/tmp/redis.sock';

		let queryServers = 	config.queryServers 
							|| process.env.SPREDIS_QUERY_SERVERS 
							// || '/tmp/redis.sock';
							|| 'redis://localhost:6379';
							// || 'redis://localhost:6379,redis://localhost:6380';
							// || 'redis://localhost:6380';
							// || 'redis://localhost:6380,redis://localhost:6382';
							// || 'redis://localhost:6380,redis://localhost:6381,redis://localhost:6382';
							// || 'redis://localhost:6379,redis://localhost:6380,redis://localhost:6381,redis://localhost:6382';


		if (!config.pool) {
			config.pool = {min:1, max:8, idleTimeoutMillis: 30000};
		}
		if (config.pool.min < 0) config.pool.min = 0;
		if (config.pool.max < config.pool.min) config.pool.max = config.pool.min;
		if (config.pool.max < 1) config.pool.max = 1;
		config.pool.idleTimeoutMillis = config.pool.idleTimeoutMillis || 30000;
		if (config.pool.idleTimeoutMillis <= 0) config.pool.idleTimeoutMillis = 30000;

		if (_.isString(queryServers)) {
			queryServers = queryServers.split(',');
			queryServers = _.map(queryServers, _.trim)
		}
		this.queryServers = _.uniq(queryServers);
		this.queryPools = {};

		// this.singleServerMode = false;
		// if (queryServers.length === 0 ||
		// 		 (queryServers.length === 1 && indexServer === queryServers[0])) {
		// 		this.singleServerMode = true;
		// }
		this._indexServer = new Redis(indexServer);
		this._lockServer = new Redis(lockServer);
		_.forEach(this.queryServers, (url) => {
			let factory = new RedisFactory(url, config.pool);
			this.queryPools[url] = pooler.createPool(factory, factory.poolOptions)
		});
		// this._queryServers = _.map(queryServers, (url) => {
		// 	let r = new Redis(url);
		// 	// r.config('SET', 'slave-read-only', 'no').then(() => {
		// 	// 	console.log('configured query server')
		// 	// }).catch(e=> {
		// 	// 	console.error(e.stack);
		// 	// })
		// 	return r;
		// });

		this._QI = 0;
		
	}

	indexServer() {
		return this._indexServer;
	}

	lockServer() {
		return this._lockServer;
	}

	_querier() {
		let url = this.queryServers[this._QI];
		this._QI = this._QI < this.queryServers.length - 1 ? this._QI + 1 : 0;
		return url;
	}

	async grabQueryConnection() {
		let pool = this.queryPools[this._querier()];
		let conn = await pool.acquire();
		conn.__pool = pool;
		return conn;
	}

	releaseQueryConnection(conn) {
		if (conn.__pool) {
			conn.__pool.release(conn);
			conn.__pool = null;
		}
	}

	queryServer() {
		return this._querier();
	}

	quit() {
		this._indexServer && this._indexServer.quit();
		this._lockServer && this._lockServer.quit();
		_.forEach(this.queryPools, (s) => {
			s.drain().then(function() {
			  s.clear();
			});
		})
	}

}

module.exports = function(config) {
	return new DB(config);
}