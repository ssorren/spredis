const Redis = require('ioredis');
const _ = require('lodash');

class DB {
	constructor(config) {
		config = config || {};
		let indexServer = 	config.indexServer 
							|| process.env.SPREDIS_INDEX_SERVER 
							|| 'redis://localhost:6379';

		let lockServer = 	config.lockServer 
							|| process.env.SPREDIS_LOCK_SERVER 
							|| indexServer
							|| 'redis://localhost:6379';

		let queryServers = 	config.queryServers 
							|| process.env.SPREDIS_QUERY_SERVERS 
							|| 'redis://localhost:6379';
							// || 'redis://localhost:6380,redis://localhost:6381,redis://localhost:6382';

		if (_.isString(queryServers)) {
			queryServers = queryServers.split(',');
			queryServers = _.map(queryServers, _.trim)
		}
		queryServers = _.uniq(queryServers);

		this.singleServerMode = false;
		if (queryServers.length === 0 ||
				 (queryServers.length === 1 && indexServer === queryServers[0])) {
				this.singleServerMode = true;
		}
		this._indexServer = new Redis(indexServer);
		require('../../lua/load-scripts')(this._indexServer);
		this._lockServer = new Redis(lockServer);
		this._queryServers = _.map(queryServers, (url) => {
			let r = new Redis(url);
			require('../../lua/load-scripts')(r);
			r.config('SET', 'slave-read-only', 'no').then(() => {
				console.log('configured query server')
			})
			return r;
		});

		this._QI = 0;
		
	}

	indexServer() {
		return this._indexServer;
	}

	lockServer() {
		return this._lockServer;
	}

	_querier() {
		let redis = this._queryServers[this._QI];
		this._QI = this._QI < this._queryServers.length - 1 ? this._QI + 1 : 0;
		return redis;
	}

	queryServer() {
		return this.singleServerMode ? this._indexServer : this._querier();
	}

	quit() {
		this._indexServer && this._indexServer.quit();
		this._lockServer && this._lockServer.quit();
		_.forEach(this._queryServers, (s) => {
			s.quit();
		})
	}

}

module.exports = function(config) {
	return new DB(config);
}