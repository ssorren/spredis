const Strategy = require('./Strategy');
var _ = require('lodash');

module.exports = class RangeStrategy extends Strategy {
	getQueryValue(val, min) {
		if (val === null || val === undefined || val === '') {
			return min ? '(-inf' : '+inf';
		}
		return this.actualValue(val);
	}

	get lang() {
		return 'M';
	}

	set lang(l) {

	}

	sortIndex(lang) {
		//ignore language from the query
		return this.indexName + ':V:' + this.lang;
	}

	indexForValue(val, lang) {
		//ignore language for number type fields, will alway be 'M' for
		return this.indexName + ':V:' + this.lang;
	}

	setForSingleValue(pipe, val, lang, hint, cleanUp) {
		// if (val === true) {
		// 	index: this.indexForValue(val);
		// }
		let indexName = this.indexForValue(val, lang);
		let store = this.getTempIndexName(cleanUp);
		if (_.isString(val) || _.isNumber(val) || val === true || val === false) {
			// console.log('spredis.zlinkset', indexName, store, this.getQueryValue(val));

			pipe.call('spredis.zlinkset', indexName, store, this.getQueryValue(val));
			if (hint) {
				let store2 = this.getTempIndexName(cleanUp);
				pipe.call('spredis.stinterstore', store2, store, hint);
				return store2;
			}
			return store;
		}
		if (val.min === null && val.max === null) return hint;
		val = {min:this.getQueryValue(val.min,true), max:this.getQueryValue(val.max, false)};
		// console.log('spredis.storerangebyscore', store, indexName, hint || '' , val.min, val.max);
		
		pipe.call('spredis.storerangebyscore', store, indexName, hint || '' , val.min, val.max);
		return store;	
	}



	unIndexField(pipe, id, val, pos) {
		let exists = this._exists(val);
		let indexName = this.indexForValue(val, this.lang);
		let rank = exists ? this.toRank(val) : '-inf';
		pipe.call('spredis.zrem', indexName, this.toLex(id, pos), rank);
	}

	indexField(pipe, id, val, pos) {
		let exists = this._exists(val);
		
		let rank = exists ? this.toRank(val) : '-inf';
		let indexName = this.indexForValue(val, this.lang);
					
		pipe.call('spredis.zadd',indexName, this.toLex(id, pos), rank);
		return 1
	}
}