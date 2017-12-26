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
		return this.indexForValue(null, lang);
	}

	indexForValue(val, lang) {
		//ignore language for number type fields, will alway be 'M' for
		return this.indexName + ':V:' + this.lang;
	}

	setForSingleValue(pipe, val, lang, hint, cleanUp) {
		// if (val === true) {
		// 	index: this.indexForValue(val);
		// }
		if (_.isString(val) || _.isNumber(val)) {
			val = {min:val, max:val};
		}
		if (val.min === null && val.max === null) return hint;
		val = {min:this.getQueryValue(val.min,true), max:this.getQueryValue(val.max, false)};
		let store = this.getTempIndexName(cleanUp);
		let indexName = this.indexForValue(val, lang);
		
		// if (hint) {
		// 	let hintStore = this.getTempIndexName(cleanUp);
			
		// 	pipe.zinterstore(hintStore, 2, hint, indexName, 'WEIGHTS', 0, 1);
		// 	pipe.storeRange(indexName, store, hint, val.min, val.max);
			
		// 	// pipe.zinterstore(hintStore, 2, hint, indexName, 'WEIGHTS', 0, 1);
		// 	// pipe.storeRange(indexName, store, val.min, val.max);

		// 	return store
		// }
		let hintStore = this.getTempIndexName(cleanUp);
		pipe.storeRange(indexName, store, hint, hintStore, val.min, val.max);
		return store;	
	}



	unIndexField(pipe, id, val, pos) {
		let indexName = this.indexForValue(val, this.lang);
		pipe.zrem(indexName, this.toLex(id, pos));
	}

	indexField(pipe, id, val, pos) {
		let exists = this._exists(val);
		
		let rank = exists ? this.toRank(val) : '-inf';
		let indexName = this.indexForValue(val, this.lang);
					
		pipe.zadd(indexName, rank, this.toLex(id, pos));
		return 1
	}
}