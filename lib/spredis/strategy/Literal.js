const Strategy = require('./Strategy');
// var SEP = require('../constants').SEP
var _ = require('lodash');
const textUtils = require('../../text');
const clean = textUtils.clean;
module.exports = class LiteralStrategy extends Strategy {
	getQueryValue(val) {
		return this.toSort(val);//String(val).toLowerCase();
	}

	trimWildCard(val) {
		return _.trim(val, '*');
	}
	
	_lexSearch(pipe, store, index, hint, value) {
		let start, stop;
		if (_.endsWith(value, '*')) {
			value = value.substr(0, value.length - 1);
			start = '[' + value;
			stop = '[' + value + '\\xff';
		} else {
			start = '[' + value;
			stop = '[' + value;
		}
		pipe.call('spredis.storerangebylex', store, index, hint, start, stop);
	}

	lexSetForSingleValue(pipe, val, lang, hint, cleanUp) {
		hint = hint || '';
		val = clean(String(val));
		let prefixStore = null;
		let suffixStore = null;

		if (textUtils.isPrefixValue(val)) {
			prefixStore = this.getTempIndexName(cleanUp);
			let v = this.trimWildCard(val) + '*'; //may be a suffix search as well
			let indexName = this.prefixSearchIndex(lang);
			// console.log(prefixStore, indexName, hint, v)
			this._lexSearch(pipe, prefixStore, indexName, hint, v);
		}
		
		if (this.type.suffix && textUtils.isSuffixValue(val)) {
			suffixStore = this.getTempIndexName(cleanUp);
			let v = this.trimWildCard(val).split('').reverse().join('') +'*'; //may be a prefix search as well
			let indexName = this.suffixSearchIndex(lang);
			this._lexSearch(pipe, suffixStore, indexName, hint, v);
		}

		if (prefixStore || suffixStore) {
			if (prefixStore && suffixStore) {
				let store = this.getTempIndexName(cleanUp);
				pipe.call('spredis.stinterstore', store, prefixStore, suffixStore);
				return store
			}
			// console.log("returning", prefixStore || suffixStore);
			return prefixStore || suffixStore;
		}
		let store = this.getTempIndexName(cleanUp);
		this._lexSearch(pipe, store, this.prefixSearchIndex(lang), hint, val);
		return store;	
	}

	setForSingleValue(pipe, val, lang, hint, cleanUp) {
		if ((this.type.prefix && textUtils.isPrefixValue(val)) || (this.type.suffix && textUtils.isSuffixValue(val))) {
			return this.lexSetForSingleValue(pipe, val, lang, hint, cleanUp);
		}
		if (hint) {
			let store = this.getTempIndexName(cleanUp);
			pipe.call('spredis.stinterstore', store, hint, this.indexForSpecificValue(pipe, val, lang, cleanUp));
			return store;
		}
		return this.indexForSpecificValue(pipe, val, lang, cleanUp);
	}

	getValFromSource(doc, fieldStrategies) {
		let fieldStrategy = fieldStrategies[this.source];
		let v = doc[this.source]; 
		return this._exists(v) ? v : ((fieldStrategy) ? fieldStrategy.defaultValue : null);
	}

	indexForSpecificValue(pipe, val, lang, cleanUp) {
		let indexName = this.indexForValue(val, this.lang);
		let store = this.getTempIndexName(cleanUp);
		pipe.call('spredis.zllinkset', indexName, store, this.getQueryValue(val));
		return store;
	}

	prefixSearchIndex(lang) {
		return this.indexForValue(null, lang);
	}

	suffixSearchIndex(lang) {
		return this.indexName + ':SUFFIX:'+ lang;
	}

	unsortedValuesIndex(lang) {
		return this.indexName + ':UNSORTED:'+ lang;	
	}

	unIndexField(pipe, id, val, pos) {
		var exists = this._exists(val);
		if (exists) {
			var indexName = this.indexForValue(val, this.lang);
			// var specIndexName = this.indexForSpecificValue(val);
			// pipe.zrem(indexName, this.toLex(id, pos));
			// pipe.call('spredis.zrem', indexName, id);
			let suffix = clean(val).split("").reverse().join("");
			pipe.call('spredis.zlrem', this.indexForValue(val, this.lang), id, clean(val));
			if (this.type.supportsSuffix && this.type.suffix) {
				pipe.call('spredis.zlrem', this.suffixSearchIndex(this.lang), id, suffix);
			}

			// if (specIndexName) pipe.call('spredis.srem', specIndexName, id);
			if (this.type.supportsFacet && this.type.facet) {
				pipe.call('spredis.hdel',this.facetName(this.lang), id, pos);
			}
			// pipe.call('spredis.dhashdel', this.sortIndex(this.lang), this.toLex(id, pos));
			// pipe.del(this.ascSortWeightKey + this.toLex(id, pos));
			// pipe.del(this.descSortWeightKey + this.toLex(id, pos));
		}
	}

	indexField(pipe, id, val, pos) {
		let exists = this._exists(val);
		// if (this.type.sort) {
		// 	pipe.call('spredis.dhashset', this.sortIndex(this.lang), id, exists ? '+inf' : '-inf');
		// }
		if (this._exists(val)) {
			// pipe.zadd(this.tempSortValueKey(this.lang), '+inf', this.toTempSort(val, id, pos));
			// let pIndex = this.prefixSearchIndex(this.lang);
			//NX options is important here. to save space, we're re-using the sort index for literals, don't want to overwrite the score
			// let lexValue = this.toLexValue(val, id, pos)
			// pipe.zadd(this.prefixSearchIndex(this.lang), 0, lexValue);
			let doSort = this.type.supportsSort && this.type.sort ? 1 : 0;
			pipe.call('spredis.zladd', this.indexForValue(val, this.lang), id, doSort, clean(val)); 
			// console.log(this.dirtySortKey)

			if (this.type.supportsFacet && this.type.facet) {
				pipe.call('spredis.hsetstr',this.facetName(this.lang), id, pos, this.actualValue(val));
			}

			if (doSort) {
				pipe.sadd(this.dirtySortKey, this.indexForValue(val, this.lang));
			}

			if (this.type.supportsSuffix && this.type.suffix) {
				let suffix = clean(val).split("").reverse().join("");
				//NX options is important here. to save space, we're re-using the sort index for literals, don't want to overwrite the score
				// pipe.zadd(this.suffixSearchIndex(this.lang), 0, this.toLexValue(suffix, id, pos)); 
				pipe.call('spredis.zladd', this.suffixSearchIndex(this.lang), id, 0, suffix);
			}
		}
		return 1
	}

}