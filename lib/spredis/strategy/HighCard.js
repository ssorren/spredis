const Literal = require('./Literal');
const _ = require('lodash');
const constants = require('../constants');
const textUtils = require('../../text');
const clean = textUtils.clean;

module.exports = class HighCardStrategy extends Literal {
	
	setForSingleValue(pipe, val, lang, hint, cleanUp) {

		val = clean(String(val));
		let prefixStore = null;
		let suffixStore = null;

		if (textUtils.isPrefixValue(val)) {
			prefixStore = this.getTempIndexName(cleanUp);
			let v = this.trimWildCard(val) + '*'; //may be a suffix search as well
			let indexName = this.prefixSearchIndex(lang);
			pipe.storeLexRange(indexName, prefixStore, v);
		}
		
		if (this.type.suffix && textUtils.isSuffixValue(val)) {
			suffixStore = this.getTempIndexName(cleanUp);
			let v = this.trimWildCard(val).split('').reverse().join('') +'*'; //may be a prefix search as well
			let indexName = this.suffixSearchIndex(lang);
			pipe.storeLexRange(indexName, suffixStore, v);
		}

		if (prefixStore || suffixStore) {
			if (hint) {
				let store = this.getTempIndexName(cleanUp);
				if (!prefixStore) {
					pipe.szinterstore(store, 2, hint, suffixStore);
					return store;
				}
				if (!suffixStore) {
					pipe.szinterstore(store, 2, hint, prefixStore);
					return store;
				}
				let combined = this.getTempIndexName(cleanUp);
				pipe.szunionstore(combined,2,prefixStore, suffixStore);
				pipe.szinterstore(store, 2, hint, combined);
				return store;
			}
			if (prefixStore && suffixStore) {
				let store = this.getTempIndexName(cleanUp);
				pipe.szunionstore(store, 2, prefixStore, suffixStore);
				return store
			}
			return prefixStore || suffixStore;
		}
		let store = this.getTempIndexName(cleanUp);
		pipe.storeLexRange(this.prefixSearchIndex(lang), store, val);
		if (hint) {
			let store2 = this.getTempIndexName(cleanUp);
			pipe.szinterstore(store2, 2, store, hint);
			return store2;
		}
		return store;	
	}

	unIndexField(pipe, id, val, pos) {
		var exists = this._exists(val);
		pipe.zrem(this.sortIndex(this.lang), this.toLex(id, pos));
		if (exists) {
			pipe.zrem(this.prefixSearchIndex(this.lang),  this.toTempSort(val, id, pos)); 
			let suffix = this.toSort(val).split("").reverse().join("");
			pipe.zrem(this.suffixSearchIndex(this.lang), this.toTempSort(suffix, id, pos)); 
		}
	}
	toLexValue(val, id, pos) {
		return [clean(val), this.toLex(id,pos)].join(constants.SEP);
	}

	indexField(pipe, id, val, pos) {
		let exists = this._exists(val);
		if (this.type.sort) {
			pipe.zadd(this.sortIndex(this.lang), exists ? -1 : -2, id);
		}
		if (exists) {
			// pipe.zadd(this.tempSortValueKey(this.lang), '+inf', this.toTempSort(val, id, pos));
			// let pIndex = this.prefixSearchIndex(this.lang);
			//NX options is important here. to save space, we're re-using the sort index for literals, don't want to overwrite the score
			let lexValue = this.toLexValue(val, id, pos)
			pipe.zadd(this.prefixSearchIndex(this.lang), 0, lexValue); 
			// console.log(this.dirtySortKey)

			if (this.type.sort) {
				let unsorted = this.unsortedValuesIndex(this.lang);
				pipe.zadd(unsorted, 0, lexValue);
				pipe.sadd(this.dirtySortKey, [this.prefixSearchIndex(this.lang), this.sortIndex(this.lang), unsorted].join(','));

			}

			if (this.type.supportsSuffix && this.type.suffix) {
				let suffix = this.toSort(val).split("").reverse().join("");
				//NX options is important here. to save space, we're re-using the sort index for literals, don't want to overwrite the score
				pipe.zadd(this.suffixSearchIndex(this.lang), 0, this.toLexValue(suffix, id, pos)); 
			}
		}
		return 1
	}	
}
