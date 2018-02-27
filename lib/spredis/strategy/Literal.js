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
			pipe.call('spredis.stinterstore', store, hint, this.indexForSpecificValue(val, lang));
			return store;
		}
		return this.indexForSpecificValue(val, lang);
	}

	getValFromSource(doc, fieldStrategies) {
		let fieldStrategy = fieldStrategies[this.source];
		let v = doc[this.source]; 
		return this._exists(v) ? v : ((fieldStrategy) ? fieldStrategy.defaultValue : null);
	}

	specificValuePrefix(val, lang) {
		return this.indexName + ':V:' + lang + ":"
	}

	indexForSpecificValue(val, lang) {
		return this.specificValuePrefix(val, lang) + this.getQueryValue(val);
	}

	prefixSearchIndex(lang) {
		return this.sortIndex(lang);
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
			var specIndexName = this.indexForSpecificValue(val);
			// pipe.zrem(indexName, this.toLex(id, pos));
			pipe.call('spredis.zrem', indexName, id);
			pipe.call('spredis.zlrem', this.prefixSearchIndex(this.lang), id);
			pipe.call('spredis.zlrem', this.suffixSearchIndex(this.lang), id);


			if (specIndexName) pipe.call('spredis.srem', specIndexName, id);

			pipe.call('spredis.hdel',this.facetName(this.lang), id, pos);
			// pipe.call('spredis.dhashdel', this.sortIndex(this.lang), this.toLex(id, pos));
			// pipe.del(this.ascSortWeightKey + this.toLex(id, pos));
			// pipe.del(this.descSortWeightKey + this.toLex(id, pos));
		}
	}

	indexField(pipe, id, val, pos) {
		let exists = this._exists(val);
		
		if (this.type.sort && !this.type.array) {
			// this.setAscSortValue(pipe, id, val, pos);
			// this.setDescSortValue(pipe, id, val, pos);

			if (this.type.sortStrategy === 'L' && this.type.sort) {
				let v = this._exists(val) ? this.toSort(val) : '';
				// pipe.storeSort(
				// 	this.sortAllValuesName(this.lang),
				// 	this.sortIndex(this.lang),
				// 	v,
				// 	this.toLex(id, pos),
				// 	String(this.type.alpha ? 1 : 0)
				// );
			} else {
				let v = this._exists(val) ? this.toSort(val) : 0;
				// pipe.call('spredis.dhashset', this.sortIndex(this.lang), this.toLex(id, pos), v)
			}

		}
		
		if (exists) {
			let rank = this.toRank(val);
			let specIndexName = this.indexForSpecificValue(val, this.lang);
			if (specIndexName) {
				pipe.call('spredis.sadd', specIndexName, id);
			} else {

				pipe.call('spredis.zadd',this.indexForValue(val, this.lang), id, rank)

				// pipe.zadd(this.indexForValue(val, this.lang), rank, this.toLex(id, pos));
			}

			if (this.type.facet) {
				pipe.call('spredis.hsetstr',this.facetName(this.lang), id, pos, this.actualValue(val));
			}
			
			if ( (this.type.supportsPrefix && this.type.prefix) || (this.type.sortStrategy === 'L' && this.type.sort) ) {
				let pIndex = this.prefixSearchIndex(this.lang);
				//NX options is important here. to save space, we're re-using the sort index for literals, don't want to overwrite the score
				// if (pIndex) pipe.zadd(pIndex, 'NX', 0, this.toSort(val)); 
				pipe.call('spredis.zladd', this.prefixSearchIndex(this.lang), id, 0, this.toSort(val));
				if (this.type.sort) {
					// console.log('pushing sort!', this.dirtySortKey, this.prefixSearchIndex(this.lang));
					pipe.sadd(this.dirtySortKey, this.prefixSearchIndex(this.lang));
				}

			}

			if (this.type.supportsSuffix && this.type.suffix) {
				let sIndex = this.suffixSearchIndex(this.lang);
				let suffix = this.toSort(val).split("").reverse().join("");
				//NX options is important here. to save space, we're re-using the sort index for literals, don't want to overwrite the score
				// if (sIndex) pipe.zadd(sIndex, 'NX', 0, suffix); 
				pipe.call('spredis.zladd', sIndex, id, 0, suffix); 

			}
		}
		return 1
	}
}