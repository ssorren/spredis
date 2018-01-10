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

	setForSingleValue(pipe, val, lang, hint, cleanUp) {
		//check for prefix search

		let pWildCardIndex = null;
		if (this.type.prefix && textUtils.isPrefixValue(val)) {
			let v = this.trimWildCard(clean(val));
			pWildCardIndex = this.getTempIndexName(cleanUp);
			// cleanUp.push(pWildCardIndex);
			pipe.wildCardSet(this.prefixSearchIndex(lang), pWildCardIndex, this.specificValuePrefix(v,lang), v, 0, hint);
		}

		//check for suffix search
		//TODO: some logic for reverseing string in LUA, this doesn't work right now
		let sWildCardIndex = null;
		if (this.type.prefix && textUtils.isSuffixValue(val)) {
			let v = this.trimWildCard(clean(val));
			sWildCardIndex = this.getTempIndexName(cleanUp);
			// cleanUp.push(sWildCardIndex);
			pipe.wildCardSet(this.prefixSearchIndex(lang), sWildCardIndex, this.specificValuePrefix(v,lang), v, 0, hint);
		}

		//both a prefix and a suffix
		if (pWildCardIndex && sWildCardIndex) {
			let t = this.getTempIndexName(cleanUp);
			// cleanUp.push(t);
			pipe.zunionstore(t, 2, pWildCardIndex, sWildCardIndex)
			return t;
		}
		if (pWildCardIndex) return pWildCardIndex;
		if (sWildCardIndex) return sWildCardIndex;

		if (hint) {
			let store = this.getTempIndexName(cleanUp);
			// let store2 = this.getTempIndexName();
			// pipe.zinterstore(store, 2, hint, this.indexForSpecificValue(val, lang));
			pipe.call('spredis.stinterstore', store, hint, this.indexForSpecificValue(val, lang));
			
			// pipe.convertToSet(store, store2)
			// cleanUp.push(store, store2);
			// return store2;
			// console.log('WTF')
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
		return this.sortAllValuesName(lang);
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
			pipe.zrem(indexName, this.toLex(id, pos));
			if (specIndexName) pipe.call('spredis.srem', specIndexName, this.toLex(id, pos));
			pipe.call('spredis.hashdel',this.facetName(this.lang), id, this.actualValue(val));
			pipe.call('spredis.dhashdel', this.sortIndex(this.lang), this.toLex(id, pos));
			// pipe.del(this.ascSortWeightKey + this.toLex(id, pos));
			// pipe.del(this.descSortWeightKey + this.toLex(id, pos));
		}
	}

	indexField(pipe, id, val, pos) {
		let exists = this._exists(val);
		
		if (this.type.sort && !this.type.array) {
			// this.setAscSortValue(pipe, id, val, pos);
			// this.setDescSortValue(pipe, id, val, pos);

			if (this.type.sortStrategy === 'L') {
				let v = this._exists(val) ? this.toSort(val) : '';
				pipe.storeSort(
					this.sortAllValuesName(this.lang),
					this.sortIndex(this.lang),
					v,
					this.toLex(id, pos),
					String(this.type.alpha ? 1 : 0)
				);
			} else {
				let v = this._exists(val) ? this.toSort(val) : 0;
				pipe.call('spredis.dhashset', this.sortIndex(this.lang), this.toLex(id, pos), v)
			}

		}
		
		if (exists) {
			let rank = this.toRank(val);
			let specIndexName = this.indexForSpecificValue(val, this.lang);
			if (specIndexName) {
				pipe.call('spredis.sadd', specIndexName, this.toLex(id, pos));
			} else {
				pipe.zadd(this.indexForValue(val, this.lang), rank, this.toLex(id, pos));	
			}

			if (this.type.facet) {
				pipe.call('spredis.hashset',this.facetName(this.lang), id, this.actualValue(val));
			}
			if (this.type.supportsPrefix && this.type.prefix) {
				let pIndex = this.prefixSearchIndex(this.lang);
				//NX options is important here. to save space, we're re-using the sort index for literals, don't want to overwrite the score
				if (pIndex) pipe.zadd(pIndex, 'NX', 0, this.toSort(val)); 

			}

			if (this.type.supportsSuffix && this.type.suffix) {
				let sIndex = this.suffixSearchIndex(this.lang);
				let suffix = this.toSort(val).split("").reverse().join("");
				//NX options is important here. to save space, we're re-using the sort index for literals, don't want to overwrite the score
				if (sIndex) pipe.zadd(sIndex, 'NX', 0, suffix); 

			}
		}
		return 1
	}
}