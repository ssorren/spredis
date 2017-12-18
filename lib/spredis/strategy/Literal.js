const Strategy = require('./Strategy');
// var SEP = require('../constants').SEP
var _ = require('lodash');
const textUtils = require('../../text');
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
			let v = this.trimWildCard(val);
			pWildCardIndex = this.getTempIndexName();
			cleanUp.push(pWildCardIndex);
			pipe.wildCardSet(this.prefixSearchIndex(lang), pWildCardIndex, this.specificValuePrefix(v,lang), v, 0, hint);
		}

		//check for suffix search
		let sWildCardIndex = null;
		if (this.type.prefix && textUtils.isSuffixValue(val)) {
			let v = this.trimWildCard(val);
			sWildCardIndex = this.getTempIndexName();
			cleanUp.push(sWildCardIndex);
			pipe.wildCardSet(this.prefixSearchIndex(lang), sWildCardIndex, this.specificValuePrefix(v,lang), v, 0, hint);
		}

		//both a prefix and a suffix
		if (pWildCardIndex && sWildCardIndex) {
			let t = this.getTempIndexName();
			cleanUp.push(t);
			pipe.zunionstoreBuffer(t, 2, pWildCardIndex, sWildCardIndex, 'WEIGHTS', 0, 0)
			return t;
		}
		if (pWildCardIndex) return pWildCardIndex;
		if (sWildCardIndex) return sWildCardIndex;

		if (hint) {
			let store = this.getTempIndexName();
			pipe.zinterstoreBuffer(store, 2, hint, this.indexForSpecificValue(val, lang), 'WEIGHTS', 0, 0);
			cleanUp.push(store);
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
}