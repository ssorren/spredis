/*
	a small generic text field.
*/

const Strategy = require('./Strategy');
const _ = require('lodash');
const textUtils = require('../../text');
const clean = textUtils.clean;

module.exports = class LiteralStrategy extends Strategy {
	getQueryValue(val) {
		return this.toSort(val);//String(val).toLowerCase();
	}

	trimWildCard(val) {
		return _.trim(val, '*');
	}
	
	toCompositeQuery(val) {
		val = clean(String(val));
		let wc = textUtils.isPrefixValue(val);
		if (wc) {
			return [val.substr(0, val.length - 1), 1];
		}
		return [val, 0];
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
			let v = this.trimWildCard(val); //may be a suffix search as well
			v = this.type.stem ? textUtils.stem(v, lang) : v;
			let indexName = this.prefixSearchIndex(lang);
			this._lexSearch(pipe, prefixStore, indexName, hint, v + '*');
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
			return prefixStore || suffixStore;
		}
		let store = this.getTempIndexName(cleanUp);
		val = this.type.stem ? textUtils.stem(val, lang) : val;
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
		let indexName = this.indexForValue(val, lang);
		let store = this.getTempIndexName(cleanUp);
		val = this.getQueryValue(val);
		val = this.type.stem ? textUtils.stem(val, lang) : val;
		pipe.call('spredis.zllinkset', indexName, store, val);
		return store;
	}

	prefixSearchIndex(lang) {
		return this.indexForValue(null, lang);
	}

	suffixSearchIndex(lang) {
		return this.indexName + ':SUFFIX:'+ this.resolveLang(lang);
	}

	indexForValue(val, lang) {
		return this.indexName + ':V:' + this.resolveLang(lang);
	}

	unIndexField(pipe, id, val, pos, lang='en') {
		var exists = this._exists(val);
		if (exists) {
			var indexName = this.indexForValue(val, lang);
			let indexVal = clean(val);
			indexVal = this.type.stem ? textUtils.stem(indexVal, lang) : indexVal;

			pipe.call('spredis.zlrem', this.indexForValue(val, lang), id, indexVal);
			if (this.type.supportsSuffix && this.type.suffix) {
				let suffix = clean(val).split("").reverse().join("");
				pipe.call('spredis.zlrem', this.suffixSearchIndex(lang), id, suffix);
			}

			if (this.type.supportsFacet && this.type.facet) {
				pipe.call('spredis.hdel',this.facetName(lang), id, pos);
			}

		}
	}

	indexField(pipe, id, val, pos, lang='en') {
		let exists = this._exists(val);

		if (this._exists(val)) {
			let doSort = this.type.supportsSort && this.type.sort ? 1 : 0;
			let indexVal = clean(val);
			indexVal = this.type.stem ? textUtils.stem(indexVal, lang) : indexVal;
			pipe.call('spredis.zladd', this.indexForValue(val, lang), id, doSort, indexVal); 

			if (this.type.supportsFacet && this.type.facet) {
				pipe.call('spredis.hsetstr',this.facetName(lang), id, pos, this.actualValue(val));
			}

			if (this.type.supportsSuffix && this.type.suffix) {
				let suffix = clean(val).split("").reverse().join("");
				pipe.call('spredis.zladd', this.suffixSearchIndex(lang), id, 0, suffix);
			}
		}
		return 1
	}

}