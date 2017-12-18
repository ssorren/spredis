var Literal = require('./Literal');
var _ = require('lodash');

module.exports = class BooleanStrategy extends Literal {
	getQueryValue(val) {
		return  val ? '1' : '0';
	}

	get lang() {
		return 'M';
	}

	set lang(l) {

	}

	_exists(val) {
		return true;
	}

	prefixSearchIndex(lang) {
		return null;
	}

	suffixSearchIndex(lang) {
		return null;
	}
	
	getValFromSource(doc, fieldStrategies) {
		let fieldStrategy = fieldStrategies[this.source];
		let v = doc[this.source];
		return this.actualValue(v);
		// return this._exists(v) ? v : ((fieldStrategy) ? fieldStrategy.defaultValue : null);
	}

	indexForValue(val, lang) {
		//ignore language for number type fields, will alway be 'M' for math
		return this.indexName + ':V:' + this.lang;
	}

	specificValuePrefix(val, lang) {
		//ignore language for number type fields, will alway be 'M' for math
		return this.indexName + ':V:' + this.lang + ":"
	}

	sortIndex(lang) {
		return this.indexName + ':SALL:' + this.lang;
	}
}
