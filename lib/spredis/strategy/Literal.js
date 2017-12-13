const Strategy = require('./Strategy');
// var SEP = require('../constants').SEP
var _ = require('lodash');
module.exports = class LiteralStrategy extends Strategy {
	getQueryValue(val) {
		return this.toSort(val);//String(val).toLowerCase();
	}


	setForSingleValue(pipe, val, lang, hint, cleanUp) {
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
}