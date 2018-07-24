/*
	the strategy for searching by document id
*/

const Strategy = require('./Strategy');
const _ = require('lodash');

module.exports = class DocumentIdStrategy extends Strategy {
	getQueryValue(val) {
		return val;//String(val).toLowerCase();
	}

	
	indexArguments(val) {
		return null;
	}
	
	toCompositeQuery(val) {
		return [val, 0];
	}

	indexForSearchValue(pipe, val, lang, hint, cleanUp) {
		val = _.isArray(val) ? val : [val];
		let store = this.getTempIndexName(cleanUp);
		pipe.call('spredis.storerecordsbyid', this.prefix, store, hint, ...val);
		return store;
	}

}