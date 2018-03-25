var Range = require('./Range');
var _ = require('lodash');

module.exports = class BooleanStrategy extends Range {
	getQueryValue(val) {
		return  val ? '1' : '0';
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
}
