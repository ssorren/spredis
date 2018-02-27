const Literal = require('./Literal');
const _ = require('lodash');
const constants = require('../constants');
const textUtils = require('../../text');
const clean = textUtils.clean;

module.exports = class HighCardStrategy extends Literal {
	setForSingleValue(pipe, val, lang, hint, cleanUp) {
		return this.lexSetForSingleValue(pipe, val, lang, hint, cleanUp);
	}
	

	unIndexField(pipe, id, val, pos) {
		var exists = this._exists(val);
		// pipe.call('spredis.dhashdel', this.sortIndex(this.lang), id);
		if (exists) {
			// pipe.zrem(this.prefixSearchIndex(this.lang),  this.toTempSort(val, id, pos));
			pipe.call('spredis.zlrem', this.prefixSearchIndex(this.lang),  id);
			pipe.call('spredis.zlrem', this.suffixSearchIndex(this.lang),  id);
			// let suffix = this.toSort(val).split("").reverse().join("");
			// pipe.zrem(this.suffixSearchIndex(this.lang), this.toTempSort(suffix, id, pos)); 
		}
	}
	toLexValue(val, id, pos) {
		return [clean(val), this.toLex(id,pos)].join(constants.SEP);
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
			let lexValue = this.toLexValue(val, id, pos)
			// pipe.zadd(this.prefixSearchIndex(this.lang), 0, lexValue);
			pipe.call('spredis.zladd', this.prefixSearchIndex(this.lang), id, 0, clean(val)); 
			// console.log(this.dirtySortKey)

			if (this.type.sort) {
				// let unsorted = this.unsortedValuesIndex(this.lang);
				// pipe.zadd(unsorted, 0, lexValue);
				pipe.sadd(this.dirtySortKey, this.prefixSearchIndex(this.lang));

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
