const Literal = require('./Literal');
const tolex = require('../../text').lex
const _ = require('lodash');

module.exports = class TextStrategy extends Literal {

	

	get _sources() {
		if (!this.__sources && this.source) {
			this.__sources = this.source.split(',');
			_.remove(this.__sources, (s) => {
				return s === '' || s === null || s === undefined;
			})
		}
		return this.__sources || [];
	}

	prefixSearchIndex(lang) {
		return this.indexName + ':PREFIX:'+ lang;;
	}

	suffixSearchIndex(lang) {
		return this.indexName + ':SUFFIX:'+ lang;;
	}

	getValFromSource(doc, fieldStrategies) {
		let s = this._sources;
		let a = [];
		for (var i = 0; i < s.length; i++) {
			let fieldStrategy = fieldStrategies[s[i]];
			let v = doc[s[i]]
			v = this._exists(v) ? v : ((fieldStrategy) ? fieldStrategy.defaultValue : null);
			if (this._exists(v)) {
				v = tolex(String(v)).split(/\s/);
				a.push.apply(a, v);
			}
		};
		return a;
	}

	
	unIndexField(pipe, id, tokens, pos) {
		if (tokens && tokens.length) {
			for (var i = tokens.length - 1; i >= 0; i--) {
				let val = tokens[i]
				let rank = this.toRank(val);
				let specIndexName = this.indexForSpecificValue(val, this.lang);
				pipe.srem(specIndexName, this.toLex(id));
			};
		}
	}

	indexField(pipe, id, tokens, pos) {
	
		if (tokens && tokens.length) {
			for (let i = tokens.length - 1; i >= 0; i--) {
				let val = tokens[i];
				let rank = this.toRank(val);
				let specIndexName = this.indexForSpecificValue(val, this.lang);
				pipe.sadd(specIndexName, this.toLex(id));
				
				if (this.type.supportsPrefix && this.type.prefix) {
					let pIndex = this.prefixSearchIndex(this.lang);
					//NX options is important here. to save space, we're re-using the sort index for literals, don't want to overwrite the score
					if (pIndex) pipe.zaddBuffer(pIndex, 'NX', 0, this.toSort(val)); 

				}
				if (this.type.supportsSuffix && this.type.suffix) {
					let sIndex = this.suffixSearchIndex(this.lang);
					let suffix = this.toSort(val).split("").reverse().join("");
					//NX options is important here. to save space, we're re-using the sort index for literals, don't want to overwrite the score
					if (sIndex) pipe.zaddBuffer(sIndex, 'NX', 0, suffix); 

				}
			};
		}
		return 1
	}
}