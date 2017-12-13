const Literal = require('./Literal');
const tolex = require('../../text').lex
const _ = require('lodash');

module.exports = class TextStrategy extends Literal {

	unIndexField(pipe, id, tokens, pos) {
		if (tokens && tokens.length) {
			for (var i = tokens.length - 1; i >= 0; i--) {
				let val = tokens[i]
				let rank = this.toRank(val);
				let specIndexName = this.indexForSpecificValue(val, this.lang);
				pipe.zremBuffer(specIndexName, rank, this.toLex(id));
			};
		}
	}

	get _sources() {
		if (!this.__sources && this.source) {
			this.__sources = this.source.split(',');
			_.remove(this.__sources, (s) => {
				return s === '' || s === null || s === undefined;
			})
		}
		return this.__sources || [];
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

	indexField(pipe, id, tokens, pos) {
	
		if (tokens && tokens.length) {
			for (let i = tokens.length - 1; i >= 0; i--) {
				let val = tokens[i];
				let rank = this.toRank(val);
				let specIndexName = this.indexForSpecificValue(val, this.lang);
				pipe.zaddBuffer(specIndexName, rank, this.toLex(id));
			};
		}
		return 1
	}
}