const Literal = require('./Literal');
const tolex = require('../../text').lex
const _ = require('lodash');
const textUtils = require('../../text');

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

	unIndexField(pipe, id, tokens, pos, lang='en') {
		if (tokens && tokens.length) {
			tokens = textUtils.filter(tokens, lang);
			for (var i = tokens.length - 1; i >= 0; i--) {
				let val = tokens[i]
				super.unIndexField(pipe, id, val, pos);
			};
		}
	}

	indexField(pipe, id, tokens, pos, lang='en') {
		if (tokens && tokens.length) {
			tokens = textUtils.filter(tokens, lang);
			for (let i = tokens.length - 1; i >= 0; i--) {
				let val = tokens[i];
				super.indexField(pipe, id, val, pos);
			};
		}
		return 1
	}
}