var SEP = require('../constants').SEP;
const tolex = require('../../text').lex;
const utils = require('../../utils');

module.exports = {
	toRank: (val) => {
		return 1;
	},
	toLex: (id, pos) => {
		return id;
		// return pos ? String(id) + ';' + String(pos): String(id);;
	},
	idFromLex: (lex) => {
		return lex;
	},
	toSort: (val) => {
		return tolex(val);
	},
	actualValue: (val) => {
		return String(val);
	},
	toText: (val) => {
		return String(val);
	},
	supportsSource: true,
	alpha: true,
	index:true,
	array: false,
	prefix: true,
	suffix: false,
	supportsPrefix: true,
	supportsSuffix: true,
	searchType: 'T',
	supportsSort: false,
	supportsFacet: false,
	universal:false,
	stem: false
}