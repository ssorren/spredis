var SEP = require('../constants').SEP;
const utils = require('../../utils');
module.exports = {
	toRank: (val) => {
		return val ? 1 : 0;
	},
	toLex: (id, pos) => {
		return id;
		// return pos ? String(id) + ';' + String(pos): String(id);
	},
	idFromLex: (lex) => {
		return lex;
	},
	toSort: (val) => {
		return val ? 1 : 0;
	},
	actualValue: (val) => {
		return val ? 1 : 0;
	},
	toText: (val) => {
		return val ? '1' : '0';
	},
	toAlphaSort: (val) => {
		return val ? '1' : '0';
	},
	toDescAlphaSort: (val) => {
		return val ? '0' : '1';
	},
	index:true,
	array: false,
	supportsSource: true,
	searchType: 'L'
}