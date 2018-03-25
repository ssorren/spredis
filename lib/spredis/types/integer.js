var SEP = require('../constants').SEP;
const utils = require('../../utils');

module.exports = {
	toRank: (val) => {
		return parseInt(val);
	},
	toLex: (id, pos) => {
		return id;
		// return pos ? String(id) + ';' + String(pos): String(id);
	},
	idFromLex: (lex) => {
		return lex;
	},
	toSort: (val) => {
		return parseInt(val);
	},
	actualValue: (val) => {
		return parseInt(val);
	},
	toText: (val) => {
		return String(val);
	},
	toAlphaSort: (val) => {
		return utils.integerToAscAlphaSort(val, 54);
	},
	toDescAlphaSort: (val) => {
		return utils.integerToDescAlphaSort(val, 54);
	},
	index:true,
	searchType: 'R',
	supportsSort: true,
	supportsFacet: true,
	universal:true
}