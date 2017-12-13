var SEP = require('../constants').SEP;
const utils = require('../../utils');

module.exports = {
	toRank: (val) => {
		return [parseFloat(val[0]), parseFloat(val[1])];
	},
	toLex: (id, pos) => {
		return id;
		// return pos ? String(id) + ';' + String(pos): String(id);
	},
	idFromLex: (lex) => {
		return lex;
	},
	toSort: (val) => {
		return 0;
	},
	actualValue: (val) => {
		return [parseFloat(val[0]), parseFloat(val[1])];
	},
	toText: (val) => {
		return null;
	},
	supportsSource: false,
	index:true,
	sort: false,
	searchType: 'G'
}