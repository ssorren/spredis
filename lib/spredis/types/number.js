var SEP = require('../constants').SEP;
const utils = require('../../utils');

module.exports = {
	toRank: (val, precision) => {
		return precision ? Math.round(parseFloat(val) * Math.pow(10, precision)) : parseFloat(val);
	},
	toLex: (id, pos) => {
		return id;
		// return pos ? String(id) + ';' + String(pos): String(id);
	},
	idFromLex: (lex) => {
		return lex;
	},
	toSort: (val, precision) => {
		return precision ? Math.round(parseFloat(val) * Math.pow(10, precision)) : parseFloat(val);
	},
	actualValue: (val) => {
		return parseFloat(val);
	},
	toText: (val) => {
		return String(val);
	},
	toAlphaSort: (val, precision) => {
		val = precision ? Math.round(parseFloat(val) * Math.pow(10, precision)) : Math.round(parseFloat(val));
		return utils.integerToAscAlphaSort(val, 63);
	},
	toDescAlphaSort: (val, precision) => {
		val = precision ? Math.round(parseFloat(val) * Math.pow(10, precision)) : Math.round(parseFloat(val));
		return utils.integerToDescAlphaSort(val, 63);
	},
	sortPrecision: 2,
	index:true,
	array: false,
	searchType: 'R'
}