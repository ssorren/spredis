var SEP = require('../constants').SEP;
const utils = require('../../utils');

const max = Math.pow(2, 16);

function checkMax(val) {
	val = parseInt(val);
	return val > max ? max : val;
}
module.exports = {
	toRank: (val) => {
		return checkMax(val);
	},
	toLex: (id, pos) => {
		return id;
		// return pos ? String(id) + ';' + String(pos): String(id);
	},
	idFromLex: (lex) => {
		return lex;
	},
	toSort: (val) => {
		return checkMax(val);
	},
	actualValue: (val) => {
		return checkMax(val);
	},
	toText: (val) => {
		return String(checkMax(val));
	},
	toAlphaSort: (val) => {
		return utils.integerToAscAlphaSort(checkMax(val), 16);
	},
	toDescAlphaSort: (val) => {
		return utils.integerToDescAlphaSort(checkMax(val), 16);
	},
	index:true,
	searchType: 'R'
}