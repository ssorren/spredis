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
	fromString: (s) => {
		return parseInt(s);
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
	compositeType: 'DBL',
	index:true,
	searchType: 'R',
	supportsSort: true,
	supportsFacet: true,
	universal:true,
	name: 'smallinteger',
	serverType: 2,
	fullText: 0
}