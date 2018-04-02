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

	toSort: (val) => {
		return parseInt(val);
	},
	actualValue: (val) => {
		return parseInt(val);
	},
	toText: (val) => {
		return String(val);
	},

	index:true,
	searchType: 'R',
	supportsSort: true,
	supportsFacet: true,
	universal:true
}