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

	toSort: (val) => {
		return 0;
	},
	actualValue: (val) => {
		return [parseFloat(val[0]), parseFloat(val[1])];
	},
	toText: (val) => {
		return null;
	},
	compositeType: 'GEO',
	supportsSource: false,
	index:true,
	sort: false,
	searchType: 'G',
	supportsSort: true,
	supportsFacet: false,
	universal:true,
	name: 'geo',
	serverType: 1,
	fullText: 0
}