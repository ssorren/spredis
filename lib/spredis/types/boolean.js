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
	
	fromString: (s) => {
		return parseInt(s) ? true : false;
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
	compositeType: 'DBL',
	index:true,
	array: false,
	supportsSource: true,
	supportsFacet: true,
	searchType: 'L',
	supportsSort: true,
	universal:true,
	name: 'boolean'
}