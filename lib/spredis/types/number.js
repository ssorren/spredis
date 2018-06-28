var SEP = require('../constants').SEP;
const utils = require('../../utils');

module.exports = {
	toRank: (val, precision) => {
		// return precision ? Math.round(parseFloat(val) * Math.pow(10, precision)) : parseFloat(val);
		return parseFloat(val);
	},
	toLex: (id, pos) => {
		return id;
		// return pos ? String(id) + ';' + String(pos): String(id);
	},
	fromString: (s) => {
		return Number(s);
	},
	toSort: (val, precision) => {
		// return precision ? Math.round(parseFloat(val) * Math.pow(10, precision)) : parseFloat(val);
		return parseFloat(val);
	},
	actualValue: (val) => {
		return parseFloat(val);
	},
	toText: (val) => {
		return String(val);
	},
	compositeType: 'DBL',
	sortPrecision: 2,
	index:true,
	array: false,
	searchType: 'R',
	supportsSort: true,
	supportsFacet: true,
	universal:true,
	name: 'number'
}