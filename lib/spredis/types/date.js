const moment = require('moment');
const SEP = require('../constants').SEP;
const utils = require('../../utils');

module.exports = {
	toRank: (val) => {
		return moment(val, moment.ISO_8601).unix();
	},
	toLex: (id, pos) => {
		return id;
		// return pos ? String(id) + ';' + String(pos): String(id);
	},
	fromString: (s) => {
		return moment(s, 'X');;
	},
	toSort: (val) => {
		return moment(val, moment.ISO_8601).unix();
	},
	actualValue: (val) => {
		return moment(val, moment.ISO_8601).unix();
	},
	toText: (val) => {
		return String(val);
	},

	index:true,
	array: false,
	searchType: 'R',
	supportsSort: true,
	supportsFacet: true,
	universal:true
}