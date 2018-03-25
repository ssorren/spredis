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
	idFromLex: (lex) => {
		return lex;
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
	toAlphaSort: (val) => {
		val = moment(val, moment.ISO_8601).unix();
		return utils.integerToAscAlphaSort(val, 38);
	},
	toDescAlphaSort: (val) => {
		val = moment(val, moment.ISO_8601).unix();
		return utils.integerToDescAlphaSort(val, 38);
	},
	index:true,
	array: false,
	searchType: 'R',
	supportsSort: true,
	supportsFacet: true,
	universal:true
}