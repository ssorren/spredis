var SEP = require('../constants').SEP;
// var hash = require('string-hash');
const tolex = require('../../text').lex;
const utils = require('../../utils');

module.exports = {
	toRank: (val) => {
		return 0;//hash(String(val).toLowerCase());
	},
	toLex: (id, pos) => {
		return [id, pos].join(SEP);
		// return pos ? String(id) + ';' + String(pos): String(id);
	},
	idFromLex: (lex) => {
		return lex.split(SEP)[0];
	},
	toSort: (val) => {
		return tolex(val);
	},
	actualValue: (val) => {
		return String(val);
	},
	toText: (val) => {
		return String(val);
	},
	toAlphaSort: (val, precision) => {
		return utils.strAscAlphaSort(val, 56);
	},
	toDescAlphaSort: (val, precision) => {
		return utils.strDescAlphaSort(val, 56);
	},
	alpha: true,
	index:true,
	array: false,
	prefix: true,
	suffix: false,
	supportsSource: true,
	supportsPrefix: true,
	supportsSuffix: true,
	searchType: 'L',
	sortStrategy: 'L'
}