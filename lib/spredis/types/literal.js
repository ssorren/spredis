var SEP = require('../constants').SEP;
// var hash = require('string-hash');
const tolex = require('../../text').lex;
const utils = require('../../utils');

module.exports = {
	toRank: (val) => {
		return 1;//hash(String(val).toLowerCase());
	},
	toLex: (id, pos) => {
		return id;
		// return pos ? String(id) + ';' + String(pos): String(id);
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
	compositeType: 'LEX',
	alpha: true,
	index:true,
	array: false,
	prefix: true,
	suffix: false,
	supportsSource: true,
	supportsPrefix: true,
	supportsSuffix: true,
	searchType: 'L',
	sortStrategy: 'L',
	supportsSort: true,
	supportsFacet: true,
	universal: false,
	stem: false,
	name: 'literal',
	serverType: 3,
	fullText: 0
}