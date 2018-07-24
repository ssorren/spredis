var SEP = require('../constants').SEP;
// var hash = require('string-hash');
const tolex = require('../../text').lex;
const utils = require('../../utils');

module.exports = {
	toRank: (val) => {
		return val;//hash(String(val).toLowerCase());
	},
	toLex: (id, pos) => {
		return id;
		// return pos ? String(id) + ';' + String(pos): String(id);
	},

	toSort: (val) => {
		return val;
	},
	actualValue: (val) => {
		return String(val);
	},
	toText: (val) => {
		return String(val);
	},
	compositeType: 'LEX',
	alpha: true,
	index:false,
	array: false,
	prefix: false,
	suffix: false,
	supportsSource: false,
	supportsPrefix: false,
	supportsSuffix: false,
	searchType: 'I',
	sortStrategy: 'I',
	supportsSort: false,
	supportsFacet: false,
	universal: false,
	stem: false,
	name: 'documentid',
	serverType: 3,
	fullText: 0
}