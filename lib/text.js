var stopWords = require('stopwords-iso');
var _ = require('lodash');
var diacritics = require('diacritics').remove;
var stemmer = require('node-snowball');

const STEM_LANGS = {
	ara:"arabic",
	ar:"arabic",
	dan:"danish",
	da:"danish",
	dut:"dutch",
	nid:"dutch",
	nl:"dutch",
	eng:"english",
	en:"english",
	fin:"finnish",
	fi:"finnish",
	fre:"french",
	fra:"french",
	fr:"french",
	ger:"german",
	deu:"german",
	de:"german",
	hun:"hungarian",
	hu:"hungarian",
	ita:"italian",
	it:"italian",
	nno:"norwegian",
	nn:"norwegian",
	nob:"norwegian",
	nb:"norwegian",
	por:"portuguese",
	pt:"portuguese",
	spa:"spanish",
	es:"spanish",
	swe:"swedish",
	sv:"swedish",
	rum:"romanian",
	ron:"romanian",
	ro:"romanian",
	tam:"tamil",
	ta:"tamil",
	tur:"turkish",
	tr:"turkish"
}

// stopWords = _.uniq(_.map(stopWords, (word) => {
// 	return lex(word);
// }));

function punctuation(str) { //need to keep * for wildcard searches
	str = str ? String(str) : '';
	return String(str).replace(/[&\/\\#,+\(\)$~%\.!^'"\;:?\[\]<>{}]/g, '');
};

function lex(str) { //same as punctuation but also removes *
	str = str ? String(str) : '';
	return diacritics(String(str).replace(/[&\/\\#,+\(\)$~%\.!^'"\;:*?\[\]<>{}]/g, '')).toLowerCase();
};


function removeJunk(text) {
	text = text ? String(text) : '';
	return diacritics(punctuation(text).toLowerCase());
};

function tokenize(text) {
	var tokens = removeJunk(text).split(/\s/);
	_.remove(tokens,(t) => {return t === '';});
	return tokens;
}

function stem(text, lang='en') {
	return stemmer.stemword(text, STEM_LANGS[lang]);
}

function filter(text, lang='en') {
	let stops = stopWords[lang];
	let tokens = _.isArray(text) ? text : tokenize(text);
	return stops ? _.difference(tokens, stops) : tokens;
}

function lexTokens(text) {
	var tokens = lex(text).split(/\s/);
	_.remove(tokens,(t) => {return t === '';});
	return _.difference(tokens, stopWords);
}

module.exports = {
	internationalize: diacritics,
	tokenize: tokenize,
	filter: filter,
	clean: removeJunk,
	stem: stem,
	lex: lex,
	lexTokens: lexTokens,
	isPrefixValue: (val) => {return _.endsWith(val, '*');},
	isSuffixValue: (val) => {return _.startsWith(val, '*');}
}
