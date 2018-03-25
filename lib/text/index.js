var stopWords = require('stopwords-iso');
var _ = require('lodash');
var diacritics = require('diacritics').remove;

stopWords = _.uniq(_.map(stopWords, (word) => {
	return lex(word);
}));

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

function filter(text, lang='en') {
	let stops = stopWords[lang];
	return stops ? _.difference(tokenize(text), stops) : tokenize(text);
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
	lex: lex,
	lexTokens: lexTokens,
	isPrefixValue: (val) => {return _.endsWith(val, '*');},
	isSuffixValue: (val) => {return _.startsWith(val, '*');}
}
