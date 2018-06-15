/*
	simple class to contain and nest queries. used by Searcher class
*/
const constants = require('./constants');
const 	AND = constants.AND,
		OR = constants.OR,
		NOT = constants.NOT;

module.exports = class Clause {
	constructor(query, type) {
		this.query = query;
		this.type = type || AND;
		this.children = [];
		this.index = null;
	}
}