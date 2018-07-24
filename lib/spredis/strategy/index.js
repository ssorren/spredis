'use strict';

/*
	maps specific field types to their more generic strategies
*/
module.exports = {
	date: require('./Range'),
	integer: require('./Range'),
	literal: require('./Literal'),
	text: require('./Text'),
	number: require('./Range'),
	smallinteger: require('./Range'),
	boolean: require('./Boolean'),
	geo: require('./Geo'),
	documentid: require('./DocumentId')
}