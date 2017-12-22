'use strict';
var _ = require('lodash');
var types = require('./types');
var strategies = require('./strategy');

function getTypeDef(field) {
	var t = types[field.type];
	if (!t) throw new Error('No such type:' + field.type);
	var o = _.clone(t);
	_.assign(o, field);
	return o;
}

function getStrategy(def, prefix) {
	var Clazz = strategies[def.typeDef.type];
	if (!Clazz) throw new Error('No stratgey for type:' + def.typeDef.type);
	var strat = new Clazz(def, prefix);
	return strat;
}

const FIELD_TYPE_CODE = {
	literal: 'L',
	boolean: 'B',
	date: 'D',
	integer: 'I',
	number: 'N',
	text: 'T',
	geo: 'G',
	smallinteger: 'S',
	highcard: 'H'
}
module.exports = class Definition {
	constructor(name, prefix, field) {
		this.name = name;
		this.validateField(field)
		this.prefix = prefix
		this.indexName = prefix + ':I:' +  FIELD_TYPE_CODE[field.type] + ':' + name + '::'
		// this.typeDef = getType(field);
		// console.log(this.indexName)
		var type = getTypeDef(field);
		this.typeDef = type;
		// _.assign(this, type);
		// if (type.index) {
		this.strategy = getStrategy(this, prefix);
		// }
	}

	get redis() {
		return this._redis
	}

	set redis(redis) {
		this._redis = redis;
		if (this.stratgey) {
			this.strategy.redis = redis;
		}
	}

	validateField(field) {
		if (!field.type) throw new Error('No type defined:' + JSON.stringify(field))
		//TODO: check for proper field definition
	}
}