/*
	a special strategy which pre-intersects a list of fields
	handy for commonly run queries
*/

const _ = require('lodash');
const textUtils = require('../../text');
const lexTokens = textUtils.lexTokens;
const Prefixed = require('../Prefixed');
const UNIVERSAL_LANG = 'U';

const QTYPE_MAP = {
	GEO: 1,
	DBL: 2,
	LEX: 3
};

function _exists(val) {
	return val !== null && val !== undefined && val !== '';
}

module.exports = class CompoundStrategy extends Prefixed {
	constructor(prefix, name, def, allStrategies) {
		super(prefix, name, def, allStrategies);
		this.name = name;
		this.indexName = prefix + ':I:C:' + name + '::';
		this.strategies = {};
		this.rankStrategy = def.rank ? allStrategies[def.rank] : null;
		// this.allIdsKey = prefix + ':ALLIDS';
		this.prefix = prefix;

		if (allStrategies[name]) {
			throw new Error(`Composite can not have the same name as an index field (${name})`)
		}

		this.sortedFields = _.uniq(def.fields);

		if (!this.sortedFields || this.sortedFields.length <= 1) {
			throw new Error(`Compound definition error (${name}):  must have at least 2 fields in a composite definition ('${this.sortedFields}' provided)`);
		}
		var self = this;
		_.forEach(this.sortedFields, (f) => {
			let strat = allStrategies[f];
			if (!strat) {
				throw new Error(`Composite definition error (${name}):  field '${f}' is not defined. Fields in composite indeces must be defined (can be defined with index=false if you are looking to save space)`);
			}
			if (!strat.type.compositeType) {
				throw new Error(`Composite definition error (${name}): field '${f}' is not of an acceptable type. '${strat.type.name}' type fields are not allowed.`);
			}
			self.strategies[f] = strat;
		});

		this.priority = this.sortedFields.length;
	}

	// get lang() {
	// 	//TODO: multilingual support
	// 	return 'CI';
	// }

	resolveLang(lang='en') {
		return UNIVERSAL_LANG; //this.universal ? UNIVERSAL_LANG : lang;
	}

	_stripArray(a, strat, fromQuery) {
		
		if (!fromQuery) {
			if (!_.isArray(a)) return a;
			if (_.isArray(a) && a.length === 1) return a[0];
			return _.filter(a, (v) => {
				return strat._exists(v);
			});
		}
		a = !_.isArray(a) ? [a] : a;
		// let wildcard = _.find(a, (val) => {
		// 	return textUtils.isPrefixValue(val) || textUtils.isSuffixValue(val)
		// })
		// if (wildcard) return [];
		return _.filter(a, (v) => {
			return strat._exists(v);
		});
		
	}


	querySatisfies(query, deferringOrs) {
		let matchCount = 0;
		for (let i = 0; i < this.sortedFields.length; i++) {
			let f = this.sortedFields[i];
			let strat = this.strategies[f];
			// console.log(query[f], strat._exists(query[f]));
			if (strat._queryExists(query[f])) matchCount++;
		}
		// console.log(matchCount, this.sortedFields.length);
		return matchCount === this.sortedFields.length ? this.sortedFields : 0;
	}

	fieldsSatisfiedBy(doc, deferringOrs) {
		for (let i = 0; i < this.sortedFields.length; i++) {
			let f = this.sortedFields[i];
			let strat = this.strategies[f];
			let qv = this._stripArray(doc[f], strat);
			if ((!_.isArray(qv) && !strat._exists(qv)) || (qv && qv.length === 0)) return 0;
			if (deferringOrs) {
				if (_.isArray(qv) && qv.length > 1) {
					return 0;
				}
			}
		}
		return this.sortedFields;
	}

	fieldsSatisfiedByRange(doc, rangeField) {
		// if (rangeField !== this.rankStrategy.name) return 0;
		for (let i = 0; i < this.sortedFields.length; i++) {
			let f = this.sortedFields[i];
			let strat = this.strategies[f];
			let qv = this._stripArray(doc[f], strat);
			if ((!_.isArray(qv) && !strat._exists(qv)) || (qv && qv.length === 0)) return 0;
		}
		return this.sortedFields;
	}

	toRank(val) {
		if (!this.rankStrategy) {
			return 0
		};
		let strat
		return this.rankStrategy.toRank(val, this.rankStrategy.sortPrecision);
	}

	_everyValue(base, field, perms) {
		var v = base[field];
		if (perms.length == 0) {
			//first iteration, we need to seed perms with the first value
			_.forEach(v, (a) => {
				let o = {};
				o[field] = a;
				perms.push(o);
			})
			return perms;
		}
		let newPerms = [];
		// we need every possible value combination
		_.forEach(perms, (o) => {
			_.forEach(v, (a) => {				
				let oo = {};
				_.assign(oo, o);
				oo[field] = a;
				newPerms.push(oo);
			});
		});
		return newPerms;
	}

	permutations(doc) {
		//this is gonna suck if more than 1 of the fields is an array type.
		let base = {};
		_.forEach(this.sortedFields, (f) => {
			let strat = this.strategies[f]
			base[f] =  strat.toIndexableValue(doc[f]);
			//logic is easier if all values are arrays
			if (!_.isArray(base[f])) base[f] = [base[f]];
		});
		let perms = [];
		let self = this;
		//build the permutations field by field
		_.forEach(this.sortedFields, (f) => {
			perms = self._everyValue(base, f, perms);
		});
		return perms;
	}

	indexForValue(value, lang) {
		return this.indexName + ':V:CP:' + this.resolveLang(lang)
	}

	indexForQuery(pipe, base, lang, hint, cleanUp) {
		let indexName = this.indexForValue(base, lang);
		let store = this.getTempIndexName(cleanUp);
		let value, strat, type, f, v, command = [];
		for (let i = 0; i < this.sortedFields.length; i++) {
			f = this.sortedFields[i];
			strat = this.strategies[f];
			type = strat.type.compositeType;
			value = base[f];
			value = _.isArray(value) ? value : [value];
			
			command.push(QTYPE_MAP[type]);
			command.push(value.length);
			for (let k = 0; k < value.length; k++) {
				command.push(...strat.toCompositeQuery( value[k] ));
			}
		}
		// console.log(command);
		pipe.call('spredis.comprangestore', store, indexName, hint || '', ...command);
		return store;
	}

	// indexForPermutation(pipe, value, lang, cleanUp) {
	// 	let indexName = this.indexForValue(value, lang);
	// 	let s = this.lexValue(value, lang);
	// 	let store = this.getTempIndexName(cleanUp);
	// 	for (let i = 0; i < this.sortedFields.length; i++) {
	// 		f = this.sortedFields[i];
	// 		strat = this.strategies[f];
	// 		type = strat.type.compositeType;
	// 	}
	// 	// pipe.call('spredis.zllinkset', indexName, store, s);
	// 	return store;
	// }

	toLex(id, pos) {
		return String(id);
	}


	lexValue(value, lang) {
		// let a = [];
		let command = [];
		let type, strat, f;
		for (let i = 0; i < this.sortedFields.length; i++) {
			f = this.sortedFields[i];
			strat = this.strategies[f];
			type = strat.type.compositeType;
			command.push(type);
			switch(type) {
				case 'GEO':
					// let latlon = strat.toRank(value[f]);
					// console.log('value:', value);
					command.push(value[f].lat);
					command.push(value[f].lon);
					break;
				case 'DBL':
					command.push(strat.toRank(value[f]));
					break;
				default:
					command.push(strat.getQueryValue(value[f]));
			}
			// a.push(lexTokens(  strat.getQueryValue(value[f]) ).join('+'))
		}
		// console.log(command);
		return command;
		// console.log(command);
		// return a.join(';');
	}

	unIndexDocument(pipe, doc, lang='en') {
		if (!doc) return;
		//unindex it
		let perms = this.permutations(doc);

		for (let i = 0; i < perms.length; i++) {
			let perm = perms[i];
			if (this.fieldsSatisfiedBy(perm)) {
				let s = this.lexValue(perm, lang);
				let indexName = this.indexForValue(perm, lang);
				let id = this.toLex(doc._id);

				pipe.call('spredis.comprem', indexName, id, this.sortedFields.length, ...s);
			}
		}
	}

	

	indexDocument(pipe, doc, lang='en') {
		if (!doc) return;
		//index it
		// console.log('indexing...');
		let perms = this.permutations(doc);
		for (var i = 0; i < perms.length; i++) {
			let perm = perms[i];
			if (this.fieldsSatisfiedBy(perm)) {
				let s = this.lexValue(perm, lang);
				let indexName = this.indexForValue(perm, lang);
				let id = this.toLex(doc._id);
				pipe.call('spredis.compadd', indexName, id, this.sortedFields.length, ...s);
				// pipe.call('spredis.zladd', indexName, id, 0, s);
			}
		}

	}
}