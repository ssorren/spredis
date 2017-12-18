// var Strategy = require('./Strategy');
const _ = require('lodash');
const textUtils = require('../../text');
const lexTokens = textUtils.lexTokens;

function _exists(val) {
	return val !== null && val !== undefined && val !== '';
}

module.exports = class CompoundStrategy {
	constructor(prefix, name, def, allStrategies) {
		this.name = name;
		this.indexName = prefix + ':I:C:' + name + '::';
		this.strategies = {};
		this.rankStrategy = def.rank ? allStrategies[def.rank] : null;
		this.allIdsKey = prefix + ':ALLIDS';

		if (def.rank && !this.rankStrategy) {
			throw new Error(`Compound definition error (${name}): rank field '${def.rank}' is not a declared field.`);
		}

		if (allStrategies[name]) {
			throw new Error(`Compounds can not have the same name as an index field (${name})`)
		}

		if (this.rankStrategy && this.rankStrategy.type.searchType !== 'R') {
			throw new Error(`Compound definition error (${name}): rank field '${def.rank}' is not YET an acceptable type. Currently, only dates, small integers, integers and numbers are allowed`);
		}

		if (this.rankStrategy && this.rankStrategy.type.array) {
			throw new Error(`Compound definition error (${name}): array type rank fields are not allowed in compounds (${def.rank})`);
		}

		this.sortedFields = _.sortedUniq(def.fields);

		if (!this.sortedFields || this.sortedFields.length <= 1) {
			throw new Error(`Compound definition error (${name}):  must have at least 2 fields in a compound definition ('${this.sortedFields}' provided)`);
		}
		var self = this;
		_.forEach(this.sortedFields, (f) => {
			let strat = allStrategies[f];
			if (!strat) {
				throw new Error(`Compound definition error (${name}):  field '${f}' is not defined. Fields in compounds must be defined (can be defined with index=false if you are looking to save space)`);
			}
			if (strat.type.searchType !== 'L') {
				throw new Error(`Compound definition error (${name}): field '${f}' is not an acceptable type. Only booleans and literals are allowed`);
			}
			self.strategies[f] = strat;
		});

		this.priority = this.sortedFields.length;
	}
	get lang() {
		//TODO: multilingual support
		return 'CI';
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
		let wildcard = _.find(a, (val) => {
			return textUtils.isPrefixValue(val) || textUtils.isSuffixValue(val)
		})
		if (wildcard) return [];
		return _.filter(a, (v) => {
			return strat._exists(v);
		});
		
	}

	querySatisfies(query, deferringOrs) {
		for (let i = 0; i < this.sortedFields.length; i++) {
			let f = this.sortedFields[i];
			if (query[f] === undefined) return 0;
			let strat = this.strategies[f];
			let qv = this._stripArray(query[f], strat, strat.type.prefix || strat.type.suffix);

			// console.log(qv)
			if ((!_.isArray(qv) && !strat._exists(qv)) || (qv && qv.length === 0)) return 0;
			if (deferringOrs) {
				if (_.isArray(qv) && qv.length > 1) {
					return 0;
				}
			}
		}
		return this.sortedFields;
	}


	fieldsSatisfiedBy(doc, deferringOrs) {
		for (let i = 0; i < this.sortedFields.length; i++) {
			let f = this.sortedFields[i];
			let strat = this.strategies[f];
			let qv = this._stripArray(doc[f], strat);

			// console.log(qv)
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
		if (rangeField !== this.rankStrategy.name) return 0;
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
				o[field] = a;
				newPerms.push(o);
			});
		});
		return newPerms;
	}

	permutations(doc) {
		//this is gonna suck if more than 1 of the fields is an array type.
		let base = {};
		_.forEach(this.sortedFields, (f) => {
			base[f] = doc[f];
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
		//TODO: multi-language support
		let a = [];
		for (let i = 0; i < this.sortedFields.length; i++) {
			let f = this.sortedFields[i];
			let strat = this.strategies[f]
			a.push(lexTokens(  strat.getQueryValue(value[f]) ).join('+'))
		}
		return this.indexName + ':V:' + this.lang + ':' + a.join(';');
	}

	toLex(id, pos) {
		return pos ? String(id) + ';' + String(pos): String(id);
	}

	unIndexDocument(pipe, doc) {
		if (!doc) return;
		//unindex it
		let perms = this.permutations(doc);

		for (let i = 0; i < perms.length; i++) {
			let perm = perms[i];
			if (this.fieldsSatisfiedBy(perm)) {
				let indexName = this.indexForValue(perm, this.lang);
				let id = this.toLex(doc.id);
				let rStrat = this.rankStrategy;
				pipe.zremBuffer(indexName ,id);
			}
		}
	}

	indexDocument(pipe, doc) {
		if (!doc) return;
		//index it
		let perms = this.permutations(doc);
		// console.log(perms)
		for (var i = 0; i < perms.length; i++) {
			let perm = perms[i];
			if (this.fieldsSatisfiedBy(perm)) {

				let indexName = this.indexForValue(perm, this.lang);
				// console.log(this.name, perm, indexName)
				let id = this.toLex(doc.id);
				let rStrat = this.rankStrategy;
				pipe.zaddBuffer(indexName, this.toRank( rStrat ? doc[rStrat.name] : null ) ,id);
			}
		}

	}
	// getQueryValue(val) {
	// 	return  val ? '1' : '0';
	// }

	// get lang() {
	// 	return 'M';
	// }

	// set lang(l) {

	// }

	// indexForValue(val, lang) {
	// 	//ignore language for number type fields, will alway be 'M' for math
	// 	return this.indexName + ':V:' + this.lang;
	// }

	// specificValuePrefix(val, lang) {
	// 	//ignore language for number type fields, will alway be 'M' for math
	// 	return this.indexName + ':V:' + this.lang + ":"
	// }

	// sortIndex(lang) {
	// 	return this.indexName + ':SALL:' + this.lang;
	// }
}