const _ = require('lodash');
const uuid = require('uuid').v4;
// let pack = JSON.stringify,
// 	unpack = JSON.parse;
const constants = require('./constants');
const 	TEXT_QUERY = constants.TEXT_QUERY,
		EXPRESSION = constants.EXPRESSION,
		AND = constants.AND,
		OR = constants.OR,
		NOT = constants.NOT;


const pack = require('snappy').compressSync;
const unpack = require('snappy').uncompressSync;

const textUtils = require('../text');
const Text = require('./strategy/Text');
const Prefixed = require('./Prefixed');
const Clause = require('./Clause');
const SUBCLAUSE = /^\$.+/;
const ANDCLAUSE = /^\$and.*/;
const ORCLAUSE = /^\$or.*/;
const NOTCLAUSE = /^\$not.*/;

const isClause = (f) => {
	return f !== TEXT_QUERY && SUBCLAUSE.test(f);
}

const clauseType = (f) => {
	if (!isClause(f)) return null;
	if (ORCLAUSE.test(f)) return OR;
	if (NOTCLAUSE.test(f)) return NOT;
	if (ANDCLAUSE.test(f)) return AND;
	return null;
}

module.exports = class Searcher extends Prefixed {
	constructor(prefix, defs, compounds) {
		super();
		this.prefix = prefix;
		this.valueKey = prefix + ':DOCS';
		this.valueKeyPattern = this.valueKey + '->*';
		this.allIdsKey = prefix + ':ALLIDS';
		this.idsOnlyKey = prefix + ':IDS';
		this.defs = defs;
		this.defNames = _.keys(defs);
		/*
			when we reduce a query to compounds, we want the most complex first
		*/
		this.compounds = _.reverse( _.sortBy( _.values(compounds), 'priority'));
		this.allTextStrategies = _.map(defs, (def) => {
			let strat = def.strategy;
			if (strat instanceof Text) return strat;
			return null;
		});
		_.remove(this.allTextStrategies, (s)=>{
			return s === null || s === undefined;
		});
	}

	orRegex(lang) {
		//TODO: multi language support
		return /\sor\s|\|/;
	}
	validateSorts(sort) {
		for (let i = 0; i < sort.length; i++) { //only supporting primary sort right now, ...redis
			let field = sort[i].field;
			let def = this.defs[field];
			if (!def) throw new Error('No definition for "' + field + '" (from sort clause)');
			let strat = def.strategy;
			if (!strat) throw new Error('No search strategy for "' + field + '" (from sort clause, index probably set to false in defnition)');
			if (!strat.type.sort)  throw new Error('Sorting is not enabled for ' + field);
			sort[i].order = (sort[i].order || strat.asc).toUpperCase();
			if (sort[i].order !== strat.asc && sort[i].order !== strat.desc) throw new Error('Invalid sort order: ' + order);
		};
	}
	getStrategy(name) {
		let def = this.defs[name];
		return def ? def.strategy : null;
	}

	individualTextQuery(pipe, crit, fields, cleanUp, hint) {
		let strats = this.allTextStrategies;
		let indeces = [];
		for (let i = 0; i < strats.length; i++) {
			let strat = strats[i];
			let stratHint = hint;
			// let stratIndeces = [];
			for (let k = 0; k < crit.query.length; k++) {
				let ifv = strat.indexForSearchValue(pipe, crit.query[k], crit.lang, stratHint, cleanUp);
				if (ifv) {
					stratHint = ifv;
					// if (ifv.index) {
					// 	// stratIndeces.push(ifv.index);
					// 	stratHint = ifv.index;

					// }
					// if (ifv.cleanUp && ifv.cleanUp.length) cleanUp.push.apply(cleanUp, ifv.cleanUp);	
				}	
			};

			if (stratHint) indeces.push(stratHint);
		};
		
		indeces = _.uniq(indeces);
		if (indeces.length > 1) {
			let store = this.getTempIndexName();
			cleanUp.push(store);
			let com = [store]
			com.push.apply(com, indeces);
			pipe.sunionstore.apply(pipe, com);
			return store;
		} else if (indeces.length) {
			// console.log(indeces[0])
			return indeces[0];
		}
		return hint;
	}

	textPhase(pipe, queries, fields, cleanUp, hint) {
		// console.log("hint:", hint, queries);
		if (!queries || queries.length === 0) return hint;
		let indeces = [];
		for (let i = 0; i < queries.length; i++) {
			let res = this.individualTextQuery(pipe, queries[i], fields, cleanUp, hint);
			if (res && res != hint) {
				indeces.push(res);
			}
		};

		if (indeces.length > 1) {
			let store = this.getTempIndexName(cleanUp);
			// cleanUp.push(store);
			let com = [store]
			com.push.apply(com, indeces);
			pipe.sunionstore.apply(pipe, com);
			return store;
		} else if (indeces.length) {
			return indeces[0];
		}
		return hint;
	}

	performPhase(pipe, crits, cleanUp, hint) { //, forceDefers) {
		let indeces = [];
		// let defers = [];
		// console.log("NO DEFERS!!!!!!");
		if (!crits) return hint;
		for (let i = 0; i < crits.length; i++) {
			let val = crits[i].val;
			let strat = crits[i].strat;
			let lang = crits[i].lang;
			// if (!forceDefers && _.isArray(val) && val.length > 1) {
			// 	defers.push(crits[i]);
			// 	continue;
			// }
			
			let ifv = strat.indexForSearchValue(pipe, val, lang, hint, cleanUp);
			if (ifv) {
				indeces.push(ifv);
				hint = ifv;
				// if (ifv.index) {
					
				// 	hint = ifv.index;
				// }
				// if (ifv.cleanUp && ifv.cleanUp.length) cleanUp.push.apply(cleanUp, ifv.cleanUp);	
			}
		};
		if (indeces.length === 1) {
			return indeces[0];
		} else if (indeces.length > 1) {
			let inter = this.getTempIndexName(cleanUp);
			indeces.unshift(indeces.length);
			indeces.unshift(inter);
			// cleanUp.push(inter)
			pipe.szinterstore.apply(pipe, indeces);
			return inter;
 		}

 		return hint;
	}

	
	compoundPhase(pipe, compoundIndeces, cleanUp, hint) {
		if (!compoundIndeces || compoundIndeces.length === 0) return hint;
		let indeces = [];
		for (let i = 0; i < compoundIndeces.length; i++) {
			let ci = compoundIndeces[i];
			ci = _.isArray(ci) && ci.length == 1 ? ci[0] : ci;
			if (_.isArray(ci)) {
				let temp = this.getTempIndexName(cleanUp);
				let command = [temp];
				command.push.apply(command, ci);
				pipe.sunionstore.apply(pipe, command);
				indeces.push(temp);
			} else {
				indeces.push(ci);
			}
		}
		if (hint) {
			indeces.push(hint);
		}
		if (indeces.length === 0) return null;
		if (indeces.length === 1) return indeces[0];
		let store = this.getTempIndexName(cleanUp);
		indeces.unshift(store);
		pipe.sinterstore.apply(pipe, indeces);
		return store;
	}

	getRequestLang(request) {
		return request.lang || 'en';
	}

	distancExpression(pipe, expr, idSet, cleanUp) {
		let strat = expr.strategy;
		if (strat) {
			//check to see if we've found this index already first.
			// calculating the distance takes some time- lets' not do it more than once if we can avoid it
			let preCalculated = expr._indexName ? true : false;
			let distanceIndex = expr._indexName || strat.indexForValue(expr.val.from); 
			//get the coords
			let query = strat.getQueryValue(expr.val);
			let store = this.getTempIndexName();
			
			cleanUp.push(store);
			// pipe.getScores(exprIds, store, distanceIndex);

			//let's only calculate distance for the items we're viewing
			pipe.zinterstore(store, 2, idSet, distanceIndex, 'WEIGHTS', 0, 1);
			
			expr.resultIndex = pipe.length;
			if (preCalculated) {
				expr.scoreType = true;
				pipe.zrange(store, 0, -1, 'WITHSCORES');
			} else {
				pipe.georadius(store, query.from[1], query.from[0], '+inf', query.unit, 'WITHDIST')
			}
			// pipe.zinterstoreBuffer(store, 2, idSet, distanceIndex, 'WEIGHTS', 0, 1);
		}
	}

	assignExpressionValues(idMap, expressions, res) {
		// let idMap = {};
				
		// for (let i = 0; i < items.length; i++) {
		// 	idMap[items[i]._id] = items[i];
		// }
		
		for (let i = 0; i < expressions.length; i++) {
			let ex = expressions[i];
			let eRes = res[ ex.resultIndex ][1];
			if (ex.scoreType) {
				for (let k = 0; k < eRes.length; k+=2) {
					let doc = idMap[ eRes[k] ];
					if (doc) doc[ex.name] = eRes[k + 1];
				}
			} else {
				for (let k = 0; k < eRes.length; k++) {
					let doc = idMap[ eRes[k][0] ];
					if (doc) doc[ex.name] = eRes[k][1];
				}	
			}				
		}
	}


	applySort(pipe, indexName, sort, lang, cleanUp) {
		// the following block is for slower but more precise sort, may come in handy later
		/*			
		let com = [indexName, 'BY', 'nosort']
		for (let i = 0; i < sort.length; i++) {
			let strat = this.defs[sort[i].field].strategy;
			let order = sort[i].order;
			com.push('GET', (order === strat.desc ? strat.descSortPattern : strat.ascSortPattern));
		}

		com.push('GET', '#', 'STORE', interMediate);
		// console.log(com.join(' '))
		pipe.sortBuffer.apply(pipe, com);

		pipe.multiSortBuffer(interMediate, finalSort, sort.length + 1, start, count);
		*/

		let finalSort = this.getTempIndexName();
		cleanUp.push(finalSort);
		
		let mi = [indexName, finalSort, sort.length];
		let orders = [];
		for (let i = 0; i < sort.length; i++) {
			let strat = this.defs[sort[i].field].strategy;
			orders.push(sort[i].order);
			mi.push(strat.sortIndex(lang))
		}
		mi.push.apply(mi, orders);
		pipe.multiInterSort.apply(pipe, mi)
		return finalSort;
	}

	textQueries(fields, query, lang) {
		let textQueries = [];
		let textField = _.remove(fields, (f) => {
			return f === TEXT_QUERY;
		});

		if (query && textField.length && query[textField[0]]) {
			let textClauses = String(query[textField[0]])
									.toLowerCase()
									.split(this.orRegex(lang));
			textQueries = _.map(textClauses, (clause) => {
				clause = textUtils.filter(clause);
				if (clause && clause.length) return clause;
				return null;
			})
			_.remove(textQueries, (t) => {
				return t === null || t === undefined || t.length === 0;
			});
			textQueries = _.map(textQueries, (t) => {
				return {query: t, lang: lang};	
			});
		}
		return textQueries;
	}

	expressions(expr) {
		let exprs = [];
		
		_.forEach(expr, (val, key) => {
			if (val.type === 'distance') {
				let strat = this.getStrategy(val.field);
				if (strat) {
					exprs.push({name: key, strategy: strat, val: val});
				} else {
					throw new Error(`Could not find strategy for distance expression field '${val.field}'`);
				}
			} else {
				exprs.push({name: key, val: val});
			}
			
		});	

		return exprs;
	}

	buildFacets(pipe, facets, indexName, lang, cleanUp) {
		if (!facets || !_.keys(facets).length) return;
		let self = this;
		let fcount = 0;
		// let facetResultIndex = pipe.length;
		let ensuredSet = this.getTempIndexName(cleanUp)
		pipe.ensureZSet(indexName, ensuredSet)
		let command = [indexName, ensuredSet];
		_.each(facets, (value, key) => {
			let strat = self.getStrategy(value.field);
			if (!strat.type.facet) throw new Error('Facet not enabled for field "' + value.field + '"');
			value.resultIndex = self.getTempIndexName(cleanUp);
			let order = (value.order ? value.order.toUpperCase() : null) || 'ASC';
			let count = value.count || 10;

			// facet.field = ARGV[i]
			// facet.count = ARGV[i + 1]
			// facet.store = ARGV[i + 2]
			// facet.order = ARGV[i + 3]
			// facet.key = ARGV[i + 4]
			command.push(value.field, count,  value.resultIndex, order, strat.facetPattern(lang))
			// pipe.allFacets(indexName, value.field, count,  value.resultIndex, order, strat.facetPattern(lang))
			// pipe.facet(indexName, strat.facetPattern(lang), value.resultIndex, order, count);
			value.pipeIndex = pipe.length;
			value.facetIndex = fcount;
			fcount++;
		})

		pipe.allFacets.apply(pipe, command);
	}

	compoundIndeces(query, lang) {
		let compoundIndeces = [];
		for (var i = 0; i < this.compounds.length; i++) {
			let index = this.compounds[i];
			let satisfied = index.querySatisfies(query, false);
			// console.log("satisfied:",satisfied,query)

			if (satisfied && satisfied.length) {
				// compoundIndeces
				let base = {};
				_.forEach(satisfied, (sat) => {
					base[sat] = query[sat];
					delete query[sat];
				});
				let permutaions = index.permutations(base);
				if (permutaions.length === 1) {
					compoundIndeces.push(index.indexForValue(permutaions[0], lang))
				} else {
					let union = [];
					for (let i = 0; i < permutaions.length; i++) {
						union.push(index.indexForValue(permutaions[i], lang));
					}
					compoundIndeces.push(union);
				}
			}
		}
		return compoundIndeces;
	}

	parseSubClauses(clause, lang) {
		let query = clause.query;
		let fields = _.keys(query);
		// console.log(fields);
		for (var i = 0; i < fields.length; i++) {
			let f = fields[i];
			let type = clauseType(f)
			if (type !== null) {
				console.log('found sub clause!');
				clause.children.push( this.parseClause(new Clause( query[f], type ), lang) );
				delete query[f];
			}
		}
		return clause;
	}

	parseClause(clause, lang) {
		// let topClause = new Clause(query, AND); //AND clause
		/*
			we're going to call the compound phase first as it will remove fields from the query- 
			leaving less to search for.
		*/
		let query = clause.query;
		this.parseSubClauses(clause, lang);
		clause.compoundIndeces = this.compoundIndeces(query, lang);
		clause.fields = _.keys(query);
		clause.textQueries = this.textQueries(clause.fields, query, lang);
		// clause.expressions = this.expressions(clause.fields, query);
		clause.strats = _.map(clause.fields, (f) => {
			let strat = this.getStrategy(f);
			let val = query[f];
			if (!strat) throw new Error('No search strategy for ' + f + ' (index probably set to false in defnition, or no definition exists)');
			return {val: val, strat:strat, lang: lang};
		});

		clause.literalStrats = _.filter(clause.strats,(s) => {
			return s.strat.type.searchType === 'L';
		});

		clause.rankStrats = _.filter(clause.strats,(s) => {
			return s.strat.type.searchType === 'R';
		});

		clause.geoStrats = _.filter(clause.strats, (s) => {
			return s.strat.type.searchType === 'G';
		});
		return clause;
	}

	getClauseResult(clause, pipe, hint, cleanUp) {
		let compoundIndex = this.compoundPhase(pipe, clause.compoundIndeces, cleanUp, hint)
		let literalIndex = this.performPhase(pipe, clause.literalStrats, cleanUp, compoundIndex);
		let textIndex = this.textPhase(pipe, clause.textQueries, null, cleanUp, literalIndex);
		let rangeIndex = this.performPhase(pipe, clause.rankStrats, cleanUp, textIndex, true);
		return this.performPhase(pipe, clause.geoStrats, cleanUp, rangeIndex);
	}

	processClause(clause, pipe, hint, cleanUp) {

		clause.index = this.getClauseResult(clause, pipe, hint, cleanUp);
		for (var i = 0; i < clause.children.length; i++) {
			let child = clause.children[i];
			let ci = this.processClause(child, pipe, clause.index, cleanUp);
			if (ci.index) {
				let store = this.getTempIndexName(cleanUp);
				if (ci.type === NOT) {
					pipe.sdiffstore(store, clause.index, ci.index);
				} else {
					pipe.sinterstore(store, clause.index, ci.index);
				}
				clause.index = store;
			}
		}
		return clause;
	}

	async search(request) {
		let prepStartTime = new Date().getTime();

		let indeces = [];
		let cleanUp = [];
		let lang = this.getRequestLang(request);

		let myConn = this.redis.queryServer();
		let pipe = myConn.pipeline();
		// let pipe = this.redis.multi();

		let query = request.query ? _.cloneDeep(request.query) : null;
		let originalQuery = request.query;
		// console.log('theQuery:', query)
		let clause = this.parseClause(new Clause(query, AND), lang);
		this.processClause(clause, pipe, null, cleanUp);
		if (clause.index) {
			indeces.push(clause.index);
		}

		let hasQuery = indeces.length;

		let sort = request.sort || null;
		let count = request.count || ((request.count === 0) ? 0 : 10);
		let start = request.start || 0;
		let idsOnly = request.idsOnly;

		let multiSort = false;
		// let facetResultIndex = 0;
		let memberResultindex = 0;
		let countResultindex = 0;
		let getItemsIndex = 0;

		if (count > 0 && sort && sort.length) { 
			//validate sorts
			this.validateSorts(sort)
			multiSort = true;
		}
		let indexName = this.allIdsKey; //calling all cars
		

		if (hasQuery) {
			indeces = _.uniq(indeces);
			if (indeces.length > 1) {
				indexName = this.getTempIndexName();
				cleanUp.push(indexName);
				indeces.unshift(indeces.length);
				indeces.unshift(indexName);
				pipe.zinterstoreBuffer.apply(pipe, indeces);
			} else {
				indexName = indeces[0];
			}
		}

		
		if (multiSort) {
			indexName = this.applySort(pipe, indexName, sort, lang, cleanUp);
		}
		countResultindex = pipe.length;
		pipe.totalFound(indexName);

		let exprSet = null;
		let expressions = [];
		if (count > 0) {
			if (!idsOnly) {
				if (request.expr) {
					// exprIds = this.getTempIndexName();
					// cleanUp.push(exprIds);
					// pipe.sortBuffer(indexName, 'by', 'nosort', 'get', '#', 'LIMIT', start, count, 'STORE', exprIds);
					// console.log(request.expr);
					expressions = this.expressions(request.expr);
					// console.log(expressions);
					exprSet = this.getTempIndexName();
					cleanUp.push(exprSet);

					getItemsIndex = pipe.length;
					pipe.getDocsBuffer(indexName, this.valueKey, start, count, exprSet);
					// pipe.sortBuffer(exprIds, 'by', 'nosort', 'get', this.valueKeyPattern);

					
					// pipe.convertToSet(exprIds,exprSet); //convert to set is destructive to list, must be after the sort:get->valueKeyPattern

					for (let i = 0; i < expressions.length; i++) {
						let e = expressions[i];
						if (e.val.type === 'distance') {
							this.distancExpression(pipe, e, exprSet, cleanUp)
						}
					}
				} else {
					getItemsIndex = pipe.length;
					pipe.getDocsBuffer(indexName, this.valueKey, start, count);
					// pipe.sortBuffer(indexName, 'by', 'nosort', 'get', this.valueKeyPattern, 'LIMIT', start, count);
				}
			} else {
				memberResultindex = pipe.length;
				if (multiSort) {
					pipe.sort(finalSort, 'by', 'nosort', 'LIMIT', start, count)
				} else {
					pipe.zrange(indexName, start, count)	
				}
				
			}

		}
		
		if (request.facets) {
			this.buildFacets(pipe, request.facets, indexName, lang, cleanUp);
		}
		let prepEndTime = new Date().getTime();

		let response = await this.executeSearch(pipe, {
					request: request,
					expressions: expressions,
					countResultindex:countResultindex, 
					memberResultindex:memberResultindex,
					getItemsIndex:getItemsIndex,
					prepTime: prepEndTime - prepStartTime,
					count: count,
					idsOnly: idsOnly,
					exprIds: exprSet ? true : false
				});

		if (cleanUp.length) {
			myConn.delBuffer.apply(myConn, cleanUp).then( (res) => {
				// console.log("cleaned up %s items.", String(res));
			});
		}
		// console.log("res:", res);
		return response;
	}

	async executeSearch(pipe, opts) {
		let startTime = new Date().getTime();
		let res = await  pipe.exec();
		// pipe = null;
		// console.log(indexName, JSON.stringify(res, null, 2));
		let endTime = new Date().getTime();
		let startSerialize = new Date().getTime();
		let endSerialize = startSerialize;
		let totalFound;
		totalFound = res[opts.countResultindex][1];
		let itemCount = 0;
		let items = null;
		// let exprStartTime = 0,
		// 	exprEndTime = 0;
		if (opts.count === 0 || totalFound === 0) {
			items = [];
		} else if (opts.idsOnly) {
			items = res[opts.memberResultindex][1];
			itemCount = items.length;
		} else {
			let idMap = {};
			items = _.map(res[opts.getItemsIndex][1], (i) =>{ 
				// console.log(i)
				let doc =  JSON.parse(unpack(i).toString());
				idMap[doc._id] = doc;
				delete doc._id;//no need to send this to client, _id should be obfuscated
				return doc;
			});
			itemCount = items.length;
			if (opts.exprIds && opts.expressions.length) {
				this.assignExpressionValues(idMap, opts.expressions, res);
			}			
		}
		
		let facetRes = {};
		_.each(opts.request.facets, (value, key) => {
			let o = {};
			if (value.facetIndex >= 0) {

				let fres = _.chunk(res[value.pipeIndex][1][value.facetIndex], 2);
			
				_.each(fres, (a) => o[ a[0] ] = parseInt(a[1]));
				facetRes[key] = o;	
			}
			
		});

		endSerialize = new Date().getTime();

		let response = {
			totalFound: totalFound,
			start: opts.request.start || 0,
			count: itemCount,
			facets: facetRes,
			items: items,
			prepTimeMillis: opts.prepTime || 0,
			queryTimeMillis: endTime - startTime,
			serializeTimeMillis: endSerialize - startSerialize
		}
		return response
	}
}