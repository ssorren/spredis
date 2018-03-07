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

const DISTANCE_CONVERSION = {
	'm':1,
	'km':0.001,
	'mi': 0.000621371,
	'ft': 3.28084
}
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
	validateSorts(sort, expressions) {
		expressions = expressions || [];

		for (let i = 0; i < sort.length; i++) { //only supporting primary sort right now, ...redis
			let field = sort[i].field;
			sort[i].order = (sort[i].order || strat.asc).toUpperCase();

			let exprSort = _.find(expressions, (e) => {
				return e.name === field;
			})
			if (exprSort) {
				if (!exprSort.strategy) throw new Error('No strategy for sort expression "' + field + '" (from sort clause)');
				sort[i].expr = exprSort;
				continue;
			}
			let def = this.defs[field];
			if (!def) throw new Error('No definition for "' + field + '" (from sort clause)');
			let strat = def.strategy;
			if (!strat) throw new Error('No search strategy for "' + field + '" (from sort clause, index probably set to false in defnition)');
			if (!strat.type.sort)  throw new Error('Sorting is not enabled for ' + field);
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
				}	
			};
			if (stratHint) indeces.push(stratHint);
		};
		
		indeces = _.uniq(indeces);
		if (indeces.length > 1) {
			let store = this.getTempIndexName();
			cleanUp.push(store);
			// console.log('doing or here1....');
			let com = ['spredis.stunionstore',store]
			com.push.apply(com, indeces);
			pipe.call.apply(pipe, com);
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
			let self = this;
			let tIndex = _.find(indeces, (index) => {
				return self.isTempIndex(index);
			});
			if (tIndex) {
				store = tIndex;
				_.remove(indeces, (index) => {
					return index === store;
				});
				let com = ['spredis.staddall', store];
				for (var i = 0; i < indeces.length; i++) {
					com.push(indeces[i]);
				}
				pipe.call.apply(pipe, com);
			} else {
				pipe.call('spredis.stunionstore', store, indeces[0], indeces[1]);
				
				if (indeces.length > 2) {
					// console.log('doing or here3....');
					let com = ['spredis.staddall', store];
					for (var i = 2; i < indeces.length; i++) {
						com.push(indeces[i]);
					}
					pipe.call.apply(pipe, com);
				}
			}
			// console.log('doing or here2....');
			
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
			// indeces.unshift(indeces.length);
			// indeces.unshift(inter);
			// indeces.unshift('spredis.stinterstore');
			// cleanUp.push(inter)
			
			pipe.call.apply(pipe, ['spredis.stinterstore', inter,  ...indeces]);
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
				let command = ['spredis.stunionstore', temp];
				command.push.apply(command, ci);
				pipe.call.apply(pipe, command);
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
		// indeces.unshift(indeces.length);
		// indeces.unshift(store);
		// indeces.unshift('spredis.stinterstore');
		pipe.call.apply(pipe, ['spredis.stinterstore', store, ...indeces]);
		return store;
	}

	getRequestLang(request) {
		return request.lang || 'en';
	}

	distancExpression(pipe, expr, resultSet, start, count, lang, cleanUp) {
		let strat = expr.strategy;
		if (strat) {
			// console.log(`distance resolver (${expr.name}): ${expr.resolver}`);
			let query = strat.getQueryValue(expr.val);
			if (!expr.resolver) {
				let exprIndex = this.getTempIndexName(cleanUp);
				expr.resolver = exprIndex;
				pipe.call('spredis.setgeoresolver', exprIndex, expr.strategy.sortIndex(lang), 1, query.from[0], query.from[1], query.unit);
			}
			expr.resultIndex = pipe.length;
			pipe.call('spredis.resolveexpr', resultSet, expr.resolver, start, count);
			//check to see if we've found this index already first.
			// calculating the distance takes some time- lets' not do it more than once if we can avoid it
			// let preCalculated = expr._indexName ? true : false;
			// let distanceIndex = expr._indexName || strat.indexForValue(expr.val.from); 
			// //get the coords
			
			// let store = this.getTempIndexName();
			
			// cleanUp.push(store);
			// // pipe.getScores(exprIds, store, distanceIndex);

			// //let's only calculate distance for the items we're viewing
			// pipe.zinterstore(store, 2, idSet, distanceIndex, 'WEIGHTS', 0, 1);
			
			
			// if (preCalculated) {
			// 	expr.scoreType = true;
			// 	pipe.zrange(store, 0, -1, 'WITHSCORES');
			// } else {
			// 	pipe.georadius(store, query.from[1], query.from[0], '+inf', query.unit, 'WITHDIST')
			// }
			// pipe.zinterstoreBuffer(store, 2, idSet, distanceIndex, 'WEIGHTS', 0, 1);
		}
	}


	assignExpressionValues(idMap, expressions, res, expResult) {
		// let idMap = {};
				
		// for (let i = 0; i < items.length; i++) {
		// 	idMap[items[i]._id] = items[i];
		// }
		// console.log("Expression results...");
		for (let i = 0; i < expressions.length; i++) {
			let ex = expressions[i];
			let eRes = res[ ex.resultIndex ][1];

			// console.log("Expression result:",eRes);
			expResult[ex.name] = eRes;
			// console.log(JSON.stringify(ex));
			if (ex.val.type === 'distance') {

				let conv = DISTANCE_CONVERSION[ (ex.val.unit || 'm').toLowerCase() ] || 1;
				for (let k = 0; k < eRes.length; k++) {
					eRes[k] *= conv;
				}
			}
			// if (ex.scoreType) {
			// 	for (let k = 0; k < eRes.length; k+=2) {
			// 		let doc = idMap[ eRes[k] ];
			// 		if (doc) doc[ex.name] = eRes[k + 1];
			// 	}
			// } else {
			// 	for (let k = 0; k < eRes.length; k++) {
			// 		let doc = idMap[ eRes[k][0] ];
			// 		if (doc) doc[ex.name] = eRes[k][1];
			// 	}	
			// }				
		}
	}


	applySort(pipe, indexName, sort, lang, cleanUp) {
	

		// let primarySort = this.getTempIndexName(cleanUp);
		// // let ps = sort[0];
		// let psStrat =  this.defs[sort[0].field].strategy;
		// let psw = sort[0].order == 'ASC' ? 1 : -1;
		// pipe.zinterstore(primarySort, 2, psStrat.sortIndex(lang), indexName, 'weights', psw, 0);


		let finalSort = this.getTempIndexName(cleanUp);
		
		let mi = ['spredis.sort', indexName, finalSort];
		if (sort) {
			for (let i = 0; i < sort.length; i++) {
				if (!sort[i].expr) {
					let strat = this.defs[sort[i].field].strategy;
					mi.push(strat.sortIndex(lang), sort[i].order);
				} else {
					let exprSortIndex = this.getTempIndexName(cleanUp);
					let expr = sort[i].expr;
					let query = expr.strategy.getQueryValue(expr.val);
					expr.resolver = exprSortIndex;
					pipe.call('spredis.setgeoresolver', exprSortIndex, expr.strategy.sortIndex(lang), 1, query.from[0], query.from[1]);
					mi.push(exprSortIndex, sort[i].order);
				}
				
			}	
		}
		pipe.call.apply(pipe, mi);
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

		let command = ['spredis.facets',indexName];
		_.each(facets, (value, key) => {
			let strat = self.getStrategy(value.field);
			if (!strat.type.facet) throw new Error('Facet not enabled for field "' + value.field + '"');
			value.resultIndex = self.getTempIndexName(cleanUp);
			let order = (value.order ? value.order.toUpperCase() : null) || 'ASC';
			let count = value.count || 10;
			
			command.push(strat.facetPattern(lang), count, order);
			
			value.pipeIndex = pipe.length;
			value.facetIndex = fcount;
			fcount++;
		})

		pipe.call.apply(pipe, command);

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
		if (_.isArray(clause.query)) {
			// console.log('parsing subclause...');
			delete clause.query;
			delete clause.fields;
			_.forEach(query, (q) => {
				clause.children.push( this.parseClause(new Clause( q , null ), lang) );
			})
		} else {
			let fields = _.keys(query);
		// console.log(fields);
			for (var i = 0; i < fields.length; i++) {
				let f = fields[i];
				let type = clauseType(f)
				if (type !== null) {
					// console.log('found sub clause!');
					clause.children.push( this.parseClause(new Clause( query[f], type ), lang) );
					delete query[f];
				}
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
		clause.compoundIndeces = this.compoundIndeces(query, lang);
		
		clause.fields = _.keys(query);
		this.parseSubClauses(clause, lang);
		clause.textQueries = this.textQueries(clause.fields, query, lang);
		// clause.expressions = this.expressions(clause.fields, query);
		
		_.remove(clause.fields, (f) => {
			return clauseType(f);
		});

		// console.log(clause);
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
		let compoundIndex = this.compoundPhase(pipe, clause.compoundIndeces, cleanUp, hint);
		let literalIndex = this.performPhase(pipe, clause.literalStrats, cleanUp, compoundIndex);
		let textIndex = this.textPhase(pipe, clause.textQueries, null, cleanUp, literalIndex);
		let rangeIndex = this.performPhase(pipe, clause.rankStrats, cleanUp, textIndex, true);
		return this.performPhase(pipe, clause.geoStrats, cleanUp, rangeIndex);
	}

	processClause(clause, pipe, hint, cleanUp) {
		clause.index = this.getClauseResult(clause, pipe, hint, cleanUp);
		if (clause.type === OR) {
			let childResults = [];
			for (let i = 0; i < clause.children.length; i++) {
				let child = clause.children[i];
				childResults.push(this.processClause(child, pipe, clause.index, cleanUp));
			}
			let store = this.getTempIndexName(cleanUp);
			let command = ['spredis.stunionstore', store];
			_.forEach(childResults, (cr) => {
				if (cr.index) command.push(cr.index);
			})
			pipe.call.apply(pipe, command);
			clause.index = store;
		} else {
			for (let i = 0; i < clause.children.length; i++) {
				let child = clause.children[i];
				let ci = this.processClause(child, pipe, clause.index, cleanUp);
				if (ci.index) {
					let store = this.getTempIndexName(cleanUp);
					if (ci.type === NOT) {
						pipe.call('spredis.stdiffstore',store, clause.index, ci.index);
					} else {
						pipe.call('spredis.stinterstore',store, clause.index, ci.index);
					}
					clause.index = store;
				}
			}
		}
		return clause;

		// clause.index = this.getClauseResult(clause, pipe, hint, cleanUp);
		// for (var i = 0; i < clause.children.length; i++) {
		// 	let child = clause.children[i];
		// 	let ci = this.processClause(child, pipe, clause.index, cleanUp);
		// 	if (ci.index) {
		// 		console.log(1)
		// 		let store = this.getTempIndexName(cleanUp);
		// 		if (ci.type === NOT) {
		// 			console.log(2)
		// 			pipe.call('spredis.stdiffstore',store, clause.index, ci.index);
		// 		} else {
		// 			console.log(3)
		// 			console.log('spredis.stdiffstore',store, clause.index, ci.index);
		// 			pipe.call('spredis.stinterstore',store, clause.index, ci.index);
		// 		}
		// 		console.log(4)
		// 		clause.index = store;
		// 	}
		// }
		// console.log('clause.index', clause.index)
		// return clause;
	}

	async search(request) {
		let prepStartTime = new Date().getTime();

		let indeces = [];
		let cleanUp = [];
		let lang = this.getRequestLang(request);

		let myConn = this.redis.queryServer();
		let pipe = this.wrapPipe(myConn.pipeline());
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
		// console.log(`hasQuery: ${hasQuery}`);
		
		let count = request.count || ((request.count === 0) ? 0 : 10);
		let sort = count ? (request.sort || null) : null;

		let start = request.start || 0;
		let idsOnly = request.idsOnly;

		// let multiSort = false;
		// let facetResultIndex = 0;
		let memberResultindex = 0;
		let countResultindex = 0;
		let getItemsIndex = 0;
		if (count == 0) sort == null;

		let expressions = [];
		if (request.expr) {
			expressions = this.expressions(request.expr);
		}

		if (sort && sort.length) { 
			this.validateSorts(sort, expressions);
		}
		let indexName = this.allIdsKey; //calling all cars
		

		if (hasQuery) {
			indeces = _.uniq(indeces);
			if (indeces.length > 1) {
				indexName = this.getTempIndexName(cleanUp);
				// indeces.unshift(indexName);
				// indeces.unshift('spredis.stinterstore');
				
				pipe.call.apply(pipe, ['spredis.stinterstore', indexName, ...indeces]);
			} else {
				indexName = indeces[0];
			}
		}

		countResultindex = pipe.length;
		pipe.call('spredis.scard', indexName);
		// pipe.totalFound(indexName);
		// let facetIndex = indexName;
		// if (multiSort) {
		indexName = this.applySort(pipe, indexName, sort, lang, cleanUp);
		// }

		let exprSet = null;
		
		if (count > 0) {
			if (!idsOnly) {
				getItemsIndex = pipe.length;
				pipe.getDocsBuffer(indexName, this.valueKey, start, count);

				if (expressions.length) {
					// exprIds = this.getTempIndexName();
					// cleanUp.push(exprIds);
					// pipe.sortBuffer(indexName, 'by', 'nosort', 'get', '#', 'LIMIT', start, count, 'STORE', exprIds);
					// console.log(request.expr);
					// console.log(expressions);
					// exprSet = this.getTempIndexName();
					// cleanUp.push(exprSet);

					
					// pipe.sortBuffer(exprIds, 'by', 'nosort', 'get', this.valueKeyPattern);

					
					// pipe.convertToSet(exprIds,exprSet); //convert to set is destructive to list, must be after the sort:get->valueKeyPattern

					for (let i = 0; i < expressions.length; i++) {
						let e = expressions[i];
						if (e.val.type === 'distance') {
							this.distancExpression(pipe, e, indexName, start, count, lang, cleanUp)
						}
					}
				}
			} else {
				memberResultindex = pipe.length;
				pipe.getIdsBuffer(indexName, start, count);
				// if (multiSort) {
				// 	pipe.sort(indexName, 'by', 'nosort', 'LIMIT', start, count)
				// } else {
				// 	pipe.zrange(indexName, start, count)	
				// }
				
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
		// console.log("executeSearch");
		let startTime = new Date().getTime();
		let res = await  pipe.exec();
		// pipe = null;
		// console.log(JSON.stringify(res, null, 2));
		let endTime = new Date().getTime();
		let startSerialize = new Date().getTime();
		let endSerialize = startSerialize;
		let totalFound;
		totalFound = res[opts.countResultindex][1];
		let itemCount = 0;
		let items = [];
		let expResult = {};
		// let exprStartTime = 0,
		// 	exprEndTime = 0;
		if (opts.count === 0 || totalFound === 0) {
			//do nothing
		} else if (opts.idsOnly) {
			items = res[opts.memberResultindex][1];
		} else {
			let idMap = {};
			let raw = res[opts.getItemsIndex][1];
			let doc;
			// console.log('raw results',res.length, res);
			// console.log('item results', opts.getItemsIndex, raw.length);
			if (raw) {
				// console.log('processing ', raw.length)
				for (let i = 0; i < raw.length; i++) {
					// doc =  JSON.parse(unpack(raw[i]).toString('utf8'));
					try {
						doc =  unpack(raw[i]).toString('utf8');
						// console.log(doc);
						idMap[doc._id] = doc;
						items.push(doc);	
					} catch(e) {
						console.error(e.stack)
					}
					
				}
			}
			// items = _.map(raw, (i) =>{ 
			// 	// console.log(i)
				
			// 	idMap[doc._id] = doc;
			// 	delete doc._id;//no need to send this to client, _id should be obfuscated
			// 	return doc;
			// });
			
			if (opts.expressions.length) {
				this.assignExpressionValues(idMap, opts.expressions, res, expResult);
			}			
		}
		itemCount = items.length;
		let facetRes = {};
		_.each(opts.request.facets, (value, key) => {
			let o = [];
			if (value.facetIndex >= 0) {
				if (res[value.pipeIndex] && _.isArray(res[value.pipeIndex][1])) {
					let fres = _.chunk(res[value.pipeIndex][1][value.facetIndex], 2);
					_.each(fres, (a) => {
						let k = {};
						k[a[0]] = parseInt(a[1]);
						o.push(k);
					});
					facetRes[key] = o;
				}
					
			}
			
		});

		endSerialize = new Date().getTime();

		let response = {
			totalFound: totalFound,
			start: opts.request.start || 0,
			count: itemCount,
			facets: facetRes,
			items: items,
			exprs: expResult,
			prepTimeMillis: opts.prepTime || 0,
			queryTimeMillis: endTime - startTime,
			serializeTimeMillis: endSerialize - startSerialize
		}
		return response
	}
}