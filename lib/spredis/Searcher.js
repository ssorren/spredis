const _ = require('lodash');
const uuid = require('uuid').v4;
// let pack = JSON.stringify,
// 	unpack = JSON.parse;
const 	TEXT_QUERY = require('./constants').TEXT_QUERY,
		EXPRESSION = require('./constants').EXPRESSION;

const pack = require('snappy').compressSync;
const unpack = require('snappy').uncompressSync;

const textUtils = require('../text');
const Text = require('./strategy/Text');

module.exports = class Searcher {
	constructor(prefix, defs, compounds) {
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

	getTempIndexName() {
		return this.prefix + ':TMP:' + uuid(); //{}'s make sure these temp keys are not replicated
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
		// console.log("strats", strats.length);
		let indeces = [];
		for (let i = 0; i < strats.length; i++) {
			let strat = strats[i];
			let stratHint = hint;
			// let stratIndeces = [];
			for (let k = 0; k < crit.query.length; k++) {
				let ifv = strat.indexForSearchValue(pipe, crit.query[k], crit.lang, stratHint);
				if (ifv) {
					if (ifv.index) {
						// stratIndeces.push(ifv.index);
						stratHint = ifv.index;

					}
					if (ifv.cleanUp && ifv.cleanUp.length) cleanUp.push.apply(cleanUp, ifv.cleanUp);	
				}	
			};

			indeces.push(stratHint)
		};
		// console.log("indeces:",indeces)
		indeces = _.uniq(indeces);
		if (indeces.length > 1) {
			let store = this.getTempIndexName();
			cleanUp.push(store);
			let com = [store, indeces.length]
			com.push.apply(com, indeces);
			pipe.zunionstoreBuffer.apply(pipe, com);
			return {index: store, defer: null}
		} else if (indeces.length) {
			return {index: indeces[0], defer: null};
		}
		return {index: hint, defer: null};
	}

	textPhase(pipe, queries, fields, cleanUp, hint) {
		// console.log("hint:", hint, queries);
		if (!queries || queries.length === 0) return {index: hint, defer: null};
		let indeces = [];
		for (let i = 0; i < queries.length; i++) {
			let res = this.individualTextQuery(pipe, queries[i], fields, cleanUp, hint);
			if (res && res.index && res.index != hint) {
				indeces.push(res.index);
			}
		};

		if (indeces.length > 1) {
			let store = this.getTempIndexName();
			cleanUp.push(store);
			let com = [store, indeces.length]
			com.push.apply(com, indeces);
			pipe.zunionstoreBuffer.apply(pipe, com);
			return {index: store, defer: null}
		} else if (indeces.length) {
			return {index: indeces[0], defer: null};
		}
		return {index: hint, defer: null};
	}

	performPhase(pipe, crits, cleanUp, hint, forceDefers) {
		let indeces = [];
		let defers = [];
		// console.log("NO DEFERS!!!!!!");
		if (!crits) return {index: hint, defer: null};
		for (let i = 0; i < crits.length; i++) {
			let val = crits[i].val;
			let strat = crits[i].strat;
			let lang = crits[i].lang;
			if (!forceDefers && _.isArray(val) && val.length > 1) {
				defers.push(crits[i]);
				continue;
			}
			
			let ifv = strat.indexForSearchValue(pipe, val, lang, hint);
			if (ifv) {
				if (ifv.index) {
					indeces.push(ifv.index);
					hint = ifv.index;
				}
				if (ifv.cleanUp && ifv.cleanUp.length) cleanUp.push.apply(cleanUp, ifv.cleanUp);	
			}
		};
		if (indeces.length === 1) {
			return {index: indeces[0], defer: defers};
		} else if (indeces.length > 1) {
			let inter = this.getTempIndexName();
			indeces.unshift(indeces.length);
			indeces.unshift(inter);
			cleanUp.push(inter)
			pipe.zinterstoreBuffer.apply(pipe, indeces);
			return {index: inter, defer: defers};
 		}

 		return {index: hint, defer: null};
	}

	compoundPhase(pipe, query, lang, cleanUp) {
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
					let temp = this.getTempIndexName();
					cleanUp.push(temp);
					let command = [temp, permutaions.length];
					for (let i = 0; i < permutaions.length; i++) {
						command.push(index.indexForValue(permutaions[i], lang));
					}
					command.push('WEIGHTS');
					for (let i = 0; i < permutaions.length; i++) {
						command.push(0);
					}
					pipe.zunionstoreBuffer.apply(pipe, command);
					compoundIndeces.push(temp);
				}
			}
		}
		let compoundIndex = null;
		if (compoundIndeces.length === 1) {
			compoundIndex = compoundIndeces[0];
		} else if (compoundIndeces.length) {
			let temp = this.getTempIndexName();
			cleanUp.push(temp);
			let command = [temp, compoundIndeces.length];
			for (let i = 0; i < compoundIndeces.length; i++) {
				command.push(compoundIndeces[i]);
			}
			command.push('WEIGHTS');
			for (let i = 0; i < compoundIndeces.lengthh; i++) {
				command.push(0);
			}
			pipe.zinterstoreBuffer.apply(pipe, command);
			compoundIndex = temp;
		}
		return compoundIndex;
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
			pipe.zinterstoreBuffer(store, 2, idSet, distanceIndex, 'WEIGHTS', 0, 1);
			
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

	expressions(fields, query) {
		let expr = _.remove(fields, (f) => {
			return f === EXPRESSION;
		});
		let exprs = [];
		if (expr.length) {
			_.forEach(query[expr[0]], (val, key) => {
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
		}
		return exprs;
	}

	buildFacets(pipe, facets, indexName, lang, cleanUp) {
		let self = this;
		let fcount = 0;
		let facetResultIndex = pipe.length;
		_.each(facets, (value, key) => {
			let strat = self.getStrategy(value.field);
			if (!strat.type.facet) throw new Error('Facet not enabled for field "' + value.field + '"');
			value.resultIndex = self.getTempIndexName()
			let order = (value.order ? value.order.toUpperCase() : null) || 'ASC';
			let count = value.count || 10;
			pipe.facet(indexName, strat.facetPattern(lang), value.resultIndex, order, count);
			cleanUp.push(value.resultIndex);
			value.facetIndex = facetResultIndex + fcount;
			fcount++;
		})
	}

	async search(request) {
		let prepStartTime = new Date().getTime();
		let myConn = this.redis.queryServer();
		let pipe = myConn.pipeline();
		// let pipe = this.redis.multi();

		let query = request.query ? _.cloneDeep(request.query) : null;
		let originalQuery = request.query;

		
		let indeces = [];
		let cleanUp = [];
		let lang = this.getRequestLang(request);

		/*
			we're going to call the compound phase first as it will remove fields from the query- 
			leaving less to search for.
		*/
		let compoundIndex = this.compoundPhase(pipe, query, lang, cleanUp);
		// console.log('compoundIndex', compoundIndex);
		let fields = _.keys(query);
		let textQueries = this.textQueries(fields, query, lang);
		let expressions = this.expressions(fields, query);

		let strats = _.map(fields, (f) => {
			let strat = this.getStrategy(f);
			let val = query[f];
			if (!strat) throw new Error('No search strategy for ' + f + ' (index probably set to false in defnition, or no definition exists)');
			return {val: val, strat:strat, lang: lang};
		})

		let literalStrats = _.filter(strats,(s) => {
			return s.strat.type.searchType === 'L';
		});

		let rankStrats = _.filter(strats,(s) => {
			return s.strat.type.searchType === 'R';
		});

		let geoStrats = _.filter(strats, (s) => {
			return s.strat.type.searchType === 'G';
		})
		
		// we'll do literal without defers first, these queries should be the fastest
		//defering in clauses is actually way slower when dealing with large initial data sets,
		let literalIndex = this.performPhase(pipe, literalStrats, cleanUp, compoundIndex, true);
		let defers = literalIndex.defer;

		//TODO: pass $textFields array
		let textIndex = this.textPhase(pipe, textQueries, null, cleanUp, literalIndex.index);

		/* don't think deferring will save us any time a this point */
		let rangeIndex = this.performPhase(pipe, rankStrats, cleanUp, textIndex.index, true);

		/* take care of our literal ors */
		//defering in clauses is actually way slower when dealing with large initial data sets,
		// let literalDeferred = this.performPhase(pipe, defers, cleanUp, rangeIndex.index, true);
		
		/*
			finally do the geo search, saving for last as we are calculating distance
			need to do some tests to see if this is faster than range searching
		 */
		let geoIndex = this.performPhase(pipe, geoStrats, cleanUp, rangeIndex.index);

		if (geoIndex.index) {
			indeces.push(geoIndex.index);
		}

		let hasQuery = indeces.length;

		let sort = request.sort || null;
		let count = request.count || ((request.count === 0) ? 0 : 10);
		let start = request.start || 0;
		let idsOnly = request.idsOnly;

		let multiSort = false;
		let facetResultIndex = 0;
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
		pipe.zcard(indexName);
		let exprSet = null;
		
		if (count > 0) {
			if (!idsOnly) {
				if (expressions.length) {
					// exprIds = this.getTempIndexName();
					// cleanUp.push(exprIds);
					// pipe.sortBuffer(indexName, 'by', 'nosort', 'get', '#', 'LIMIT', start, count, 'STORE', exprIds);
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
		
		// console.log(res[checkInterStoreIndex]);
		let facetRes = {};
		_.each(opts.request.facets, (value, key) => {
			let o = {};
			// console.log(res[value.facetIndex])
			let fres = _.chunk(res[value.facetIndex][1], 2);
			
			_.each(fres, (a) => o[ a[0] ] = parseInt(a[1]));
			facetRes[key] = o;
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