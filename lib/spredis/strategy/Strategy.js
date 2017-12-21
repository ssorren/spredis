const constants = require('../constants');
const uuid = require('uuid').v4;
const ASC = 'ASC';
const DESC = 'DESC';
const _ = require('lodash');

module.exports = class Strategy {
	constructor(def, prefix) {
		this.type = def.typeDef;
		this.prefix = prefix;
		this.allIdsKey = prefix + ':ALLIDS';
		this.name = def.name
		this.indexName = def.indexName;
		this.dirtSortKey = this.prefix + ':DIRTYSORTDATA';

		
		this.tempValueKey = this.indexName + ':WSV';


		this.ascSortWeightKey = this.indexName + ':SW:A:';
		this.descSortWeightKey = this.indexName + ':SW:D:';
		

		this.ascSortPattern = this.ascSortWeightKey+'*';
		this.descSortPattern = this.descSortWeightKey+'*';
		// this.tempWeightKey = this.prefix + ':TEMPSORTWEIGHT:' + def.name;
		// console.log(this.indexName);
	}
		
	tempSortValueKey(lang) {
		return this.tempValueKey  + ':' + lang;
	}


	_exists(val) {
		return val !== null && val !== undefined && val !== '';
	}

	getTempIndexName() {
		return this.prefix + ':TMP:' + uuid();
	}

	get sep() {
		// console.log('SEP', SEP);
		return '';
	}

	get asc() {
		return constants.ASC;
	}

	get desc() {
		return constants.DESC;
	}
	get source() {
		return this.type.source;
	}
	get defaultValue() {
		return this.type.defaultValue;
	}
	
	get supportsSource() {
		return this.type.supportsSource;
	}
	toRank(val) {
		return this.type.toRank(val, this.sortPrecision);
	}

	toLex(id, pos) {
		return this.type.toLex(id, pos);
	}

	toSort(val) {
		return this.type.toSort(val, this.sortPrecision);
	}

	actualValue(val) {
		return this.type.actualValue(val);
	}

	idFromLex(lex) {
		return this.type.idFromLex(val);;
	}

	ascSortValue(val) {
		return this.type.toAlphaSort(val, this.sortPrecision);
	}

	descSortValue(val) {
		return this.type.toDescAlphaSort(val, this.sortPrecision);
	}

	getValFromSource(doc) {
		return null;
	}

	sortPattern(lang) {
		return this.indexName + ':S:'+ lang + ':*';
	}

	facetPattern(lang) {
		return this.indexName + ':A:'+ lang;
	}

	sortWeightName(id, lang) {
		return this.indexName + ':W:'+ lang + ':' + String(id);
	}

	sortAllValuesName(lang) {
		return this.indexName + ':SALLVALS:'+ lang;
	}

	facetName(lang) {
		return this.indexName + ':A:' + lang;
	}

	sortIndex(lang) {
		return this.indexName + ':SALL:' + lang;
	}

	getQueryValue(val) {
		return val;
	}


	*indexField(redis, id, val, pos) {
		//empty implementation
	}

	indexForValue(val, lang) {
		return this.indexName + ':V:' + lang;
	}

	indexForSpecificValue(val) {
		return null;
	}

	bucketsForValue(val) {
		return this.indexName + ':BL';
	}

	get sortPrecision() {
		return this.type ? this.type.sortPrecision : null;
	}
	get lang() {
		return this.type ? this.type.lang : 'en';
	}

	set lang(l) {

	}

	setAscSortValue(pipe, id, val, pos) {
		pipe.setBuffer(this.ascSortWeightKey + this.toLex(id, pos), this.ascSortValue(val));
	}

	setDescSortValue(pipe, id, val, pos) {
		pipe.setBuffer(this.descSortWeightKey + this.toLex(id, pos), this.descSortValue(val));
	}

	prefixSearchIndex(lang) {
		return null;
	}

	suffixSearchIndex(lang) {
		return null;
	}

	indexForSearchValue(pipe, val, lang, hint) {
		val =  _.isArray(val) && val.length == 1 ? val[0] : val;
		let cleanUp = [];
		if (_.isArray(val) && val.length === 0) return {index: hint, cleanUp: null};
		if (_.isArray(val)) { //hadling the ors
			let indeces = [];
			for (var i = 0; i < val.length; i++) {
				let v = val[i]
				let index = this.setForSingleValue(pipe,v, lang, hint, cleanUp);
				indeces.push(index);
			}
			let finalStore  = this.getTempIndexName();
			cleanUp.push(finalStore);
			let len = indeces.length;
			let command = [finalStore];
			command.push.apply(command, indeces);
			// command.push('WEIGHTS');
			// for (var i = 0; i < len; i++) {
			// 	command.push(0);
			// }
			pipe.sunionstore.apply(pipe, command);
			return {index: finalStore, cleanUp: cleanUp}
		}

		let index = this.setForSingleValue(pipe, val, lang, hint, cleanUp);
		return {index: index, cleanUp: cleanUp}
		
	}

	unIndexField(pipe, id, val, pos) {
		//empty implementation
	}

	indexField(pipe, id, val, pos) {
		//empty implementation
	}

	
}