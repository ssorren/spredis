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
		return this.indexName + ':A:'+ lang + ':';
	}

	sortWeightName(id, lang) {
		return this.indexName + ':W:'+ lang + ':' + String(id);
	}

	sortAllValuesName(lang) {
		return this.indexName + ':SALLVALS:'+ lang;
	}

	facetName(id, lang) {
		return this.indexName + ':A:' + lang + ':' + String(id);	
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
			let command = [finalStore, len];
			command.push.apply(command, indeces);
			command.push('WEIGHTS');
			for (var i = 0; i < len; i++) {
				command.push(0);
			}
			pipe.zunionstoreBuffer.apply(pipe, command);
			return {index: finalStore, cleanUp: cleanUp}
		}

		let index = this.setForSingleValue(pipe, val, lang, hint, cleanUp);
		return {index: index, cleanUp: cleanUp}
		
	}

	unIndexField(pipe, id, val, pos) {
		var exists = this._exists(val);
		if (exists) {
			var indexName = this.indexForValue(val, this.lang);
			var specIndexName = this.indexForSpecificValue(val);
			pipe.zremBuffer(indexName, this.toLex(id, pos));
			if (specIndexName) pipe.zremBuffer(specIndexName, this.toLex(id, pos));
			pipe.delBuffer(this.facetName(id, this.lang), this.actualValue(val));
			pipe.zrem(this.sortIndex(this.lang), this.toLex(id, pos));
			// pipe.del(this.ascSortWeightKey + this.toLex(id, pos));
			// pipe.del(this.descSortWeightKey + this.toLex(id, pos));
		}
	}

	indexField(pipe, id, val, pos) {
		var exists = this._exists(val);
		
		if (this.type.sort && !this.type.array) {
			// this.setAscSortValue(pipe, id, val, pos);
			// this.setDescSortValue(pipe, id, val, pos);

			if (this.type.sortStrategy === 'L') {
				let v = this._exists(val) ? this.toSort(val) : '';

				pipe.storeSort(
					this.sortAllValuesName(this.lang),
					this.sortIndex(this.lang),
					v,
					this.toLex(id, pos),
					String(this.type.alpha ? 1 : 0)
				);
			} else {
				let v = this._exists(val) ? this.toSort(val) : 0;

				pipe.zadd(this.sortIndex(this.lang), v, this.toLex(id, pos))
			}

		}
		
		if (exists) {
			var rank = this.toRank(val);
			var indexName = this.indexForValue(val, this.lang);
			var specIndexName = this.indexForSpecificValue(val, this.lang);
			
			pipe.zaddBuffer(indexName, rank, this.toLex(id, pos));
			if (specIndexName) pipe.zaddBuffer(specIndexName, rank, this.toLex(id, pos));

			if (this.type.facet) {
				pipe.setBuffer(this.facetName(id, this.lang), this.actualValue(val));
			}
			if (this.type.supportsPrefix && this.type.prefix) {
				let pIndex = this.prefixSearchIndex(this.lang);
				//NX options is important here. to save space, we're re-using the sort index for literals, don't want to overwrite the score
				if (pIndex) pipe.zaddBuffer(pIndex, 'NX', 0, this.toSort(val)); 

			}

			if (this.type.supportsSuffix && this.type.suffix) {
				let sIndex = this.suffixSearchIndex(this.lang);
				let suffix = this.toSort(val).split("").reverse().join("");
				//NX options is important here. to save space, we're re-using the sort index for literals, don't want to overwrite the score
				if (sIndex) pipe.zaddBuffer(sIndex, 'NX', 0, suffix); 

			}
		}
		return 1
	}

	
}