/*
	geo spatial index field
	usually exressed by an array of floats
	[lat, lon]
*/

const Range = require('./Range');
const _ = require('lodash');

const VALID_UNIT = {
	'km': 1,
	'mi': 1,
	'm': 1,
	'f': 1
}
module.exports = class GeoStrategy extends Range {
	_exists(val) {
		if (val === null || val === undefined || val === '') return false;
		if (val.lat && val.lon) return true;
		if (_.isArray(val)) {
			return val[0] !== null && val[0] !== undefined && val[0] !== '' && val[1] !== null && val[1] !== undefined && val[1] !== '';
		}

		return false;
	}

	_queryExists(val) {
		
		if (val === null || val === undefined || val === '') return false;
		if (_.isArray(val) && val.length === 0) return false;
		return true;
	}

	toCompositeQuery(val) {
		
		val = this.getQueryValue(val);
		return [val.from[0], val.from[1], val.radius, val.unit, val.radiusField || ''];
	}

	toIndexableValue(val) {
		if (!this._exists(val)) return undefined;
		return {lat: parseFloat(val[0]), lon: parseFloat(val[1])};
	}

	indexArguments(val) {
		// console.log('val:',val);
		val = this.toIndexableValue(val);
		if (this._exists(val)) {
			return [this.name, 1, val.lat, val.lon];
			// for (var i = 0; i < val.length; i++) {
			// 	command.push(this.toRank( val[i] ))
			// }
			// return command;
		}
		return null;
	}
	
	getQueryValue(val) {
		let query = {};
		if (_.isString(val)) throw new Error(`Invalid query type for geo field (string:${val})`);
		if (_.isArray(val)) throw new Error(`Invalid query type for geo field (array:${val})`);
		if (!this._exists(val.from)) throw new Error(`Geo radius query missing from ${val}`);
		query.from = val.from;
		query.radius = val.radius ? parseFloat(val.radius) : '+inf';
		if (query.radius !== '+inf' && isNaN(query.radius)) throw new Error(`Geo search error, could not parse radius: ${val}`);
		query.unit = (val.unit || 'km').toLowerCase();
		if (!VALID_UNIT[query.unit]) throw new Error(`Geo search error: invalid unit (${val.unit})`);
		query.radiusField = val.radiusField;
		return  query;
	}

	setForSingleValue(pipe, val, lang, hint, cleanUp) {
		let query = this.getQueryValue(val);
		let store = this.getTempIndexName(cleanUp);
	
		let indexName = this.indexForValue(val, lang);
		
		let radiusField = null;
		if (val.radiusField) {
			let rStrat = this.namespace.getStrategy(val.radiusField);
			if (!rStrat) {
				throw new Error(`(geo:radiusField) Could not find strategy for ${val.radiusField}`);
			}
			radiusField = rStrat.name;// rStrat.indexForValue(null, lang);
		} else if (!query.radius) {
			throw new Error(`(geo:search) Geo searches must have a radius or a radiusField`);
		}

		if (radiusField) {
			pipe.call('spredis.storerangebyradius', this.prefix, this.name, store, hint || '', query.from[0], query.from[1], 1, query.unit, radiusField);
		} else {
			pipe.call('spredis.storerangebyradius', this.prefix, this.name, store, hint || '', query.from[0], query.from[1], query.radius, query.unit);	
		}
		return store;
	}

	async unIndexField(pipe, id, val, pos, lang='en') {
		var exists = this._exists(val);

		if (exists) {
			var rank = this.toRank(val);
			var indexName = this.indexForValue(val, lang);
			pipe.call('spredis.georem',indexName, id, rank[0], rank[1]);
		}
		
		return []
	}

	async indexField(pipe, id, val, pos, lang='en') {
		var exists = this._exists(val);
		if (exists) {
			var rank = this.toRank(val);
			var indexName = this.indexForValue(val, lang);
			pipe.call('spredis.geoadd',indexName, id, pos, rank[0], rank[1]);
		}
		
		return []
	}
}