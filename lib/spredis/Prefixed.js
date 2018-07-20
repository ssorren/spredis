/*
	simple class to extend providing some common functionality to spredis classes
*/
const uuid = require('uuid').v4;
const _ = require('lodash');
class Prefixed {
	get prefix() {
		return this._prefix;
	}

	set prefix(prefix) {
		this._prefix = prefix;
	}
	
	getTempIndexName(cleanUp) {
		let a = this.prefix + ':TMP:' + uuid(); //{}'s make sure these temp keys are not replicated
		if (cleanUp) cleanUp.push(a);
		return a;
	}

	getCursorName() {
		return this.prefix + ':CURS:' + uuid()
	}
	
	getIndexLinkName(cleanUp) {
		let a = this.prefix + ':LINK:' + uuid(); //{}'s make sure these temp keys are not replicated
		if (cleanUp) cleanUp.push(a);
		return a;
	}

	isTempIndex(index) {
		return _.startsWith(index, this.prefix + ':TMP:');
	}
}

module.exports = Prefixed