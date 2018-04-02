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

	getIndexLinkName(cleanUp) {
		let a = this.prefix + ':LINK:' + uuid(); //{}'s make sure these temp keys are not replicated
		if (cleanUp) cleanUp.push(a);
		return a;
	}

	isTempIndex(index) {
		return _.startsWith(index, this.prefix + ':TMP:');
	}

	wrapPipe(pipe) {
		return pipe;
		// pipe.spredissort = pipe.createBuiltinCommand('spredis.sort').string;
		// pipe.dhashset = pipe.createBuiltinCommand('spredis.dhashset').string;
		// pipe.dhashget = pipe.createBuiltinCommand('spredis.dhashget').string;
		// pipe.dhashdel = pipe.createBuiltinCommand('spredis.dhashdel').string;
		// pipe.storerangebylex = pipe.createBuiltinCommand('spredis.storerangebylex').string;
		// pipe.storerangebyscore = pipe.createBuiltinCommand('spredis.storerangebyscore').string;
		// return pipe;
	}
}

module.exports = Prefixed