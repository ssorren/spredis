const uuid = require('uuid').v4;

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

	wrapPipe(pipe) {
		pipe.spredissort = pipe.createBuiltinCommand('spredis.sort').string;
		pipe.dhashset = pipe.createBuiltinCommand('spredis.dhashset').string;
		pipe.dhashget = pipe.createBuiltinCommand('spredis.dhashget').string;
		pipe.dhashdel = pipe.createBuiltinCommand('spredis.dhashdel').string;
		pipe.storerangebylex = pipe.createBuiltinCommand('spredis.storerangebylex').string;
		pipe.storerangebyscore = pipe.createBuiltinCommand('spredis.storerangebyscore').string;
		return pipe;
	}
}

module.exports = Prefixed