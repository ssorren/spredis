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

}

module.exports = Prefixed