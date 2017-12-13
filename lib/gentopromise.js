
module.exports = function(gen) {
	gen.next = function(ret) {
		if (ret.done) return this.resolve(ret.value);
		var value = toPromise.call(ctx, ret.value);
		if (value && isPromise(value)) return value.then(onFulfilled, onRejected);
	}
	var a = Promise.all(gen)
}