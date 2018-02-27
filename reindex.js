const swoosh = require('./index')({});
swoosh.useNamespace('revsale').then(ns=> {
	ns.fullReIndex().then(()=> {
		console.log('re-indexing complete');
	})
});