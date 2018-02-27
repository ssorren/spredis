const spredis = require('./index')({});
spredis.initialize().then( () => {
	
	spredis.useNamespace('revsale').then(ns=> {
		ns.fullReIndex().then(()=> {
			console.log('re-indexing complete');
		})
	});	
} )
