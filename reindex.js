const spredis = require('./index')({});
spredis.initialize().then( () => {
	
	spredis.useNamespace('revsale').then(ns=> {
		// ns.drop().then(()=> {
		// 	console.log('drop complete');
		// 	// spredis.quit();
		// })
		ns.fullReIndex().then(()=> {
			console.log('re-indexing complete');
			spredis.quit();
		})
	});	
} )
