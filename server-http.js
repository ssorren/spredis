const cluster = require('cluster');
const numCPUs = require('os').cpus().length;
const Spredis = require('./lib/spredis/Spredis');

function go(config) {
	const Koa = require('koa');
	const Router = require('koa-router');
	const body = require('koa-json-body')
	const spredis = new Spredis(config ? config.redis : null);

	const respondError = (ctx, e) => {
		ctx.response.status = 500;
		ctx.response.type='json';
		ctx.response.body = {error: e.stack};
	}

	spredis.initialize().then( () => {
		// let ns = spredis.defaultNamespace;
		const app = new Koa();
		app.use(body({ limit: '512kb', fallback: true }))
		const router = new Router();
		router.get('/', (ctx, next) => {
		  ctx.response.body = 'Looking for something?';
		});

		router.post(`/:ns/search`, async (ctx, next) => {
			try {
				let ns = await spredis.useNamespace(ctx.params.ns);
				if (!ns) throw new Error(`Can not find namespace: '${ctx.params.ns}'`);
				let res = await ns.search(ctx.request.body);
				ctx.response.type='json';
				ctx.response.body = res;
			} catch (e) {
				respondError(ctx, e);
			}
		});

		router.post(`/:ns/addDocuments`, async (ctx, next) => {
			try {
				let ns = await spredis.useNamespace(ctx.params.ns);
				if (!ns) throw new Error(`Can not find namespace: '${ctx.params.ns}'`);
				let res = await ns.addDocuments(ctx.request.body);
				ctx.response.type='json';
				ctx.response.body = res;
			} catch (e) {
				respondError(ctx, e);
			}
		});

		router.post(`/:ns/deleteDocuments`, async (ctx, next) => {
			try {
				let ns = await spredis.useNamespace(ctx.params.ns);
				if (!ns) throw new Error(`Can not find namespace: '${ctx.params.ns}'`);
				let res = await ns.deleteDocuments(ctx.request.body);
				ctx.response.type='json';
				ctx.response.body = res;
			} catch (e) {
				respondError(ctx, e);
			}
		});

		router.post('/createNamespace', async (ctx, next) => {
			try {
				let res = await spredis.createNamespace(ctx.request.body);
				let id = ctx.params.id;
				ctx.response.type='json';
				ctx.response.body = res;
			} catch (e) {
				respondError(ctx, e);
			}
		});

		router.get('/:ns/doc/:id', async (ctx, next) => {
			try {
				let ns = await spredis.useNamespace(ctx.params.ns);
				let id = ctx.params.id;

				if (!ns) throw new Error(`Can not find namespace: '${ctx.params.ns}'`);
				if (!id === null || id === undefined) throw new Error(`No id supplied'`);
				let res = await ns.getDocument(id);
				ctx.response.type='json';
				ctx.response.body = res;
			} catch (e) {
				respondError(ctx, e);
			}
		});

		router.get('/:ns/namespaceConfig', async (ctx, next) => {
			try {
				let ns = await spredis.useNamespace(ctx.params.ns);

				if (!ns) throw new Error(`Can not find namespace: '${ctx.params.ns}'`);

				let res = await ns.getNamespaceConfig();
				ctx.response.type='json';
				ctx.response.body = res;
			} catch (e) {
				respondError(ctx, e);
			}
		});

		app.use(router.routes());
		app.use(router.allowedMethods());
		app.listen(process.env.PORT || config.port || 5268);
	});	
}

module.exports = function(config) {
	if (cluster.isMaster && numCPUs > 1) {
	  console.log(`Master ${process.pid} is running`);

	  // Fork workers.
	  for (let i = 0; i < numCPUs; i++) {
	    cluster.fork();
	  }

	  cluster.on('exit', (worker, code, signal) => {
	    console.log(`worker ${worker.process.pid} died`);
	  });
	} else {
	  go(config);
	  console.log(`Worker ${process.pid} started`);
	}
}