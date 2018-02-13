const cluster = require('cluster');
const numCPUs = require('os').cpus().length;

const postCommands = [
	'addDocuments',
	'deleteDocuments',
	'namespaceConfig',
	'search'
];

const Spredis = require('./lib/spredis/Spredis');

function go(config) {
	const Koa = require('koa');
	const Router = require('koa-router');
	const body = require('koa-json-body')
	const spredis = new Spredis();

	spredis.initialize().then( () => {
		// let ns = spredis.defaultNamespace;
		const app = new Koa();
		app.use(body({ limit: '100kb', fallback: true }))
		const router = new Router();
		router.get('/', (ctx, next) => {
		  ctx.response.body = 'Looking for something?';
		});
		for (var i = 0; i < postCommands.length; i++) {
			let com = postCommands[i];
			router.post(`/:ns/${com}`, async (ctx, next) => {
				try {
					let ns = spredis.useNamespace(ctx.params.ns);
					if (!ns) throw new Error(`Can not find namespace: '${ctx.params.ns}'`);
					let res = await ns[com](ctx.request.body);
					ctx.response.type='json';
					ctx.response.body = res;
				} catch (e) {
					ctx.response.status = 500;
					ctx.response.type='json';
					ctx.response.body = {error: e.stack};
				}
			});
		}
		router.get('/:ns/doc/:id', async (ctx, next) => {
			try {
				let ns = spredis.useNamespace(ctx.params.ns);
				let id = ctx.params.id;

				if (!ns) throw new Error(`Can not find namespace: '${ctx.params.ns}'`);
				if (!id === null || id === undefined) throw new Error(`No id supplied'`);
				let res = await ns.getDocument(id);
				ctx.response.type='json';
				ctx.response.body = res;
			} catch (e) {
				ctx.response.status = 500;
				ctx.response.type='json';
				ctx.response.body = {error: e.stack};
			}
		})
		app.use(router.routes());
		app.use(router.allowedMethods());
		app.listen(config.port || 5268);
	});	
}

module.exports = function() {
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