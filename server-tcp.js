


const cluster = require('cluster');
const numCPUs = require('os').cpus().length;

module.exports = function (config) {
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
    // Workers can share any TCP connection
    const worker = require('./lib/spredis/server/worker');
    const Spredis = require('./lib/spredis/Spredis');
    const spredis = new Spredis();
    spredis.initialize().then( () => {
      worker(config);
    });
    

    console.log(`Worker ${process.pid} started`);
  }
}