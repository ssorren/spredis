/*
	entry point for a server application. launch in HTTP or TCP listener mode
*/

process.on('unhandledRejection', (reason, p) => {
  console.log('Unhandled Rejection at: Promise', p, 'reason:', reason);
  // application specific logging, throwing an error, or other logic here
});

let config = {
  mode: 'tcp',
  port: 5268
};
let workerFn = config.mode === 'http' ? require('./server-http') : require('./server-tcp');

workerFn(config);
