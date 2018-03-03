process.on('unhandledRejection', (reason, p) => {
  console.log('Unhandled Rejection at: Promise', p, 'reason:', reason);
  // application specific logging, throwing an error, or other logic here
});

let config = {
  mode: 'http',
  port: 5268
};
let workerFn = config.mode === 'http' ? require('./server-http') : require('./server-tcp');

workerFn(config);
