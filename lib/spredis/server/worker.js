const net = require('net');  
 
const JSONDuplexStream = require('./json-stream');

const Gateway = require('./gateway')
const server = net.createServer(); 

class ConnectionHandler {
  constructor(conn) {
    this.stream = JSONDuplexStream();
    this.gateway = new Gateway();
    this.conn = conn;
    conn
      .pipe(this.stream.in)
      .pipe(this.gateway)
      .pipe(this.stream.out)
      .pipe(conn);

    let self = this;
    conn.once('end', (err) => {
      if (err) {
        console.log(err.stack);
      }
      self.connectionDone();
    })

    conn.on('error', (err) => {
      self.error(err);
    });

    this.stream.in.once('error',  (err) => {
      self.connectionDone(err);
    });
    this.stream.out.once('error', (err) => {
      self.connectionDone(err);
    });

  }

  error(err) {
    console.error('connection error:', err.stack);
  }

  connectionDone(err) {
    if (err) {
      console.error('protocol error:', err.stack);
    }
    this.conn.unpipe();
    delete this.stream;
    delete this.gateway;
    delete this.conn;
  }
}


module.exports = function() {
    server.on('connection', (conn) => {
      new ConnectionHandler(conn);
    });  
    server.listen(5268, function() {  
      console.log('server listening on %j', server.address());
    });  
}
