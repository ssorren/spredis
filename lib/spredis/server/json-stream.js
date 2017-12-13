var EE = require('events').EventEmitter;
var split = require('./split');
var through = require('through');
var combine = require('stream-combiner');
var stringify = JSON.stringify;

module.exports = JSONDuplexStream;

function JSONDuplexStream() {

  var self = new EE();


  // input

  var parse = through(function(chunk) {
    chunk = chunk.toString().trim();
    if (chunk.length) {
      try {
        this.queue(JSON.parse(chunk));
      }
      catch (err) {
        this.emit('error', err);
      }
    }
  });

  var input = combine(split(), parse);
  self.in = input;


  // output

  var serialize = through(function(chunk) {
    this.queue(stringify(chunk) + '\n');
  });

  self.out = serialize;

  return self;
}
