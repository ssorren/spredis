var extend = require('util')._extend;  
var inherits = require('util').inherits;  
var Transform = require('stream').Transform;
var Commander = require('./Commander');

module.exports = Gateway;

inherits(Gateway, Transform);

var defaultOptions = {  
  highWaterMark: 10,
  objectMode: true
};

function Gateway(options) {  
  if (! (this instanceof Gateway)) {
    return new Gateway(options);
  }

  options = extend({}, options || {});
  options = extend(options, defaultOptions);
  this.commander = new Commander();
  Transform.call(this, options);
}


/// _transform

Gateway.prototype._transform = _transform;

function _transform(event, encoding, callback) {  
  // if (! event.id)
  //   return handleError(new Error('event doesn\'t have an `id` field'));


  function pushed(err, reply) {
    if (err) {
      handleError(err);
    }
    else {
      
      callback(null, reply);
    }
  }

  function handleError(err) {
    console.log(err.stack);
    var reply = {
      success: false,
      error: err.message
    };

    callback(null, reply);
  }

  this.commander.exec(event, pushed);
};
