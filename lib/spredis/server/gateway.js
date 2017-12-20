var extend = require('util')._extend;  
var Transform = require('stream').Transform;
var Commander = require('./Commander');



// inherits(Gateway, Transform);
const defaultOptions = {  
  highWaterMark: 10,
  objectMode: true
};

class Gateway extends Transform {

  constructor(options) {
    options = extend({}, options || {});
    options = extend(options, defaultOptions);
    super(options);
    this.commander = new Commander(); 
  }

  static pushed(err, reply, callback) {
    if (err) {
      return callback(null, {
        success: false,
        error: err.message
      });
    }
    callback(null, reply)
  }
  _transform(event, encoding, callback) {
    // let pushed = 
    this.commander.exec(event, (err, reply) => {
      Gateway.pushed(err, reply, callback)
    });
  }
}

module.exports = Gateway;
// function Gateway(options) {  
//   if (! (this instanceof Gateway)) {
//     return new Gateway(options);
//   }

//   options = extend({}, options || {});
//   options = extend(options, defaultOptions);
//   this.commander = new Commander();
//   Transform.call(this, options);
// }


// /// _transform

// Gateway.prototype._transform = _transform;

// function _transform(event, encoding, callback) {  
//   // if (! event.id)
//   //   return handleError(new Error('event doesn\'t have an `id` field'));


//   function pushed(err, reply) {
//     if (err) {
//       handleError(err);
//     }
//     else {
      
//       callback(null, reply);
//     }
//   }

//   function handleError(err) {
//     console.log(err.stack);
//     var reply = {
//       success: false,
//       error: err.message
//     };

//     callback(null, reply);
//   }

//   this.commander.exec(event, pushed);
// };
