/* trimmed down version of 'split' */

const through = require('through')
const Decoder = require('string_decoder').StringDecoder

function emit(stream, piece) {
    stream.queue(piece);
}



module.exports = function (matcher, mapper, options) {
  let decoder = new Decoder();
  let soFar = '';
  let maxLength = options && options.maxLength;
  let trailing = options && options.trailing === false ? false : true;
  // not using mapper or matcher, we don't need to be flexible since we're using a custom implementation
  // if('function' === typeof matcher) mapper = matcher, matcher = null;
  // matcher = matcher || /\r?\n/;

  function next (stream, buffer) {
    let pieces = ((soFar != null ? soFar : '') + buffer).split('\n');
    soFar = pieces.pop();
    if (maxLength && soFar.length > maxLength) return stream.emit('error', new Error('maximum buffer reached'));
    for (let i = 0; i < pieces.length; i++) {
      let piece = pieces[i];
      emit(stream, piece);
    }
  }

  return through(function (b) {
    next(this, decoder.write(b));
  },
  function () {
    if(decoder.end) next(this, decoder.end());
    if(trailing && soFar != null) emit(this, soFar);
    this.queue(null);
  });
}