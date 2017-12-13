'use strict';
var Swoosh = require('./lib/spredis/Spredis');

module.exports = function(redisConfig) {
	return new Swoosh(redisConfig);
}