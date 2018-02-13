'use strict';
var Spredis = require('./lib/spredis/Spredis');

module.exports = function(redisConfig) {
	return new Spredis(redisConfig);
}