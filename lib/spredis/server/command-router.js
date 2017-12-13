'use strict';
/* TODO: testing only, need to figure out the correct way to get the appropriate namespace*/
var spredis = require('../../../index.js')({});
var config = require('../../../dev/sample-config');




var ns;
(async function() {
	// var db = require('../db')({});
	ns = await spredis.createNamespace(config);
})()
// getItGoing()





/* keeper code starts here */
async function execCommand(command, callback) {
	try  {
		var res = await ns.search(command.input);
		callback(null, res);
	} catch (e) {
		callback(e);
	}
	
}
module.exports = function(command, callback) {
	// try {
		if (!ns) return callback(new Error('server not ready yet...please hold'))
		execCommand(command, callback)
	} catch(e) {
		console.log(e.stack);
		callback(e);
	}	
}