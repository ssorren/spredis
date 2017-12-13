module.exports = function(redis) {
	var fs = require('fs');

	redis.defineCommand('facet', {
		numberOfKeys: 4,
		lua: fs.readFileSync(__dirname + '/literal-facet.lua', 'utf8')
	});

	redis.defineCommand('facetTest', {
		numberOfKeys: 3,
		lua: fs.readFileSync(__dirname + '/literal-facet-test.lua', 'utf8')
	});

	redis.defineCommand('storeItems', {
		numberOfKeys: 3,
		lua: fs.readFileSync(__dirname + '/store-items.lua', 'utf8')
	});
	
	redis.defineCommand('storeRange', {
		numberOfKeys: 2,
		lua: fs.readFileSync(__dirname + '/store-range.lua', 'utf8')
	});

	redis.defineCommand('unIndex', {
		numberOfKeys: 1,
		lua: fs.readFileSync(__dirname + '/un-index.lua', 'utf8')
	});

	redis.defineCommand('storeSort', {
		numberOfKeys: 5,
		lua: fs.readFileSync(__dirname + '/store-sort.lua', 'utf8')
	});

	redis.defineCommand('reSort', {
		numberOfKeys: 5,
		lua: fs.readFileSync(__dirname + '/re-sort.lua', 'utf8')
	});

	redis.defineCommand('stripDups', {
		numberOfKeys: 2,
		lua: fs.readFileSync(__dirname + '/strip-dup-ids.lua', 'utf8')
	});

	redis.defineCommand('multiSort', {
		numberOfKeys: 2,
		lua: fs.readFileSync(__dirname + '/multi-key-sort.lua', 'utf8')
	});
	redis.defineCommand('multiInterSort', {
		numberOfKeys: 3,
		lua: fs.readFileSync(__dirname + '/multi-interstore-sort.lua', 'utf8')
	});
	redis.defineCommand('convertToSet', {
		numberOfKeys: 2,
		lua: fs.readFileSync(__dirname + '/convert-to-set.lua', 'utf8')
	});
}