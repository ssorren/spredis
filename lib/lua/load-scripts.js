module.exports = function(redis) {
	var fs = require('fs');

	redis.defineCommand('facet', {
		numberOfKeys: 3,
		lua: fs.readFileSync(__dirname + '/literal-facet.lua', 'utf8')
	});

	redis.defineCommand('allFacets', {
		numberOfKeys: 2,
		lua: fs.readFileSync(__dirname + '/all-facets.lua', 'utf8')
	});

	redis.defineCommand('storeItems', {
		numberOfKeys: 3,
		lua: fs.readFileSync(__dirname + '/store-items.lua', 'utf8')
	});
	
	redis.defineCommand('storeRange', {
		numberOfKeys: 4,
		lua: fs.readFileSync(__dirname + '/store-range.lua', 'utf8')
	});

	redis.defineCommand('storeRadius', {
		numberOfKeys: 4,
		lua: fs.readFileSync(__dirname + '/store-radius.lua', 'utf8')
	});

	redis.defineCommand('unIndex', {
		numberOfKeys: 1,
		lua: fs.readFileSync(__dirname + '/un-index.lua', 'utf8')
	});

	redis.defineCommand('storeSort', {
		numberOfKeys: 2,
		lua: fs.readFileSync(__dirname + '/store-sort.lua', 'utf8')
	});

	redis.defineCommand('reSort', {
		numberOfKeys: 3,
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
		numberOfKeys: 3,
		lua: fs.readFileSync(__dirname + '/convert-to-set.lua', 'utf8')
	});
	redis.defineCommand('ensureZSet', {
		numberOfKeys: 2,
		lua: fs.readFileSync(__dirname + '/ensure-zset.lua', 'utf8')
	});
	redis.defineCommand('getScores', {
		numberOfKeys: 3,
		lua: fs.readFileSync(__dirname + '/get-scores.lua', 'utf8')
	});
	redis.defineCommand('wildCardSet', {
		numberOfKeys: 3,
		lua: fs.readFileSync(__dirname + '/wildcard-set.lua', 'utf8')
	});
	redis.defineCommand('naturalId', {
		numberOfKeys: 2,
		lua: fs.readFileSync(__dirname + '/natural-id.lua', 'utf8')
	});
	redis.defineCommand('existingId', {
		numberOfKeys: 1,
		lua: fs.readFileSync(__dirname + '/existing-id.lua', 'utf8')
	});
	redis.defineCommand('getDocs', {
		numberOfKeys: 2,
		lua: fs.readFileSync(__dirname + '/get-docs.lua', 'utf8')
	});
	redis.defineCommand('totalFound', {
		numberOfKeys: 1,
		lua: fs.readFileSync(__dirname + '/total-found.lua', 'utf8')
	});

	redis.defineCommand('szunionstore', {
		numberOfKeys: 2,
		lua: fs.readFileSync(__dirname + '/szunionstore.lua', 'utf8')
	});
	redis.defineCommand('szinterstore', {
		numberOfKeys: 2,
		lua: fs.readFileSync(__dirname + '/szinterstore.lua', 'utf8')
	});

	redis.defineCommand('storeLexRange', {
		numberOfKeys: 3,
		lua: fs.readFileSync(__dirname + '/store-lex-range.lua', 'utf8')
	});
}