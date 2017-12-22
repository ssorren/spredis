local source,facets,flen,facetCount = KEYS[1],{},#ARGV,#ARGV/5
local t = redis.call('TYPE', source)
t = t.ok or t
-- print(t)
local set = nil;
if t == 'zset' then 
	set = redis.call('ZRANGE', source, 0, -1)
else
	set = redis.call('SMEMBERS', source)
end

-- local facetKeys = {}
for i=1,flen,5 do
	local facet = {}
	facet.field = ARGV[i]
	facet.count = ARGV[i + 1]
	facet.store = ARGV[i + 2]
	facet.order = ARGV[i + 3]
	facet.key = ARGV[i + 4]
	facet.results = {}
	table.insert(facets, facet)
end
-- print(cjson.encode(facets))

local len = #set

local el = nil
local facet = {}
for i=1,len do

	for k=1,facetCount do
		facet = facets[k]
		el = redis.call('HGET', facet.key, set[i])
		if el then
			facet.results[el] = (facet.results[el] or 0) + 1
		end
	end

end

for k=1,facetCount do
	facet = facets[k]
	-- el = redis.call('HGET', facet.key, set[i])
	for name,v in pairs(facet.results) do
		redis.call('ZADD', facet.store, v, name)
	end
end
-- print(cjson.encode(facets))
-- if true then return {} end

-- for name,v in pairs(hash) do
-- 	-- print(name)
-- 	-- print(v)
-- 	redis.call('ZADD', key, v, name)
-- end
-- if true then return facets end
local results = {}
local zrange = 'ZRANGE'
for k=1,facetCount do
	facet = facets[k];
	zrange = 'ZRANGE'
	if facet.order == 'DESC' then
		zrange = 'ZREVRANGE'
	end
	table.insert(results, redis.call(zrange, facet.store, 0, facet.count, 'WITHSCORES'))
end
-- print(#results)
return results

