
local source,store,hint,hintStore = KEYS[1],KEYS[2],KEYS[3],KEYS[4]


local preHint = false;
if hint and #hint > 0 then 
	local hintCard = tonumber(redis.call('zcard', hint)) or 0
	local sourceCard = tonumber(redis.call('zcard', source)) or 0
	if hintCard < (sourceCard / 2) then -- low cardinality hint
		preHint = true
		redis.call('zinterstore', hintStore, 2, hint, source, 'weights', 0, 1)
		source = hintStore
	end
end
-- pipe.georadius(indexName, query.from[1], query.from[0], query.radius, query.unit, 'STORE', store);
-- local range = 
redis.call('georadius', source, ARGV[1], ARGV[2], ARGV[3], ARGV[4], 'store', store)

-- local len = #range

-- for i=1,len do
-- 	redis.call('sadd', store, range[i])
-- end

if hint and #hint > 0 and preHint == false then
	len = redis.call('zinterstore', hintStore, 2, store, hint)
	redis.call('rename', hintStore, store)
end

-- if #range > 0 then
-- 	redis.call('SADD', KEYS[2], unpack(range))
-- end

return len