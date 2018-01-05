
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

-- local range = redis.call('zrangebyscore', source, ARGV[1], ARGV[2])
-- local len = #range

-- for i=1,len do
-- 	redis.call('zadd', store, i, range[i])
-- end

local len = redis.call('spredis.storerangebyscore', store, source, ARGV[1], ARGV[2])



if hint and #hint > 0 and preHint == false then
	len = redis.call('zinterstore', hintStore, 2, store, hint)
	redis.call('rename', hintStore, store)
end

-- if #range > 0 then
-- 	redis.call('SADD', KEYS[2], unpack(range))
-- end

return len