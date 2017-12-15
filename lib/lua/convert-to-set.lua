
local list,set,item = KEYS[1],KEYS[2],1
-- redis.call('SADD', KEYS[2], unpack(redis.call('LRANGE', KEYS[1], 0, -1)))
repeat
    items = redis.call('lpop', list)
    if (item) then redis.call('sadd', set, item) end
until not item

-- local val = redis.call('LPOP', list);
-- while val do
	
-- 	-- count = count + 1
-- 	redis.call('SADD', set, val)
-- 	val = redis.call('LPOP', list);
-- end

return 1
