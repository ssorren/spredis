local range = redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2])

local key = KEYS[2]
local len = #range
for i=1,len do
	redis.call('SADD', key, range[i])
end



-- local function inverse(val) 
-- 	if val == '+inf' then return '-inf' end
-- 	if val == '-inf' then return '+inf' end
-- 	return '('..tostring(val)
-- end

-- redis.call('ZINTERSTORE', key, 1, KEYS[1], 'WEIGHTS', 1);

-- if ARGV[2] == '+inf' then 
-- 	redis.call('ZREMRANGEBYSCORE', key, '-inf', inverse(ARGV[1]))
-- elseif ARGV[1] == '-inf' then
-- 	redis.call('ZREMRANGEBYSCORE', key, inverse(ARGV[2]), '+inf')
-- end


return len