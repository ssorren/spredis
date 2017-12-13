-- local sorted = redis.call('SORT', KEYS[1], "BY", KEYS[2], 'GET', KEYS[2], 'ALPHA')
-- for some reaason it seems to be faster to store the ist then retrieve it rather than get the sort results directly 

-- redis.call('SORT', KEYS[1], "BY", 'nosort' , 'LIMIT', 0, 10000, 'GET', KEYS[2]..'*', 'STORE', KEYS[4])
redis.call('SORT', KEYS[1], "BY", 'nosort', 'GET', KEYS[2]..'*', 'STORE', KEYS[4])

-- print(KEYS[2])
-- if true then return {} end
-- local len = redis.call('LLEN', KEYS[4])
-- print(len)
-- if true then return {} end

-- local items = redis.call('ZSCAN', KEYS[1], 0, 'COUNT', 10000)[2]

local key1 = KEYS[4]
local key = KEYS[3]
-- local getKey = KEYS[2]
local hash = {};

local el = redis.call('RPOP', key1);
while el do
	hash[ el ] = (hash[ el ] or 0) + 1 
	el = redis.call('RPOP', key1)
end

-- local len = #items
-- -- print(len)
-- for i=1,len,2 do
-- 	-- print(getKey..items[i]);
-- 	el = redis.call('GET', getKey..items[i])
-- 	-- print(el)
-- 	hash[ el ] = (hash[ el ] or 0) + 1
-- end
-- local sorted = redis.call('LRANGE', KEYS[4], 0, -1)




-- local key = KEYS[3]



-- local len = #sorted
-- local v
-- for i=1,len do
-- 	v = sorted[i]
--  	hash[ v ] = (hash[ v ] or 0) + 1
-- end 
-- for i,v in ipairs(sorted) do
-- 	-- calculate the scores ahead of time, faster to do 1 zadd per facet
-- 	hash[ v ] = (hash[ v ] or 0) + 1
-- end

for k,v in pairs(hash) do
	redis.call('ZADD', key, v, k)
end

if ARGV[1] == 'DESC' then
	return redis.call('ZREVRANGE', key, 0, ARGV[2], 'WITHSCORES')
end
return redis.call('ZRANGE', key, 0, ARGV[2], 'WITHSCORES')


