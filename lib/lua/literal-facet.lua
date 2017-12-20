
local set = redis.call('ZRANGE', KEYS[1], 0, -1)
local len = #set

local hashkey,key = KEYS[2],KEYS[3]
local hash,el = {},nil

for i=1,len do
	el = redis.call('HGET',hashkey,set[i])
	hash[ el ] = (hash[ el ] or 0) + 1 
end

for k,v in pairs(hash) do
	redis.call('ZADD', key, v, k)
end

if ARGV[1] == 'DESC' then
	return redis.call('ZREVRANGE', key, 0, ARGV[2], 'WITHSCORES')
end
return redis.call('ZRANGE', key, 0, ARGV[2], 'WITHSCORES')


