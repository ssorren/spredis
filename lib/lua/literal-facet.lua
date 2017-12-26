-- redis.replicate_commands()
-- redis.set_repl(redis.REPL_NONE)
local t = redis.call('TYPE', KEYS[1])
t = t.ok or t
-- print(t)
local set = nil;
if t == 'zset' then 
	set = redis.call('ZRANGE', KEYS[1], 0, -1)
else
	set = redis.call('SMEMBERS', KEYS[1])
end

if true then return {} end 

local len = #set
-- print(len)
local hashkey,key = KEYS[2],KEYS[3]
local hash,el = {},nil
-- print(hashkey)
-- print(key)
for i=1,len do
	el = redis.call('HMGET',hashkey,set[i])
	-- print(el)
	if (el) then
		hash[ el ] = (hash[ el ] or 0) + 1 
	-- else 
	-- 	print(el)
	-- 	print(set[i])
	end
	-- print(hash[ el ])
end

for name,v in pairs(hash) do
	-- print(name)
	-- print(v)
	redis.call('ZADD', key, v, name)
end

if ARGV[1] == 'DESC' then
	return redis.call('ZREVRANGE', key, 0, ARGV[2], 'WITHSCORES')
end
return redis.call('ZRANGE', key, 0, ARGV[2], 'WITHSCORES')


