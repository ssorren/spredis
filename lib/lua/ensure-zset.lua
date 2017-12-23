
local source,temp = KEYS[1],KEYS[2]
local t = redis.call('TYPE',source)
t = t.ok or t
-- print(t)
if t == 'zset' then return source end
-- print(temp,source)
redis.call('ZINTERSTORE', temp, 1, source);
-- print(redis.call('ZCARD', temp))
return temp
