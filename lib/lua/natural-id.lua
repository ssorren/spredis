-- redis.replicate_commands()
-- redis.set_repl(redis.ALL)
local incrKey,map,avail,id = KEYS[1],KEYS[2],KEYS[3],ARGV[1]

local a = redis.call('HGET', map, id);

if a then return a end

a = redis.call('LPOP', avail)
if a then
	redis.call('HSET', map, id, a)
	return a
end
a = redis.call('INCR', incrKey)
redis.call('HSET', map, id, a)
return a

