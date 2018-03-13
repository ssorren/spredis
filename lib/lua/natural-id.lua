-- redis.replicate_commands()
-- redis.set_repl(redis.ALL)
local incrKey,map,id = KEYS[1],KEYS[2],ARGV[1]

local a = redis.call('HGET', map, id);

if a then return a end

a = redis.call('INCR', incrKey)
redis.call('HSET', map, id, a)
return a

