-- redis.replicate_commands()
local map,id = KEYS[1],ARGV[1]

return redis.call('HGET', map, id);

