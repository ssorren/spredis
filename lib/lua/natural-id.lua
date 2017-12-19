local incr,map,avail,id = KEYS[1],KEYS[2],KEYS[3],ARGV[1]

local score = redis.call('ZSCORE', map, id);
if score then return score end

local a = redis.call('LPOP', avail)


if a then
	redis.call('ZADD', map, a, id)
	return a
end

a = redis.call('INCR', incr)

redis.call('ZADD', map, a, id)
return a

