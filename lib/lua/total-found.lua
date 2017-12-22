local t = redis.call('TYPE', KEYS[1])
t = t.ok or t
if t == 'zset' then return redis.call('ZCARD', KEYS[1]) end
if t == 'set' then return redis.call('SCARD', KEYS[1]) end
if t == 'list' then return redis.call('LLEN', KEYS[1]) end
return 0