redis.call('ZINTERSTORE', KEYS[1], 2, KEYS[2], KEYS[3], 'WEIGHTS', 0, 1)

-- print(KEYS[2])
-- print(KEYS[3])

local res = redis.call('ZRANGE', KEYS[1], 0, 1000, 'WITHSCORES')
-- print(#res)
return res
