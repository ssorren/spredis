-- redis.replicate_commands()
-- redis.set_repl(redis.ALL)
local list,set,src,score = KEYS[1],KEYS[2],KEYS[3],0

local val = redis.call('LPOP', list);

while val do
	score = redis.call('ZSCORE', src, val);
	redis.call('ZADD', set, score, val);
	-- result[val] = redis.call('ZSCORE',set, val);
	-- redis.call('ZADD', set, 0, val)
	-- count = count + 1
	val = redis.call('LPOP', list);
end

return 1;
