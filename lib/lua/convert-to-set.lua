local list,set,count = KEYS[1],KEYS[2],0
local val = redis.call('LPOP', list);

while val do
	redis.call('ZADD', set, count, val)
	count = count + 1
	val = redis.call('LPOP', list);
end

return count;
