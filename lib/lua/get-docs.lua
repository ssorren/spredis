local tosort,hash,start,stop,setstore,res = KEYS[1],KEYS[2],(ARGV[1] or 0),(ARGV[2] or -1),ARGV[3],{}
local tempStore = '__XX:SPREDIS:DOC:TEMP:XX__';



redis.call('SORT', tosort, 'BY', 'nosort', 'GET', '#', 'LIMIT', start, stop, 'STORE', tempStore)

local val = redis.call('LPOP', tempStore);
while val do
	if setstore then 
		redis.call('SADD', setstore, val)
	end
	table.insert(res, redis.call('HGET', hash, val))
	val = redis.call('LPOP', tempStore);
end

redis.call('DEL', tempStore);

return res;



