local tosort,hash,start,stop,res = KEYS[1],KEYS[2],(ARGV[1] or 0),(ARGV[2] or -1),{}


local ids = redis.call('spredis.getresids', tosort, start, stop);

local len = #ids;
for i=1,len do
	table.insert(res, redis.call('HGET', hash, ids[i]))
	-- if setstore then 
	-- 	redis.call('SADD', setstore, val)
	-- end	
end

return res;



