local tosort,hash,start,stop,setstore,res = KEYS[1],KEYS[2],(ARGV[1] or 0),(ARGV[2] or -1),ARGV[3],{}


local ids = redis.call('spredis.getresids', tosort, start, stop);

local len,val = #ids,nil;
for i=1,len do
	val = ids[i]
	table.insert(res, redis.call('HGET', hash, val))
	if setstore then 
		redis.call('SADD', setstore, val)
	end	
end

return res;



