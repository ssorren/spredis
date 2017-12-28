-- redis.replicate_commands()
-- redis.set_repl(redis.REPL_NONE)
local list,set,tempPrefix,item = KEYS[1],KEYS[2],KEYS[3],1
-- redis.call('SADD', KEYS[2], unpack(redis.call('LRANGE', KEYS[1], 0, -1)))
-- print(list)
local t = redis.call('TYPE',list)
t = t.ok or t
-- print(t)

if t == 'list' then
	repeat
	    items = redis.call('lpop', list)
	    if (item) then redis.call('sadd', set, item) end
	until not item

elseif t == 'zset' then
	local res = redis.call('ZRANGE', list, 0, -1)
	local len = #res
	for i=1,len do
		redis.call('SADD', set, res[i])
	end
elseif t == 'set' then
	if string.sub(list,1,string.len(tempPrefix))==tempPrefix then
		redis.call('RENAME', list, set)
	else
		redis.call('SINTERSTORE', set, list);
	end
end
return 1
