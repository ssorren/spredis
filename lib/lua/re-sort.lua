-- redis.replicate_commands()
-- redis.set_repl(redis.ALL)
-- This code is still valid, but it's been moved into the app server
-- It's better to do this in small chunks, let the IO between node and redis throttle
-- the commands to we don't block redis for a long time
-- leaving this here in case we absolutly positively have to force a re-sort right away

local values,finalSort,tempSort = KEYS[1],KEYS[2],KEYS[3]

redis.call('sort', values, 'BY', '#', 'store', tempSort);
local res = redis.call('lrange', tempSort, 0, 99) -- get 100 items
local len = #res
local count,value,command = 0,nil,{};
while len > 0 do
	command = {}
	for i=1,len do
		count = count + 1
		value = string.match(res[i], ':(.+)')
		if res[i] then
			table.insert(command, count)
			table.insert(command, value)
		end
	end
	if #command > 0 then
		redis.call('zadd', finalSort, 'XX', unpack(command)) -- 'XX' only update values, the existing id may have been removed, probably not needed, but let's be sure
	end
	redis.call('ltrim', tempSort, 100, -1) -- remove the items we just processed
	res = redis.call('lrange', tempSort, 0, 99)
	len = #res
end

return 0