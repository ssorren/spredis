-- redis.replicate_commands()
-- redis.set_repl(redis.REPL_NONE)
-- local index,key,value,pattern = KEYS[1],KEYS[2],ARGV[1],';(.+)'
local index,key,value = KEYS[1],KEYS[2],ARGV[1]
-- print(1)
-- print(value)

if not value or #value == 0 then return 0 end
local start,stop = value,value
-- print(start)
if value:sub(#value) == '*' then
	value = value:sub(1,#value - 1)
	start = '['..value
	stop = '['..value..'\\xff'
else
	start = '['..value..':'
	stop = '['..value..':\\xff'
end
-- print(start)
-- -- print(stop)
-- print(key, index, start, stop)

local len = redis.call('spredis.storerangebylex', key, index, start, stop)
-- print(len)
-- print(redis.call('zcard', key));
-- local range = redis.call('zrangebylex', index, start, stop)
-- local len,v = #range,nil
-- for i=1,len do
-- 	v = string.match(range[i], pattern)
-- 	if v then
-- 		redis.call('zadd', key, i, v)
-- 	end
-- end

return len