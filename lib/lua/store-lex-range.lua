local index,key,value = KEYS[1],KEYS[2],ARGV[1]
-- print(1)
-- print(value)
if not value or #value == 0 then return 0 end
local start,stop = value,value
-- print(start)
if value:sub(#value) == '*' then
	value = value:sub(1,#value - 1)
	start = '['..value
	stop = '['..value..'\xff'
else
	start = '['..value..':'
	stop = '['..value..':\xff'
end
-- print(start)
-- print(stop)
local range = redis.call('ZRANGEBYLEX', index, start, stop)
-- print(#range)
local len,v = #range,nil
for i=1,len do
	v = string.match(range[i], ':(.+)')
	if v then
		redis.call('SADD', key, v)
	end
end

return len