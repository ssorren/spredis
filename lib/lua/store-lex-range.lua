local store,index,hint,value = KEYS[1],KEYS[2],KEYS[3],ARGV[1]

if not value or #value == 0 then return 0 end
local start,stop = value,value


if value:sub(#value) == '*' then
	value = value:sub(1,#value - 1)
	start = '['..value
	stop = '['..value..string.char(0xff)
else
	start = '['..value
	stop = '['..value
end

local len = redis.call('spredis.storerangebylex', store, index, hint, start, stop)

return len