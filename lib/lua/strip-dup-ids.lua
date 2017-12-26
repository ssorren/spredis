-- redis.replicate_commands()
-- redis.set_repl(redis.REPL_NONE)
local dest = KEYS[1]
local source = KEYS[2]

local range = redis.call('ZRANGE', source, 0, -1, 'WITHSCORES');
local len = #range;
print(dest, source)
print(len)
for i=1,len,2 do
	print(string.gsub(range[i], ';.+', ''));
	redis.call('ZADD', dest, range[i + 1], string.gsub(range[i], ';.+', '')) 	
end 

return len;
