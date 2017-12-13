local pattern = KEYS[1];



print(pattern)
local res = redis.call('SCAN', 0, 'MATCH', pattern, 'COUNT', 10000)

local cursor = tonumber(res[1]);
local results = res[2];
print(cursor)
-- print(#results)
while cursor > 0 do
	res = redis.call('SCAN', cursor, 'MATCH', pattern, 'COUNT', 10000)
	cursor = tonumber(res[1])
	results = res[2]
	print(cursor)
	print(#results)
end

return results;
