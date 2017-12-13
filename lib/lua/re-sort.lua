-- var sortSet = resortArgs[0];
-- var allValuesKey = resortArgs[1];
-- var weightPattern = resortArgs[2] + ':*';
-- var alpha = parseInt(resortArgs[3]);
-- var valueSort;

-- var sorted; 
-- if (alpha) {
-- 	valueSort = yield this.redis.sort(this.allIdsKey, 'BY', weightPattern, 'GET', weightPattern, 'ASC', 'ALPHA', 'STORE', allValuesKey)
-- 	sorted = yield this.redis.sort(this.allIdsKey, 'BY', weightPattern, 'GET', '#', 'GET', weightPattern, 'ASC', 'ALPHA', 'STORE', 'TEMPSORT')
-- } else {
-- 	valueSort = yield this.redis.sort(this.allIdsKey, 'BY', weightPattern, 'GET', weightPattern, 'ASC', 'STORE', allValuesKey)
-- 	sorted = yield this.redis.sort(this.allIdsKey, 'BY', weightPattern, 'GET', '#', 'GET', weightPattern, 'ASC', 'STORE', 'TEMPSORT')
-- }


local sortSet = KEYS[1]
local allValuesKey = KEYS[2]
local weightPattern = KEYS[3]..':*'
local allIdsKey = KEYS[4]
local finalSort = KEYS[5]
local alpha = ARGV[1]
local valueSort,sorted
print('sorting '..allIdsKey..' by '..weightPattern..' to '..allValuesKey..' by '..alpha)
if alpha == 'ALPHA' then
	redis.call('SORT', allIdsKey, 'BY', weightPattern, 'GET', weightPattern, 'ASC', 'ALPHA', 'STORE', allValuesKey)
else 
	redis.call('SORT', allIdsKey, 'BY', weightPattern, 'GET', weightPattern, 'ASC', 'STORE', allValuesKey)
end
-- print(#sorted)
local values = redis.call('LRANGE', allValuesKey, 0, -1)
local len = #values
for i=1,len do
	if values[i] ~= '' then
		-- print('trying to set '..tostring(values[i])..' to '..tostring(i))
		redis.call('ZADD', 'XX:XX:XX:XX:TEMP:SET:XX:XX:XX:XX', 'NX', i,  values[i])
	end
end
redis.call('RENAME', 'XX:XX:XX:XX:TEMP:SET:XX:XX:XX:XX', sortSet)
if alpha == 'ALPHA' then
	redis.call('SORT', allIdsKey, 'BY', weightPattern, 'GET', '#', 'GET', weightPattern, 'ASC', 'ALPHA', 'STORE', allValuesKey)
else 
	redis.call('SORT', allIdsKey, 'BY', 'nosort', 'GET', '#', 'GET', weightPattern, 'ASC', 'STORE', allValuesKey)
end
-- for i,v in ipairs(sorted) do
-- 	-- calculate the scores ahead of time, faster to do 1 zadd per facet
-- 	-- hash[ v ] = (hash[ v ] or 0) + 1
-- 	print('not setting '..i,v)
-- end
-- for i=1,#sorted do
-- 	print(sorted[i])
-- end
values = redis.call('LRANGE', allValuesKey, 0, -1)
len = #values
for i=1,len,2 do

	-- if sorted[i + 1] then
		-- local bump = i + 1;
		-- print(values[i])
		-- print(values[i + 1])
		-- print(weightPattern)
		local val = values[i + 1];
		if val == '' then
			redis.call('ZADD', finalSort, -1, values[i])
		else
			local score = redis.call('ZSCORE', sortSet, values[i + 1])
			redis.call('ZADD', finalSort, score, values[i])
		end
		-- print('not setting '..tostring(sorted[i])..' to '..tostring(sorted[bump]))
	-- end
	-- if not v then
	-- 	print('not setting '..k,v)
	-- 	redis.call('ZADD', finalSort, -1, v)
	-- else
	-- 	print('setting '..k,v)
	-- 	redis.call('ZADD', finalSort, k, v)
	-- end
end
redis.call('del', allValuesKey)
return 1