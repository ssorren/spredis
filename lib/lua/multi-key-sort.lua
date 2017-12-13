-- note: this only works is each column value has a fixed string length. 
-- also, the id must be the last column sent, 
-- numKeys must include the id field in the count
-- we're relying on the main application to take care of that

-- may need this function later, for distance/score sorting
-- will need to pad the result to 26 character, 
-- this is the length needed to represent 1/2 the circumference of the earth in feet (the smallest unit we can sort by)

-- local function numberToBinStr(x)
-- 	x = math.floor(tonumber(x) + 0.5) -- round floats
-- 	ret=""
-- 	while x~=1 and x~=0 do
-- 		ret=tostring(x%2)..ret
-- 		x=math.modf(x/2)
-- 	end
-- 	ret=tostring(x)..ret
-- 	return ret
-- end


local numKeys,source,dest,offset,count = tonumber(ARGV[1]),KEYS[1],KEYS[2],ARGV[2],ARGV[3]
-- print(tostring(redis.call('TIME')[2]))
-- local startTime = redis.call('TIME')
-- print(startTime)

local keys = redis.call('LLEN', source)
local len,incr = tonumber(keys),numKeys

-- print(len)
-- print(numKeys)

local sortStrOffset = 0;
for i=1,len,incr do
	local s = '';
	for k=1,numKeys do
		if i < numKeys then
			-- we're going to do a substring to pull off the id later, faster than regex,
			-- we only need to do this on the first row. that's why we're using i instagead of k
			sortStrOffset = #s + 1
			-- i hate non-zero based indexed languages
		end
		s = s..redis.call('LPOP', source)
	end
	redis.call('RPUSH', dest, s)
end

redis.call('SORT', dest, 'LIMIT', offset, count, 'ASC', 'ALPHA','STORE', dest)

len = tonumber(redis.call('LLEN', dest))
for i=1,len do
	-- just cycle through the list, pull from the front and put the result on the tail end
	-- we'll be left with nothing but the ids themselves
	local a = redis.call('LPOP', dest)
	redis.call('RPUSH', dest, string.sub(a, sortStrOffset))
end
-- local endTime = redis.call('TIME')
-- print(endTime)
-- local sortTime = endTime - startTime
-- print(sortTime)
return len











