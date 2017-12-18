local allValuesKey = KEYS[1]
local finalSort = KEYS[2]

local value = ARGV[1]
local id = ARGV[2]
local alpha = ARGV[3]


if value == '' then
	-- if no value, then it's always at the bottom of the sort
	redis.call('ZADD', finalSort, -1, id)
	return 0;
end



local currentScore = redis.call("ZSCORE", allValuesKey, value)


if currentScore ~= false then
	redis.call('ZADD', finalSort, currentScore, id)	
	return 0
end

local lex = '('..value
local prevValue,nextValue,prevScore,nextScore
local before = tonumber(redis.call('ZLEXCOUNT', allValuesKey, '-', lex))

local nextValueArray = redis.call('ZRANGEBYLEX', allValuesKey, lex, '+', 'LIMIT', 0, 1);

if before > 0 and #nextValueArray > 0 then
	prevValue = redis.call('ZRANGEBYLEX', allValuesKey, '-', lex, 'LIMIT', before - 1, 1)[1];
	nextValue = nextValueArray[1];
	prevScore = tonumber(redis.call("ZSCORE", allValuesKey, prevValue))
	nextScore = tonumber(redis.call("ZSCORE", allValuesKey, nextValue))
	currentScore = prevScore + ((nextScore - prevScore) / 2 ) -- cut the baby in 1/2
	redis.call('ZADD', allValuesKey, currentScore, value)
	redis.call('ZADD', finalSort, currentScore, id)
	return 0
elseif before == 0 and #nextValueArray == 0 then
	-- the first value, let's make it a fairly big nuber easily divisible by 2 many times
	redis.call('ZADD', allValuesKey, 131072, value)
	redis.call('ZADD', finalSort, 131072, id)
	return 0
elseif #nextValueArray == 0 then
	-- the last in the list, double the previos score
	prevValue = redis.call('zrangebylex', allValuesKey, '-', lex, 'LIMIT', before - 1, 1)[1];
	currentScore = tonumber(redis.call("ZSCORE", allValuesKey, prevValue)) + 131072;
	redis.call('ZADD', allValuesKey, currentScore, value)
	redis.call('ZADD', finalSort, currentScore, id)
	return 0;
elseif before == 0 then
	
	nextValue = nextValueArray[1];
	currentScore = tonumber(redis.call("ZSCORE", allValuesKey, nextValue)) / 2
	redis.call('ZADD', allValuesKey, currentScore, value)
	redis.call('ZADD', finalSort, currentScore, id)
	return 0
end

return 0
