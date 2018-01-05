-- redis.replicate_commands()
-- redis.set_repl(redis.ALL)
local allValuesKey = KEYS[1]
local finalSort = KEYS[2]

local value = ARGV[1]
local id = ARGV[2]
local alpha = ARGV[3]

local sortIncr,reSortIncr = 131072,8

if value == '' then
	-- if no value, then it's always at the bottom of the sort
	-- redis.call('ZADD', finalSort, -1, id)
	redis.call('spredis.dhashset', finalSort, id, '-inf')
	return 0;
end



local currentScore = redis.call("ZSCORE", allValuesKey, value)


if currentScore ~= false then
	redis.call('spredis.dhashset', finalSort, id, currentScore)
	-- redis.call('ZADD', finalSort, currentScore, id)	
	return currentScore
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
	redis.call('spredis.dhashset', finalSort, id, currentScore)
	
elseif before == 0 and #nextValueArray == 0 then
	-- the first value, let's make it a fairly big nuber easily divisible by 2 many times
	redis.call('ZADD', allValuesKey, sortIncr, value)
	redis.call('spredis.dhashset', finalSort, id, sortIncr)
	-- redis.call('ZADD', finalSort, sortIncr, id)
	
elseif #nextValueArray == 0 then
	-- the last in the list, double the previos score
	prevValue = redis.call('zrangebylex', allValuesKey, '-', lex, 'LIMIT', before - 1, 1)[1];
	currentScore = tonumber(redis.call("ZSCORE", allValuesKey, prevValue)) + sortIncr;
	redis.call('ZADD', allValuesKey, currentScore, value)
	redis.call('spredis.dhashset', finalSort, id, currentScore)
	-- redis.call('ZADD', finalSort, currentScore, id)
	
elseif before == 0 then
	
	nextValue = nextValueArray[1];
	currentScore = tonumber(redis.call("ZSCORE", allValuesKey, nextValue)) / 2
	redis.call('ZADD', allValuesKey, currentScore, value)
	redis.call('spredis.dhashset', finalSort, id, currentScore)
	-- redis.call('ZADD', finalSort, currentScore, id)
	
end
if (currentScore % 1) ~= 0 then
	-- print('fudging:'..allValuesKey..':'..tostring(currentScore))
end
return currentScore
