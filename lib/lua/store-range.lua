-- redis.replicate_commands()
-- redis.set_repl(redis.REPL_NONE)
local source,store,hint,hintStore = KEYS[1],KEYS[2],KEYS[3],KEYS[4]


-- print(source,store,hint,hintStore)
-- print(hint == '')
local preHint = false;
if hint and #hint > 0 then 
	-- print('have hint')
	local hintCard = tonumber(redis.call('scard', hint)) or 0
	-- print(hintCard)
	if hintCard < 5000 then -- low cardinality hint
		preHint = true
		redis.call('zinterstore', hintStore, 2, hint, source, 'WEIGHTS', 0, 1)
		source = hintStore
	end
end

local range = redis.call('ZRANGEBYSCORE', source, ARGV[1], ARGV[2])

local len = #range

-- print(len);
for i=1,len do
	redis.call('SADD', store, range[i])
end

if hint and preHint == false then
	len = redis.call('sinterstore', hintStore, store, hint)
	redis.call('rename', hintStore, store)
end

-- if #range > 0 then
-- 	redis.call('SADD', KEYS[2], unpack(range))
-- end

return len