-- redis.replicate_commands()
-- redis.set_repl(redis.REPL_NONE)
local key,count,hasZSet,t,setArgs = KEYS[1],tonumber(KEYS[2]),false,nil,{}

for i=1,count do
	t = redis.call('TYPE', ARGV[i])
	t = t.ok or t
	table.insert(setArgs, ARGV[i]) -- need to ignore weights
	if t == 'zset' then
		hasZSet = true
		break
	end
end

if hasZSet then
	return redis.call('ZUNIONSTORE', key, count, unpack(ARGV))
end
return redis.call('SUNIONSTORE', key, unpack(setArgs))
