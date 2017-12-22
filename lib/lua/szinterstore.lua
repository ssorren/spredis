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
	return redis.call('ZINTERSTORE', key, count, unpack(ARGV))
end
return redis.call('SINTERSTORE', key, unpack(setArgs))