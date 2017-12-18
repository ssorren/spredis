local lexSet,store,keyPattern,val,suffix,hint = KEYS[1],KEYS[2],KEYS[3],ARGV[1],tonumber(ARGV[2]),ARGV[3]

local min,max = '['..val,'['..val..'\xff'
local res = redis.call('ZRANGEBYLEX', lexSet, min, max)

local command,weights,len = {'ZUNIONSTORE', store, #res},{'WEIGHTS'},#res
local sets,interstores = {},{}

for i=1,len do
	local s = res[i]
	if suffix == 1 then s = string.reverse(s) end
	table.insert(sets, keyPattern..s)
	table.insert(weights, 0)
end

if hint then
	for i=1,len do
		local iname = '_SPREDIS:TEMP:INTER:'..tostring(i)
		local count = redis.call('ZINTERSTORE', iname, 2, hint, sets[i], 'WEIGHTS', 0, 0)
		table.insert(interstores, iname)
	end
	sets = interstores
end

for i,v in ipairs(sets) do
	table.insert(command, v)
end


for i,v in ipairs(weights) do
	table.insert(command, v)
end

local count = redis.call(unpack(command))

if #intestores then
	redis.call('DEL', unpack(interstores))
end

return count;

