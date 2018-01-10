
-- local function printType(k)
-- 	local t = redis.call('TYPE', k)
-- 	print(t.ok or t)
-- end

local lexSet,store,keyPattern,val,suffix,hint = KEYS[1],KEYS[2],KEYS[3],ARGV[1],tonumber(ARGV[2]),ARGV[3]

-- printType(hint)

local min,max = '['..val,'['..val..'\xff'
local res = redis.call('ZRANGEBYLEX', lexSet, min, max)

-- local command,weights,len = {'ZUNIONSTORE', store, #res},{'WEIGHTS'},#res


-- local command,len = {'ZUNIONSTORE', store, #res},#res

local command,len = {'spredis.stunionstore', store},#res
local sets,interstores = {},{}

for i=1,len do
	local s = res[i]
	if suffix == 1 then s = string.reverse(s) end
	table.insert(sets, keyPattern..s)
	-- table.insert(weights, 0)
end
-- print(table.concat(sets, ', '))

if #hint > 0 then
	-- print('have hint')
	for i=1,len do
		local iname = '_XX:SPREDIS:TEMP:INTER:'..tostring(i)
		local count = redis.pcall('spredis.stinterstore', iname, hint, sets[i])

		-- print(count)
		table.insert(interstores, iname)
	end
	sets = interstores
end


for i,v in ipairs(sets) do
	table.insert(command, v)
end

-- print('command:')
-- print(table.concat(command, ', '))
-- for i,v in ipairs(weights) do
-- 	table.insert(command, v)
-- end

local count = redis.pcall(unpack(command))
-- print(count)
if #intestores then
	redis.call('DEL', unpack(interstores))
end

return count;