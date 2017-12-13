local numKeys,source,dest = tonumber(KEYS[3]),KEYS[1],KEYS[2]
local columns = {}
local orders = {}
-- print(ARGV[1])
-- print(KEYS[1])
-- print(source)
-- print(dest)

for i=1,numKeys do
    table.insert(columns, ARGV[i])
end

for i=1,numKeys do
    table.insert(orders, ARGV[i + numKeys])
end

-- print(1)
local current_multiplier = 1
local current_divisor = 1
-- print(1.1)
local command = {'ZINTERSTORE', dest, tostring(numKeys + 1)}
-- print(command)
local colCommand = {source}
local weightCommand = {'WEIGHTS', 0}
-- print(2)
-- print(table.concat(command, ' '))
-- print(table.concat(columns, ' '))
-- print(table.concat(orders, ' '))
for i,column in ipairs(columns) do
    local order = orders[i]
    -- print(table.concat(redis.call('ZRANGE', column, 0, 0, 'WITHSCORES'), ',' ))
    -- print(redis.call('ZRANGE', column, 0, 0, 'WITHSCORES')[1])
    local low = math.abs(redis.call('ZRANGE', column, 0, 0, 'WITHSCORES')[2]);
    -- print(low)
    local high = math.abs(redis.call('ZRANGE', column, -1, -1, 'WITHSCORES')[2]);
    table.insert(colCommand, column)
    if order == 'ASC' then 
        table.insert(weightCommand, current_multiplier)
    else
        table.insert(weightCommand, '-'..current_multiplier)
    end
    local maxv = tostring(math.ceil( math.max(low, high) ))

    local s = '0.'
    current_divisor = current_divisor + #maxv
    for i=1,current_divisor - 1 do
        s = s..'0'
    end
    s = s..'1'
    current_multiplier = s;
    
end
for i,v in ipairs(colCommand) do
    table.insert(command,v)
end
for i,v in ipairs(weightCommand) do
    table.insert(command,v)
end

return redis.call(unpack(command));
-- print(table.concat(command, ' '));
