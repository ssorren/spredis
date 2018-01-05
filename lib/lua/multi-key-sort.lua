-- local cCommand = {'spredis.sort', source, dest}
-- for i=1,numKeys do
--     table.insert(cCommand, ARGV[i])
--     table.insert(cCommand, ARGV[i + numKeys])
-- end

-- print(table.concat(KEYS, ', '))
-- print(table.concat(ARGV, ', '))
return redis.call('spredis.sort', KEYS[1], KEYS[2], KEYS[3], KEYS[4], unpack(ARGV))
-- print(res.err)
-- print(redis.call('llen', KEYS[2]))











