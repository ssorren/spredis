local map,id = KEYS[1],ARGV[1]

return redis.call('ZSCORE', map, id);

