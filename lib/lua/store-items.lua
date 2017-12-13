redis.call('SORT', KEYS[2], 'BY', 'nosort','GET', KEYS[1]..'*', 'LIMIT', ARGV[1], ARGV[2], 'STORE', KEYS[3]);
return redis.call('LLEN', KEYS[3]);

