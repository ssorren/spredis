# spredis
Redis based CloudSearch Alternative

Currently in pre-alpha phase. Check back soon for more.

Will be looking for collaborators soon.

## Initial Testing Results
Inital testing is extremely promising. The test database we're using has about 250k (~744MB of JSON data) complex documents based on a real-world production use case. The memory foortprint in Redis is ~1.3GB (index and document data). Scaling up to millions of documents should just be a function of memory. The test queries include a combination of **free text, literal values, geo radius  and number/date ranges, multi-column sorting and faceted results and distance calculation**. The redis setup for the follwing tests is a single redis node runnning v4.

Here are the results we're curently seeing:
Running 100 queries concurrently (single client)...
```
Ran 100 queries in 162ms (TCP)
  Average query prep time:            0.45ms (1 max)
  Average query exec time:            2.75ms (9 max)
  Average respone serialization time: 1.04ms (3 max)
  Overhead cost:                      -262ms (-2.62ms/query)
  Speed (incl. overhead):             617 queries/sec (1.62ms/query)
  Found (incl. overhead):             4375 records (27006/sec)
  Returned (incl. overhead):          1825 records (11265/sec)
```

Running 1,000 queries consecutively (single client)...
```
Ran 1000 queries in 4829ms (TCP)
  Average query prep time:            0.476ms (3 max)
  Average query exec time:            1.712ms (6 max)
  Average respone serialization time: 0.79ms (4 max)
  Overhead cost:                      1851ms (1.851ms/query)
  Speed (incl. overhead):             207 queries/sec (4.829ms/query)
  Found (incl. overhead):             43750 records (9060/sec)
  Returned (incl. overhead):          18250 records (3779/sec)
```
> query prep time = query parsing and index planning
> 
> query exec time = time to run query (redis)
> 
> respone serialization time = serializing json document and assigning distance expression values
> 
> Overhead cost = network travel time + client code execution time (when running concurrent queries this number may be negative)
	
## Planned Server Features
* Run in embedded mode (instanciate server in your code (node only)
* Run in TCP server mode (will provide TCP client for node only)
* Run in HTTP server mode (for non-node clients)

## What's being worked on now
Here's the list of items that need to be completed to get out of pre-alpha phase

- [x] Prefix/Suffix searching for text and literal fields
- [x] Smarter indexing of documents (only indexing fields that have changed)
- [ ] Automatic re-indexing after namespace config change
- [ ] Implmentation of facets for non-literal (rank based) fields
- [ ] Thorough examples and documentation
- [x] Finish TCP client (spredis-client)
- [x] Finish HTTP client
- [x] Finish HTTP server mode
- [x] Multi-clause queries ('or clause' support)
- [ ] Support for Redis Sentinel

