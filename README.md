# spredis
Redis based CloudSearch Alternative

Currently in pre-alpha phase. Check back soon for more.

Will be looking for collaborators soon.

## Initial Testing Results
Inital testing is extremely promising. The test database we're using has about 250k complex documents based on a real-world production use case. The memory foortprint in Redis is about 2GB (index and document data). Scaling up to millions of documents should just be a function of memory. The test queries include a combination of **free text, literal values, geo radius  and number/date ranges, multi-column sorting and faceted results and distance calculation**. The redis setup for the follwing tests is a single redis node runnning v4.

Here are the results we're curently seeing:

Running 100 queries concurrently...

	Ran 100 queries in 183ms (TCP)
  		Average query prep time:            0.89ms (5 max)
  		Average query exec time:            3.3ms (10 max)
  		Average respone serialization time: 0.91ms (4 max)
  		Overhead cost:                      -327ms (-3.27ms/query)
  		Speed (incl. overhead):             546 queries/sec (1.83ms/query)
  		Found (incl. overhead):             4300 records (23497/sec)
  		Returned (incl. overhead):          1825 records (9973/sec)
  		
  		

Running 1000 queries consecutively (single client)...

	Ran 1000 queries in 4901ms (TCP)
  		Average query prep time:            0.681ms (9 max)
  		Average query exec time:            1.731ms (7 max)
  		Average respone serialization time: 0.713ms (5 max)
  		Overhead cost:                      1776ms (1.776ms/query)
  		Speed (incl. overhead):             204 queries/sec (4.901ms/query)
  		Found (incl. overhead):             43000 records (8774/sec)
  		Returned (incl. overhead):          18250 records (3724/sec)

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

* ~~Prefix/Suffix searching for text and literal fields~~
* ~~Smarter indexing of documents (only indexing fields that have changed)~~
* Automatic re-indexing after namespace config change
* Implmentation of facets for non-literal (rank based) fields
* Thorough examples and documentation
* Finish TCP client (spredis-client)
* Finish HTTP client
* Finish HTTP server mode
* Multi-clause queries ('or clause' support)
* Support for Redis Sentinel
