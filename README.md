# spredis
Redis based CloudSearch Alternative

Currently in pre-alpha phase. Check back soon for more.

Will be looking for collaborators soon.

## Planned Features
* Run in embedded mode (instanciate server in your code (node only)
* Run in TCP server mode (will provide TCP client for node only)
* Run in HTTP server mode (for non-node clients)

## What's being worked on now
Here's the list of items that need to be completed to get out of pre-alpha phase

* Smarter indexing of documents (only indexing fields that have changed)
* Automatic re-indexing after namespace config change
* Implmentation of facets for non-literal (rank based) fields
* Thorough examples and documentation
* Finish TCP client (spredis-client)
* Finish HTTP client
* Finish HTTP server mode
* Multi-clause queries ('or clause' support)
