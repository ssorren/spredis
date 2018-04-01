# Spredis (alpha)
source: [https://github.com/salsorrentino/spredis](https://github.com/salsorrentino/spredis)

**What is Spredis?**
Spredis is Redis based document index fetauring:

 1. Multi-threaded searching
 2. **Real-time indexing**
 3. Optimized multi-column sorting
 4. Geospatial indexing
 5. Reverse geo searches
 6. JSON queries
 7. Localized text indexing (including stop-words and stemming)
 8. Prefix/suffix searching
 9. JSON document store
 10. Date and Number indexing
 11. Literal (text) and Date/Number range facet support
 12. Embedded (Node), HTTP or TCP server modes
 13. Compound index support (pre-intersected literal and boolean fields)
 14. JSON document store (what you put in is what you get out)
 15. Document expiration
 16. Derived text fields (built from list of literal fields)
 17. Derived boolean fields
 
**What Spredis isn't** 
Spredis is not a relational/document database, and is not intended as replacement for one. Spredis works best when coupled with your current database engine (relational or document). 

**Why Spredis?**
Spredis was designed as an open-source alternative to products like Amazon's CloudSearch service and to overcome some of their drawbacks. Spredis indexing is real-time. Documents are available for searching immediately after the indexing request. Also, since spredis stores your raw JSON document, there is no need for heavy document transformation when you get the document back. You can store as many un-indexed fields as like, Spredis will just ignore them. 

**What do I need to run Spredis?** 
Spredis installations consist of 2 components:

 1. A Spredis enabled Redis server (v4.x)
 2. A Node application running the [Spredis module](https://www.npmjs.com/package/spredis).

The heart of Spredis is a Redis module written in C. It's responsible for taking in documents an indexing them according to a namespace configuration (JSON). Sitting in front of the Redis server is a Node application responsible for parsing queries and translating requests into Redis/Spredis commands. This allows you to scale components independently and ensure maximum throughput. ***Documentation on setup coming soon.***

## How fast is it?
Really fast. I need to work on some benchmarking, but in my current project I am frequently running into sub-millisecond response times (excluding network latency between client and server)

***Example configs and queries coming soon.***

## What's being worked on now
Here's the list of items that need to be completed to get out of alpha phase

- [x] Prefix/Suffix searching for text and literal fields
- [x] Smarter indexing of documents (only indexing fields that have changed)
- [ ] Automatic re-indexing after namespace config change
- [x] Implmentation of facets for non-literal (rank based) fields
- [ ] Thorough examples and documentation
- [x] Finish TCP client (spredis-client)
- [x] Finish HTTP client
- [x] Finish HTTP server mode
- [x] Multi-clause queries ('or clause' support)
- [ ] Support for Redis Sentinel

