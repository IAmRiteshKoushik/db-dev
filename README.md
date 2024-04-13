# Database Development

## Preliminary Notes

### 1. Persistence
A database will recover to a usable state when started after an unexpected
shutdown. While we can achieve it without a database as follows:

- Write the whole updated dataset to a file
- Call `fsync` on the new file
- Overwrite the old file by renaming the new file to the old file, which is 
guarenteed by the file-systems to be atomic.

But this is only acceptable with a tiny dataset. Databases can do incremental 
updates.

### 2. Indexing
There are two distinct types of database queries: analytical (OLAP) and 
transactional (OLTP).

- Analytical (OLAP) queries typically involve a large amount of data, with 
aggregation, grouping or join operations
- In contrast, transactional (OLTP) queries usually only touch a small amount 
of indexed data. The most common types of queres are indexed point queries 
and indexed range queries.

> Data structures that persist on disk to `look up` data are called `indexes`
in database systems. And database indexes can be larger than memory. 

Common data structures include `B-Trees` and `LSM-Trees`

### 3. Concurrency
Modern applications do not do everything sequentially, and nor do databases.
There are different levels to concurrency :

- Concurrency between readers
- Concurrency between readers and writers
- Do writers need exclusive access to the database ?
