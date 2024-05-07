# What do we want from a database
- reliability
- storage
- transactions
- indexes

We focus on indexes

# do I talk about balanced tree?

# Hash table
- Recall memory concept
- hash concept. Cryptographic hash vs fast hash
- how do we store on disk
- buckets in pages
- directory of pages

# DBM format
- UNIXv6(?)
- simple hash
- fixed page size & directory in a separate file
- page storage description
- page directory. flattened binary tree trick
- splitting events

# Problems
- can you identify problems with this scheme?
- maximum bucket size
- tokens that hash together must fit on the same page
- other problems?
- concurrency
…
