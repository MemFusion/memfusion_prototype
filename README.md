![](https://github.com/MemFusion/memfusion_prototype/docs/MF_logo_Memfusion.png)

#MemFusion
##An in-memory aggregation engine in the MongoDB ecosystem

MemFusion is a prototype for an in-memory DB and Aggregation Engine, binary compatible with MongoDB.
MongoDB stores data with BSON format, which is very easy to serialize and deserialize, but it has not been designed to speed up aggregations.<br>
MemFusion on the other hand converts your data into a new custom format named Z2, which has been designed for fast aggregation queries.
MemFusion always keeps your data in-memory and performs queries and aggregations in-memory.
But it can also persist your data, but only when the user requests it. In other words, persistence is a convenience feature.
If the integrity of your data is important you should also persist it with other databses (like MongoDB for example).<br>
MemFusion is currently single box. I plan to add distribution and automatic sharding if I find the time. Or the funds.<br>
MemFusion single box is free of charge. Distributed MemFusion will probably have a very affordable license.
For questions, feature requests and bugs: info@memfusion.com.

##Motivation
Last year I started studying MongoDB and wanted to write something to augment it and its ecosystem.
I realized that lots of people had performance issues.<br>
[One thread](https://groups.google.com/d/msgid/mongodb-user/f55b3e32-6bf8-4080-9f58-d8cdc0deb2af%40googlegroups.com?utm_medium=email&utm_source=footer)  striked me... 6 minutes to run a simple aggregation over 63 million documents of 1.3KB each... roughly 90GB total.
I thought that I could build an engine that could run such simple aggregations in less than a minute. So I started working on MemFusion.
The work is still not done yet, but I have a prototype I would like to share and get feedback on.

##Limitations
- Runs only on Windows for now. Porting to Linux is my #1 priority.<br>
- Although the BackEnd is fully multithreaded. The FrontEnd, for now, is single threaded: it serializes each request.<br>
- There is no decent logging.<br>
- No configuration. You are stuck with port 27017 for example. And with default Collection parameters. (Program.fs)<br>
- The query parser is not complete. Not all MongoDB operators are not supported yet.<br>
- Complex aggregations are not supported yet. Pipelines not there yet.<br>
- You cannot have more than 3GB of Binary Data for now.<br>
- Wire protocol compatible to MongoDB 2.6.<br>
- Builds only with Visual Studio 2013.<br>

##MongoDB Wire Protocol
You can use existing MongoDB tools (mongo.exe, mongoimport.exe,.. ) to talk to MemFusion.
That's because MemFusion adopts the same wire protocol used by MongoDB 2.6 and described [here](http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/)
However, as of now, it only implements a subset of it (details to be added).


##Z2 data format
MongoDB stores data with BSON format.
In BSON a document is a sequential list of names and values, where names are always strings and values can be different types. Even sub documents or lists.
Each field has variable length, and this is not ideal to search and aggregations, because you cannot predict where any field will start.
However, BSON is very easy to serialize and deserialize because of that.
MemFusion reads your data in BSON format and converts it to Z2 format, which has been designed to speed up aggregations.
In Z2, a document is a contiguous area in memory (like MongoDB, and unlike a column store).
In each document, each field takes exactly 128 bits, or 16 bytes.
So queries can just stream through memory and load each field into an SSE 16byte register, and use SSE (and AVX in the future) CPU instructions
to extract information.
Strings and binary data (which are variable length fields) that do not fit in 8 bytes have additional storage elsewhere.


##Collections and "Bins"
Each collection is divided into a number of "bins".
A bin is a big blob of contiguous memory (right now fixed at 100MB) where data is stored into.
Inserts are appends. The FrontEnd asks a given collection for some data amount and the collection
returns a memory region where the FrontEnd will actually serialize the BSON document into Z2 atoms.
If the latest bin is full or doesn't have enough room for the incoming data, a new Bin is created and future intake goes to that bin until
it is full.
Deletions are on the to-do list.


##Aggregation and Query Engine
The back end engine is a fast C++ engine optimized to run any query and aggregation on Z2 data.
Each query is broken down into a tree of simple filters, which I call "LFT".<br>
For example:<br>
db.test.find(  { $and: [ { price: { $gt: 1.99 } }, { cost: { $lt: 1.00 } } ] } )

becomes 2 LFTs, one for 'price>1.99' and one for 'cost<1.00' that run in parallel on all the bins.
Each of them, when finding one document that matches, keeps track of it.<br>
Another thread, the "Composer", as soon as each bin has been processed by both LFTs, compute the "and".

Both LFTs reading sequentially the memory at more or less the same time reduces L3 pollution.
And modern processor should be able to pick up a streaming pattern easily and cache in advance nicely.<br>
Furthermore... reading and writing data from and to the bins is lock-free.
LFTs read from the bins and write into stage1 buffers, which are lock-free as well.
So the bottleneck becomes esentially the memory bandwith for stage1.

##Benchmarks
Although preliminary, an aggregation like the one below runs at about 20% of memory bandwidth (2GB/s on my pc).<br>
    db.zips.aggregate({ $group: {_id:"$state", totalPop: {$sum: "$pop"} } } )

Simple searches with one or two filtes run at 50% of memory bandwidth (5GB/s on my pc):
    db.test.find(  { price: { $gt: 1.99 } } )
    db.test.find(  { $and: [ { price: { $gt: 1.99 } }, { cost: { $lt: 1.00 } } ] } )


## Features in to-do list
Not in my priority order. Please indicate your most favorite features.

1-  Port to linux.<br>
2-  Sharding.<br>
3-  Add configurability.<br>
4-  Add decent logging.<br>
5-  Improve support for MongoDB commands.<br>
6-  Complete the query parser.<br>
7-  Improve unit test coverage.<br>
8-  DB class should be concurrent.<br>
9-  faster aggregations: JIT the query.<br>
10- faster aggregations: use columns for most searched fields.<br>
11- Add real support for binary data.<br>
12- Add text searches.<br>


## Usage
- Download the source code from GitHub.
- Build it with VS 2013.
- Start the "Server" project.
- connect to it with mongo.exe or mongoimport.exe and try your queries.
An example:
- run this:  mongoimport.exe --db test --collection zips --file zips.json 
- the, inside the mongo.exe console:
    db.zips.aggregate({ $group: {_id:"$state", totalPop: {$sum: "$pop"} } } )


## Bugs
Please submit bugs to info@memfusion.com.

