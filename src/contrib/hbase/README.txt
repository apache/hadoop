HBASE
Michael Cafarella


This document gives a quick overview of HBase, the Hadoop simple
database. It is extremely similar to Google's BigTable, with a just a
few differences. If you understand BigTable, great. If not, you should
still be able to understand this document.

---------------------------------------------------------------
I.

HBase uses a data model very similar to that of BigTable. Users store
data rows in labelled tables. A data row has a sortable key and an
arbitrary number of columns. The table is stored sparsely, so that
rows in the same table can have crazily-varying columns, if the user
likes.

A column name has the form "<group>:<label>" where <group> and <label>
can be any string you like. A single table enforces its set of
<group>s (called "column groups"). You can only adjust this set of
groups by performing administrative operations on the table. However,
you can use new <label> strings at any write without preannouncing
it. HBase stores column groups physically close on disk. So the items
in a given column group should have roughly the same write/read
behavior.

Writes are row-locked only. You cannot lock multiple rows at once. All
row-writes are atomic by default.

All updates to the database have an associated timestamp. The HBase
will store a configurable number of versions of a given cell. Clients
can get data by asking for the "most recent value as of a certain
time". Or, clients can fetch all available versions at once.

---------------------------------------------------------------
II.

To the user, a table seems like a list of data tuples, sorted by row
key. Physically, tables are broken into HRegions. An HRegion is
identified by its tablename plus a start/end-key pair. A given HRegion
with keys <start> and <end> will store all the rows from (<start>,
<end>]. A set of HRegions, sorted appropriately, forms an entire
table.

All data is physically stored using Hadoop's DFS. Data is served to
clients by a set of HRegionServers, usually one per machine. A given
HRegion is served by only one HRegionServer at a time.

When a client wants to make updates, it contacts the relevant
HRegionServer and commits the update to an HRegion. Upon commit, the
data is added to the HRegion's HMemcache and to the HRegionServer's
HLog. The HMemcache is a memory buffer that stores and serves the
most-recent updates. The HLog is an on-disk log file that tracks all
updates. The commit() call will not return to the client until the
update has been written to the HLog.

When serving data, the HRegion will first check its HMemcache. If not
available, it will then check its on-disk HStores. There is an HStore
for each column family in an HRegion. An HStore might consist of
multiple on-disk HStoreFiles. Each HStoreFile is a B-Tree-like
structure that allow for relatively fast access.

Periodically, we invoke HRegion.flushcache() to write the contents of
the HMemcache to an on-disk HStore's files. This adds a new HStoreFile
to each HStore. The HMemcache is then emptied, and we write a special
token to the HLog, indicating the HMemcache has been flushed.

On startup, each HRegion checks to see if there have been any writes
to the HLog since the most-recent invocation of flushcache(). If not,
then all relevant HRegion data is reflected in the on-disk HStores. If
yes, the HRegion reconstructs the updates from the HLog, writes them
to the HMemcache, and then calls flushcache(). Finally, it deletes the
HLog and is now available for serving data.

Thus, calling flushcache() infrequently will be less work, but
HMemcache will consume more memory and the HLog will take a longer
time to reconstruct upon restart. If flushcache() is called
frequently, the HMemcache will take less memory, and the HLog will be
faster to reconstruct, but each flushcache() call imposes some
overhead.

The HLog is periodically rolled, so it consists of multiple
time-sorted files. Whenever we roll the HLog, the HLog will delete all
old log files that contain only flushed data. Rolling the HLog takes
very little time and is generally a good idea to do from time to time.

Each call to flushcache() will add an additional HStoreFile to each
HStore. Fetching a file from an HStore can potentially access all of
its HStoreFiles. This is time-consuming, so we want to periodically
compact these HStoreFiles into a single larger one. This is done by
calling HStore.compact().

Compaction is a very expensive operation. It's done automatically at
startup, and should probably be done periodically during operation.

The Google BigTable paper has a slightly-confusing hierarchy of major
and minor compactions. We have just two things to keep in mind:

1) A "flushcache()" drives all updates out of the memory buffer into
   on-disk structures. Upon flushcache, the log-reconstruction time
   goes to zero. Each flushcache() will add a new HStoreFile to each
   HStore.

2) a "compact()" consolidates all the HStoreFiles into a single
   one. It's expensive, and is always done at startup.

Unlike BigTable, Hadoop's HBase allows no period where updates have
been "committed" but have not been written to the log. This is not
hard to add, if it's really wanted.

We can merge two HRegions into a single new HRegion by calling
HRegion.closeAndMerge(). We can split an HRegion into two smaller
HRegions by calling HRegion.closeAndSplit().

OK, to sum up so far:

1) Clients access data in tables.
2) tables are broken into HRegions.
3) HRegions are served by HRegionServers. Clients contact an
   HRegionServer to access the data within its row-range.
4) HRegions store data in:
  a) HMemcache, a memory buffer for recent writes
  b) HLog, a write-log for recent writes
  c) HStores, an efficient on-disk set of files. One per col-group.
     (HStores use HStoreFiles.)

---------------------------------------------------------------
III.

Each HRegionServer stays in contact with the single HBaseMaster. The
HBaseMaster is responsible for telling each HRegionServer what
HRegions it should load and make available.

The HBaseMaster keeps a constant tally of which HRegionServers are
alive at any time. If the connection between an HRegionServer and the
HBaseMaster times out, then:

 a) The HRegionServer kills itself and restarts in an empty state.

 b) The HBaseMaster assumes the HRegionServer has died and reallocates
    its HRegions to other HRegionServers

Note that this is unlike Google's BigTable, where a TabletServer can
still serve Tablets after its connection to the Master has died. We
tie them together, because we do not use an external lock-management
system like BigTable. With BigTable, there's a Master that allocates
tablets and a lock manager (Chubby) that guarantees atomic access by
TabletServers to tablets. HBase uses just a single central point for
all HRegionServers to access: the HBaseMaster.

(This is no more dangerous than what BigTable does. Each system is
reliant on a network structure (whether HBaseMaster or Chubby) that
must survive for the data system to survive. There may be some
Chubby-specific advantages, but that's outside HBase's goals right
now.)

As HRegionServers check in with a new HBaseMaster, the HBaseMaster
asks each HRegionServer to load in zero or more HRegions. When the
HRegionServer dies, the HBaseMaster marks those HRegions as
unallocated, and attempts to give them to different HRegionServers.

Recall that each HRegion is identified by its table name and its
key-range. Since key ranges are contiguous, and they always start and
end with NULL, it's enough to simply indicate the end-key.

Unfortunately, this is not quite enough. Because of merge() and
split(), we may (for just a moment) have two quite different HRegions
with the same name. If the system dies at an inopportune moment, both
HRegions may exist on disk simultaneously. The arbiter of which
HRegion is "correct" is the HBase meta-information (to be discussed
shortly). In order to distinguish between different versions of the
same HRegion, we also add a unique 'regionId' to the HRegion name.

Thus, we finally get to this identifier for an HRegion:

tablename + endkey + regionId.

You can see this identifier being constructed in
HRegion.buildRegionName().

We can also use this identifier as a row-label in a different
HRegion. Thus, the HRegion meta-info is itself stored in an
HRegion. We call this table, which maps from HRegion identifiers to
physical HRegionServer locations, the META table.

The META table itself can grow large, and may be broken into separate
HRegions. To locate all components of the META table, we list all META
HRegions in a ROOT table. The ROOT table is always contained in a
single HRegion.

Upon startup, the HRegionServer immediately attempts to scan the ROOT
table (because there is only one HRegion for the ROOT table, that
HRegion's name is hard-coded). It may have to wait for the ROOT table
to be allocated to an HRegionServer.

Once the ROOT table is available, the HBaseMaster can scan it and
learn of all the META HRegions. It then scans the META table. Again,
the HBaseMaster may have to wait for all the META HRegions to be
allocated to different HRegionServers.

Finally, when the HBaseMaster has scanned the META table, it knows the
entire set of HRegions. It can then allocate these HRegions to the set
of HRegionServers.

The HBaseMaster keeps the set of currently-available HRegionServers in
memory. Since the death of the HBaseMaster means the death of the
entire system, there's no reason to store this information on
disk. All information about the HRegion->HRegionServer mapping is
stored physically on different tables. Thus, a client does not need to
contact the HBaseMaster after it learns the location of the ROOT
HRegion. The load on HBaseMaster should be relatively small: it deals
with timing out HRegionServers, scanning the ROOT and META upon
startup, and serving the location of the ROOT HRegion.

The HClient is fairly complicated, and often needs to navigate the
ROOT and META HRegions when serving a user's request to scan a
specific user table. If an HRegionServer is unavailable or it does not
have an HRegion it should have, the HClient will wait and retry. At
startup or in case of a recent HRegionServer failure, the correct
mapping info from HRegion to HRegionServer may not always be
available.

In summary:

1) HRegionServers offer access to HRegions (an HRegion lives at one
   HRegionServer)
2) HRegionServers check in with the HBaseMaster
3) If the HBaseMaster dies, the whole system dies
4) The set of current HRegionServers is known only to the HBaseMaster
5) The mapping between HRegions and HRegionServers is stored in two
   special HRegions, which are allocated to HRegionServers like any
   other.
6) The ROOT HRegion is a special one, the location of which the
   HBaseMaster always knows.
7) It's the HClient's responsibility to navigate all this.


---------------------------------------------------------------
IV.

What's the current status of all this code?

As of this writing, there is just shy of 7000 lines of code in the
"hbase" directory.

All of the single-machine operations (safe-committing, merging,
splitting, versioning, flushing, compacting, log-recovery) are
complete, have been tested, and seem to work great.

The multi-machine stuff (the HBaseMaster, the HRegionServer, and the
HClient) have not been fully tested. The reason is that the HClient is
still incomplete, so the rest of the distributed code cannot be
fully-tested. I think it's good, but can't be sure until the HClient
is done. However, the code is now very clean and in a state where
other people can understand it and contribute.

Other related features and TODOs:

1) Single-machine log reconstruction works great, but distributed log
   recovery is not yet implemented. This is relatively easy, involving
   just a sort of the log entries, placing the shards into the right
   DFS directories

2) Data compression is not yet implemented, but there is an obvious
   place to do so in the HStore.

3) We need easy interfaces to MapReduce jobs, so they can scan tables

4) The HMemcache lookup structure is relatively inefficient

5) File compaction is relatively slow; we should have a more
   conservative algorithm for deciding when to apply compaction.

6) For the getFull() operation, use of Bloom filters would speed
   things up

7) We need stress-test and performance-number tools for the whole
   system

8) There's some HRegion-specific testing code that worked fine during
   development, but it has to be rewritten so it works against an
   HRegion while it's hosted by an HRegionServer, and connected to an
   HBaseMaster. This code is at the bottom of the HRegion.java file.

