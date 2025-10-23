<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

HDFS Namenode Fine-grained Locking
==================================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Overview
--------

HDFS relies on a single master, the Namenode (NN), as its metadata center.
From an architectural point of view, a few elements make NN the bottleneck of an HDFS cluster:
* NN keeps the entire namespace in memory (directory tree, blocks, Datanode related info, etc.)
* Read requests (`getListing`, `getFileInfo`, `getBlockLocations`) are served from memory.
Write requests (`mkdir`, `create`, `addBlock`, `complete`) update the memory state and write a journal transaction into QJM.
Both types of requests need a locking mechanism to ensure data consistency and correctness.
* All requests are funneled into NN and have to go through the global FS lock.
Each write operation acquires this lock in write mode and holds it until that operation is executed.
This lock mode prevents concurrent execution of write operations even if they involve different branches of the directory tree.

NN fine-grained locking (FGL) implementation aims to alleviate this bottleneck by allowing concurrency of disjoint write operations.

JIRA: [HDFS-17366](https://issues.apache.org/jira/browse/HDFS-17366)

Design
------
In theory, fully independent operations can be processed concurrently, such as operations involving different subdirectory trees.
As such, NN can split the global lock into the full path lock, just using the full path lock to protect a special subdirectory tree.

### RPC Categorization

Roughly, RPC operations handled by NN can be divided into 8 main categories

| Category                               | Operations                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
|----------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Involving namespace tree               | `mkdir`, `create` (without overwrite), `getFileInfo` (without locations), `getListing` (without locations), `setOwner`, `setPermission`, `getStoragePolicy`, `setStoragePolicy`, `rename`, `isFileClosed`, `getFileLinkInfo`, `setTimes`, `modifyAclEntries`, `removeAclEntries`, `setAcl`, `getAcl`, `setXAttr`, `getXAttrs`, `listXAttrs`, `removeXAttr`, `checkAccess`, `getErasureCodingPolicy`, `unsetErasureCodingPolicy`, `getQuotaUsage`, `getPreferredBlockSize` |
| Involving only blocks                  | `reportBadBlocks`, `updateBlockForPipeline`, `updatePipeline`                                                                                                                                                                                                                                                                                                                                                                                                             |
| Involving only DNs                     | `registerDatanode`, `setBalancerBandwidth`, `sendHeartbeat`                                                                                                                                                                                                                                                                                                                                                                                                               |
| Involving both namespace tree & blocks | `getBlockLocation`, `create` (with overwrite), `append`, `setReplication`, `abandonBlock`, `addBlock`, `getAdditionalDatanode`, `complete`, `concat`, `truncate`, `delete`, `getListing` (with locations), `getFileInfo` (with locations), `recoverLease`, `listCorruptFileBlocks`, `fsync`, `commitBlockSynchronization`, `RedundancyMonitor`, `processMisReplicatedBlocks`                                                                                              |
| Involving both DNs & blocks            | `getBlocks`, `errorReport`                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| Involving namespace tree, DNs & blocks | `blockReport`, `blockReceivedAndDeleted`, `HeartbeatManager`, `Decommission`                                                                                                                                                                                                                                                                                                                                                                                              |
| Requiring locking the entire namespace | `rollEditLog`, `startCommonService`, `startActiveService`, `saveNamespace`, `rollEdits`, `EditLogTailer`, `rollingUpgrade`                                                                                                                                                                                                                                                                                                                                                |
| Requiring no locking                   | `getServerDefaults`, `getStats`                                                                                                                                                                                                                                                                                                                                                                                                                                           |

For operations involving namespace tree, fully independent operations can be handled by NN concurrently. Almost all of them use the full path as a parameter, e.g. `create`, `mkdirs`, `getFileInfo`, etc. So we can use a full path lock to make them thread-safe.

For operations involving blocks, one block belongs to one and only one `INodeFile`, so NN can use the namespace tree to make these operations thread-safe.

For operations involving DNs, NN needs a separate DN lock because DNs operate separately from the namespace tree.

For operations requiring the entire namespace locked, the global lock can be used to make these operations thread-safe. In general, these operations have low frequency and thus low impact despite the global locking.

### Full Path Lock

Used to protect operations involving namespace tree.

All of these operations receive a path or INodeID as a parameter and can further be divided into 3 main subcategories:
1. Parameters contain only one path (`create`, `mkdir`)
2. Parameters contain multiple paths (`rename`, `concat`)
3. Parameters contain INodeID (`addBlock`, `complete`)

For type 1, NN acquires a full path lock according to its semantics. Take `setPermission("/a/b/c/f.txt")` for example, the set of locks to acquire are ReadLock("/"), ReadLock("a"), ReadLock("b"), ReadLock("c") and WriteLock("f.txt"). Different lock patterns are explained in a later section.

For type 2, NN acquires full path locks in a predefined order, such as the lexicographic order, to avoid deadlocks.

For type 3, NN acquires a full path lock by in the following fashion:
- Unsafely obtains full path recursively
- Acquires the full path lock according to the lock mode
- Rechecks whether the last node of the full path is equal to the INodeID given
  - If not, that means that the `INodeFile` might have been renamed or concatenated, need to retry
  - If the max retry attempts have been reached, throw a `RetryException` to client to let client retry

### `INodeFile` Lock

Used to protect operations involving blocks.

One block belongs to one and only one `INodeFile`, so NN can use the INodeFile lock to make operations thread-safe. Normally, there is no need to acquire the full path lock since changing the namespace tree structure does not affect the block.

`concat` might change the `INodeFile` a block belongs to. Since both block related operations and `concat` need to acquire the `INodeFile` write lock, only one of them can be processed at a time.

### DN Lock

Used to protect operations involving DNs.

NN uses a `DatanodeDescriptor` object to store the information of one DN and uses `DatanodeManager` to manage all DNs in the memory. `DatanodeDescriptor` uses `DatanodeStorageInfo` to store the information of one storage device on one DN.

DNs have nothing to do with the namespace tree, so NN uses a separate DN lock for these operations. Since DNs are independent of one another, NN can assign a lock to each DN.

### Global Lock

Used for operations requiring the entire namespace locked.

There are some operations that need to lock the entire namespace, e.g. safe mode related operations, HA service related, etc. NN uses the global lock to make these operations thread-safe. Outside of these infrequent operations that require the global write lock, all other operations have to acquire the global read lock. The only exception to this rule is JMX operations being allowed to bypass locking entirely to ensure that metrics can be collected regardless of long write lock holding.

### Lock Order

As mentioned above, there are the global lock, DN lock, and full path lock. NN acquires locks in this specific order to avoid deadlocks.

Locks are to be acquired in this order:
- Global > DN > Full path
- Global > DN > Last `INodeFile`

Possible lock combinations are as follows:
- Global write lock
- Global read lock > Full path lock
- Global read lock > DN read/write lock
- Global read lock > DN read/write lock > Read/Write lock of last `INodeFile`
- Global read lock > DN read/write lock > Full path lock

### Lock Pools

NN allocates locks as needed to the INodes used by active threads, and deletes them after the locks are no longer in use. Locks for commonly accessed `INode`s like the root are cached.

NN uses an `INodeLockPool` to manage these locks. The lock pool:
- Returns a closeable lock for an INode based on the lock type,
- Removes this lock if it is no longer used by any threads.

Similar to `INodeLockPool`, a `DNLockPool` is used to manage the locks for DNs. Unlike `INodeLockPool`, `DNLockPool` keeps all locks in memory due to the comparatively lower number of locks.

### Lock Modes

Operations related to namespace tree have different semantics and may involve the modification or access of different INodes, for example: `getBlockLocation` only accesses the last iNodeFile, `delete` modifies both the parent and the last INode, `mkdir` may modify multiple ancestor INodes.

Four lock modes (plus no locking):
- LOCK_READ
  - This lock mode acquires the read locks for all INodes down the path.
  - Example operations: `getBlockLocation`, `getFileInfo`.
- LOCK_WRITE
  - This lock mode acquires the write lock for the last INode and the read locks for all ancestor INodes in the full path.
  - Example operations: `setPermission`, `setReplication`.
- LOCK_PARENT
  - This lock mode acquires the write lock for the last two INodes and the read locks for all remaining ancestor INodes in the full path.
  - Example operations: `rename`, `delete`, `create` (when the parent directory exists).
- LOCK_ANCESTOR
  - This lock mode acquires the write lock for the last existing INode and the read locks for all remaining ancestor INodes in the full path.
  - Example operations: `mkdir`, `create` (when the parent directory doesn't exist).
- NONE
  - This lock mode does not acquire any locks for the given path.

Roadmap
-------

#### Stage 1: Split the global lock into FSLock and BMLock

Split the global lock into two global locks, FSLock and BMLock.
- FSLock for operations that relate to namespace tree.
- BMLock for operations related to blocks and/or operations related to DNs.
- Both FSLock and BMLock for HA related operations.
After this step, FGL contains global FSLock and global BMLock.

No big logic changes in this step. The original logic with the global lock retains. This step aims to make the lock mode configurable.

JIRA: [HDFS-17384](https://issues.apache.org/jira/browse/HDFS-17384) [Progress: Done]

#### Stage 2: Split the global FSLock

After splitting the global lock into FSLock and BMLock, this step aims to split the global FSLock into full path locks so that fully independent operations that only involve namespace tree can be processed concurrently.
In this step, NN still uses the global BMLock to protect block related operations and DN related operations.
After this step, FGL contains global FSLock, full path lock, and global BMLock.

JIRA: [HDFS-17385](https://issues.apache.org/jira/browse/HDFS-17385) [Progress: Ongoing]

#### Stage 3: Split the global BMLock

This step aims to split the global BMLock into full path locks and DN locks.
After this step, FGL contains global FSLock, DN lock, and full path lock.

JIRA: [HDFS-17386](https://issues.apache.org/jira/browse/HDFS-17386) [Progress: Ongoing]

Configuration
------------

NN FGL implementation can be used by adding this configuration to `hdfs-site.xml`.

    <property>
      <name>dfs.namenode.lock.model.provider.class</name>
      <value>org.apache.hadoop.hdfs.server.namenode.fgl.FineGrainedFSNamesystemLock</value>
      <description>
        An implementation class of FSNamesystem lock.
        Defaults to GlobalFSNamesystemLock.class
      </description>
    </property>

The lock manager class must implement the interface defined by `org.apache.hadoop.hdfs.server.namenode.fgl.FSNLockManager`. Currently, there are two implementations:
* `org.apache.hadoop.hdfs.server.namenode.fgl.GlobalFSNamesystemLock`: the original lock mode that utilizes one global FS lock, also the default value for this config;
* `org.apache.hadoop.hdfs.server.namenode.fgl.FineGrainedFSNamesystemLock`: FGL implementation.

Adding RPC
----------

For developers adding a new RPC operation, the operation should follow FGL locking schematic to ensure data integrity:
* Global FSLock should be acquired in read mode, unless it is an administrative operation (related to HA, edit logs, etc.)
* If the operation requires access/modification of `DatanodeDescriptor` and/or `DatanodeStorageInfo`, DN lock should be acquired in read/write mode accordingly.
  * Only applicable in stage 3 once DN lock is implemented. During stage 1 and stage 2, global BMLock is to be used instead.
* If the operation deals with one or more paths/blocks, the full path lock(s) should be acquired based on the implementation details described above. It is best to check an existing RPC operation that has a similar method of access to the new operation to consult the lock implementation.
