/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Time;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LeaseManager does the lease housekeeping for writing on files.   
 * This class also provides useful static methods for lease recovery.
 * 
 * Lease Recovery Algorithm
 * 1) Namenode retrieves lease information
 * 2) For each file f in the lease, consider the last block b of f
 * 2.1) Get the datanodes which contains b
 * 2.2) Assign one of the datanodes as the primary datanode p

 * 2.3) p obtains a new generation stamp from the namenode
 * 2.4) p gets the block info from each datanode
 * 2.5) p computes the minimum block length
 * 2.6) p updates the datanodes, which have a valid generation stamp,
 *      with the new generation stamp and the minimum block length 
 * 2.7) p acknowledges the namenode the update results

 * 2.8) Namenode updates the BlockInfo
 * 2.9) Namenode removes f from the lease
 *      and removes the lease once all files have been removed
 * 2.10) Namenode commit changes to edit log
 */
@InterfaceAudience.Private
public class LeaseManager {
  public static final Logger LOG = LoggerFactory.getLogger(LeaseManager.class
      .getName());
  private final FSNamesystem fsnamesystem;
  private long softLimit = HdfsConstants.LEASE_SOFTLIMIT_PERIOD;
  private long hardLimit;
  static final int INODE_FILTER_WORKER_COUNT_MAX = 4;
  static final int INODE_FILTER_WORKER_TASK_MIN = 512;
  private long lastHolderUpdateTime;
  private String internalLeaseHolder;

  //
  // Used for handling lock-leases
  // Mapping: leaseHolder -> Lease
  //
  private final HashMap<String, Lease> leases = new HashMap<>();
  // INodeID -> Lease
  private final TreeMap<Long, Lease> leasesById = new TreeMap<>();

  private Daemon lmthread;
  private volatile boolean shouldRunMonitor;

  LeaseManager(FSNamesystem fsnamesystem) {
    Configuration conf = new Configuration();
    this.fsnamesystem = fsnamesystem;
    this.hardLimit = conf.getLong(DFSConfigKeys.DFS_LEASE_HARDLIMIT_KEY,
        DFSConfigKeys.DFS_LEASE_HARDLIMIT_DEFAULT) * 1000;
    updateInternalLeaseHolder();
  }

  // Update the internal lease holder with the current time stamp.
  private void updateInternalLeaseHolder() {
    this.lastHolderUpdateTime = Time.monotonicNow();
    this.internalLeaseHolder = HdfsServerConstants.NAMENODE_LEASE_HOLDER +
        "-" + Time.formatTime(Time.now());
  }

  // Get the current internal lease holder name.
  String getInternalLeaseHolder() {
    long elapsed = Time.monotonicNow() - lastHolderUpdateTime;
    if (elapsed > hardLimit) {
      updateInternalLeaseHolder();
    }
    return internalLeaseHolder;
  }

  Lease getLease(String holder) {
    return leases.get(holder);
  }

  /**
   * This method iterates through all the leases and counts the number of blocks
   * which are not COMPLETE. The FSNamesystem read lock MUST be held before
   * calling this method.
   */
  synchronized long getNumUnderConstructionBlocks() {
    assert this.fsnamesystem.hasReadLock() : "The FSNamesystem read lock wasn't"
      + "acquired before counting under construction blocks";
    long numUCBlocks = 0;
    for (Long id : getINodeIdWithLeases()) {
      INode inode = fsnamesystem.getFSDirectory().getInode(id);
      if (inode == null) {
        // The inode could have been deleted after getINodeIdWithLeases() is
        // called, check here, and ignore it if so
        LOG.warn("Failed to find inode {} in getNumUnderConstructionBlocks().",
            id);
        continue;
      }
      final INodeFile cons = inode.asFile();
      if (!cons.isUnderConstruction()) {
        LOG.warn("The file {} is not under construction but has lease.",
            cons.getFullPathName());
        continue;
      }
      BlockInfo[] blocks = cons.getBlocks();
      if(blocks == null) {
        continue;
      }
      for(BlockInfo b : blocks) {
        if(!b.isComplete()) {
          numUCBlocks++;
        }
      }
    }
    LOG.info("Number of blocks under construction: {}", numUCBlocks);
    return numUCBlocks;
  }

  Collection<Long> getINodeIdWithLeases() {return leasesById.keySet();}

  /**
   * Get {@link INodesInPath} for all {@link INode} in the system
   * which has a valid lease.
   *
   * @return Set<INodesInPath>
   */
  @VisibleForTesting
  Set<INodesInPath> getINodeWithLeases() throws IOException {
    return getINodeWithLeases(null);
  }

  private synchronized INode[] getINodesWithLease() {
    List<INode> inodes = new ArrayList<>(leasesById.size());
    INode currentINode;
    for (long inodeId : leasesById.keySet()) {
      currentINode = fsnamesystem.getFSDirectory().getInode(inodeId);
      // A file with an active lease could get deleted, or its
      // parent directories could get recursively deleted.
      if (currentINode != null &&
          currentINode.isFile() &&
          !fsnamesystem.isFileDeleted(currentINode.asFile())) {
        inodes.add(currentINode);
      }
    }
    return inodes.toArray(new INode[0]);
  }

  /**
   * Get {@link INodesInPath} for all files under the ancestor directory which
   * has valid lease. If the ancestor directory is null, then return all files
   * in the system with valid lease. Callers must hold {@link FSNamesystem}
   * read or write lock.
   *
   * @param ancestorDir the ancestor {@link INodeDirectory}
   * @return {@code Set<INodesInPath>}
   */
  public Set<INodesInPath> getINodeWithLeases(final INodeDirectory
      ancestorDir) throws IOException {
    assert fsnamesystem.hasReadLock();
    final long startTimeMs = Time.monotonicNow();
    Set<INodesInPath> iipSet = new HashSet<>();
    final INode[] inodes = getINodesWithLease();
    int inodeCount = inodes.length;
    if (inodeCount == 0) {
      return iipSet;
    }

    List<Future<List<INodesInPath>>> futureList = Lists.newArrayList();
    final int workerCount = Math.min(INODE_FILTER_WORKER_COUNT_MAX,
        (((inodeCount - 1) / INODE_FILTER_WORKER_TASK_MIN) + 1));
    ExecutorService inodeFilterService =
        Executors.newFixedThreadPool(workerCount);
    for (int workerIdx = 0; workerIdx < workerCount; workerIdx++) {
      final int startIdx = workerIdx;
      Callable<List<INodesInPath>> c = new Callable<List<INodesInPath>>() {
        @Override
        public List<INodesInPath> call() {
          List<INodesInPath> iNodesInPaths = Lists.newArrayList();
          for (int idx = startIdx; idx < inodeCount; idx += workerCount) {
            INode inode = inodes[idx];
            if (!inode.isFile()) {
              continue;
            }
            INodesInPath inodesInPath = INodesInPath.fromINode(
                fsnamesystem.getFSDirectory().getRoot(), inode.asFile());
            if (ancestorDir != null &&
                !inodesInPath.isDescendant(ancestorDir)) {
              continue;
            }
            iNodesInPaths.add(inodesInPath);
          }
          return iNodesInPaths;
        }
      };

      // Submit the inode filter task to the Executor Service
      futureList.add(inodeFilterService.submit(c));
    }
    inodeFilterService.shutdown();

    for (Future<List<INodesInPath>> f : futureList) {
      try {
        iipSet.addAll(f.get());
      } catch (Exception e) {
        throw new IOException("Failed to get files with active leases", e);
      }
    }
    final long endTimeMs = Time.monotonicNow();
    if ((endTimeMs - startTimeMs) > 1000) {
      LOG.info("Took {} ms to collect {} open files with leases {}",
          (endTimeMs - startTimeMs), iipSet.size(), ((ancestorDir != null) ?
              " under " + ancestorDir.getFullPathName() : "."));
    }
    return iipSet;
  }

  public BatchedListEntries<OpenFileEntry> getUnderConstructionFiles(
      final long prevId) throws IOException {
    return getUnderConstructionFiles(prevId,
        OpenFilesIterator.FILTER_PATH_DEFAULT);
  }

  /**
   * Get a batch of under construction files from the currently active leases.
   * File INodeID is the cursor used to fetch new batch of results and the
   * batch size is configurable using below config param. Since the list is
   * fetched in batches, it does not represent a consistent view of all
   * open files.
   *
   * @see org.apache.hadoop.hdfs.DFSConfigKeys#DFS_NAMENODE_LIST_OPENFILES_NUM_RESPONSES
   * @param prevId the INodeID cursor
   * @throws IOException
   */
  public BatchedListEntries<OpenFileEntry> getUnderConstructionFiles(
      final long prevId, final String path) throws IOException {
    assert fsnamesystem.hasReadLock();
    SortedMap<Long, Lease> remainingLeases;
    synchronized (this) {
      remainingLeases = leasesById.tailMap(prevId, false);
    }
    Collection<Long> inodeIds = remainingLeases.keySet();
    final int numResponses = Math.min(
        this.fsnamesystem.getMaxListOpenFilesResponses(), inodeIds.size());
    final List<OpenFileEntry> openFileEntries =
        Lists.newArrayListWithExpectedSize(numResponses);

    int count = 0;
    String fullPathName = null;
    Iterator<Long> inodeIdIterator = inodeIds.iterator();
    while (inodeIdIterator.hasNext()) {
      Long inodeId = inodeIdIterator.next();
      INode ucFile = fsnamesystem.getFSDirectory().getInode(inodeId);
      if (ucFile == null) {
        //probably got deleted
        continue;
      }

      final INodeFile inodeFile = ucFile.asFile();
      if (!inodeFile.isUnderConstruction()) {
        LOG.warn("The file {} is not under construction but has lease.",
            inodeFile.getFullPathName());
        continue;
      }

      fullPathName = inodeFile.getFullPathName();
      if (StringUtils.isEmpty(path) ||
          DFSUtil.isParentEntry(fullPathName, path)) {
        openFileEntries.add(new OpenFileEntry(inodeFile.getId(), fullPathName,
            inodeFile.getFileUnderConstructionFeature().getClientName(),
            inodeFile.getFileUnderConstructionFeature().getClientMachine()));
        count++;
      }

      if (count >= numResponses) {
        break;
      }
    }
    // avoid rescanning all leases when we have checked all leases already
    boolean hasMore = inodeIdIterator.hasNext();
    return new BatchedListEntries<>(openFileEntries, hasMore);
  }

  /** @return the lease containing src */
  public synchronized Lease getLease(INodeFile src) {return leasesById.get(src.getId());}

  /** @return the number of leases currently in the system */
  @VisibleForTesting
  public synchronized int countLease() {
    return leases.size();
  }

  /** @return the number of paths contained in all leases */
  synchronized long countPath() {
    return leasesById.size();
  }

  /**
   * Adds (or re-adds) the lease for the specified file.
   */
  synchronized Lease addLease(String holder, long inodeId) {
    Lease lease = getLease(holder);
    if (lease == null) {
      lease = new Lease(holder);
      leases.put(holder, lease);
    } else {
      renewLease(lease);
    }
    leasesById.put(inodeId, lease);
    lease.files.add(inodeId);
    return lease;
  }

  synchronized void removeLease(long inodeId) {
    final Lease lease = leasesById.get(inodeId);
    if (lease != null) {
      removeLease(lease, inodeId);
    }
  }

  /**
   * Remove the specified lease and src.
   */
  private synchronized void removeLease(Lease lease, long inodeId) {
    leasesById.remove(inodeId);
    if (!lease.removeFile(inodeId)) {
      LOG.debug("inode {} not found in lease.files (={})", inodeId, lease);
    }

    if (!lease.hasFiles()) {
      if (leases.remove(lease.holder) == null) {
        LOG.error("{} not found", lease);
      }
    }
  }

  /**
   * Remove the lease for the specified holder and src
   */
  synchronized void removeLease(String holder, INodeFile src) {
    Lease lease = getLease(holder);
    if (lease != null) {
      removeLease(lease, src.getId());
    } else {
      LOG.warn("Removing non-existent lease! holder={} src={}", holder, src
          .getFullPathName());
    }
  }

  synchronized void removeAllLeases() {
    leasesById.clear();
    leases.clear();
  }

  /**
   * Reassign lease for file src to the new holder.
   */
  synchronized Lease reassignLease(Lease lease, INodeFile src,
                                   String newHolder) {
    assert newHolder != null : "new lease holder is null";
    if (lease != null) {
      removeLease(lease, src.getId());
    }
    return addLease(newHolder, src.getId());
  }

  /**
   * Renew the lease(s) held by the given client
   */
  synchronized void renewLease(String holder) {
    renewLease(getLease(holder));
  }

  synchronized void renewLease(Lease lease) {
    if (lease != null) {
      lease.renew();
    }
  }

  /**
   * Renew all of the currently open leases.
   */
  synchronized void renewAllLeases() {
    for (Lease l : leases.values()) {
      renewLease(l);
    }
  }

  /************************************************************
   * A Lease governs all the locks held by a single client.
   * For each client there's a corresponding lease, whose
   * timestamp is updated when the client periodically
   * checks in.  If the client dies and allows its lease to
   * expire, all the corresponding locks can be released.
   *************************************************************/
  class Lease {
    private final String holder;
    private long lastUpdate;
    private final HashSet<Long> files = new HashSet<>();

    /** Only LeaseManager object can create a lease */
    private Lease(String h) {
      this.holder = h;
      renew();
    }
    /** Only LeaseManager object can renew a lease */
    private void renew() {
      this.lastUpdate = monotonicNow();
    }

    /** @return true if the Hard Limit Timer has expired */
    public boolean expiredHardLimit() {
      return monotonicNow() - lastUpdate > hardLimit;
    }

    public boolean expiredHardLimit(long now) {
      return now - lastUpdate > hardLimit;
    }

    /** @return true if the Soft Limit Timer has expired */
    public boolean expiredSoftLimit() {
      return monotonicNow() - lastUpdate > softLimit;
    }

    /** Does this lease contain any path? */
    boolean hasFiles() {return !files.isEmpty();}

    boolean removeFile(long inodeId) {
      return files.remove(inodeId);
    }

    @Override
    public String toString() {
      return "[Lease.  Holder: " + holder
          + ", pending creates: " + files.size() + "]";
    }

    @Override
    public int hashCode() {
      return holder.hashCode();
    }

    private Collection<Long> getFiles() {
      return Collections.unmodifiableCollection(files);
    }

    String getHolder() {
      return holder;
    }

    @VisibleForTesting
    long getLastUpdate() {
      return lastUpdate;
    }
  }

  public void setLeasePeriod(long softLimit, long hardLimit) {
    this.softLimit = softLimit;
    this.hardLimit = hardLimit; 
  }

  private synchronized Collection<Lease> getExpiredCandidateLeases() {
    final long now = Time.monotonicNow();
    Collection<Lease> expired = new HashSet<>();
    for (Lease lease : leases.values()) {
      if (lease.expiredHardLimit(now)) {
        expired.add(lease);
      }
    }
    return expired;
  }
  
  /******************************************************
   * Monitor checks for leases that have expired,
   * and disposes of them.
   ******************************************************/
  class Monitor implements Runnable {
    final String name = getClass().getSimpleName();

    /** Check leases periodically. */
    @Override
    public void run() {
      for(; shouldRunMonitor && fsnamesystem.isRunning(); ) {
        boolean needSync = false;
        try {
          // sleep now to avoid infinite loop if an exception was thrown.
          Thread.sleep(fsnamesystem.getLeaseRecheckIntervalMs());

          // pre-filter the leases w/o the fsn lock.
          Collection<Lease> candidates = getExpiredCandidateLeases();
          if (candidates.isEmpty()) {
            continue;
          }

          fsnamesystem.writeLockInterruptibly();
          try {
            if (!fsnamesystem.isInSafeMode()) {
              needSync = checkLeases(candidates);
            }
          } finally {
            fsnamesystem.writeUnlock("leaseManager");
            // lease reassignments should to be sync'ed.
            if (needSync) {
              fsnamesystem.getEditLog().logSync();
            }
          }
        } catch(InterruptedException ie) {
          LOG.debug("{} is interrupted", name, ie);
        } catch(Throwable e) {
          LOG.warn("Unexpected throwable: ", e);
        }
      }
    }
  }

  /** Check the leases beginning from the oldest.
   *  @return true is sync is needed.
   */
  @VisibleForTesting
  synchronized boolean checkLeases() {
    return checkLeases(getExpiredCandidateLeases());
  }

  private synchronized boolean checkLeases(Collection<Lease> leasesToCheck) {
    boolean needSync = false;
    assert fsnamesystem.hasWriteLock();

    long start = monotonicNow();
    for (Lease leaseToCheck : leasesToCheck) {
      if (isMaxLockHoldToReleaseLease(start)) {
        break;
      }
      if (!leaseToCheck.expiredHardLimit(Time.monotonicNow())) {
        continue;
      }
      LOG.info("{} has expired hard limit", leaseToCheck);
      final List<Long> removing = new ArrayList<>();
      // need to create a copy of the oldest lease files, because
      // internalReleaseLease() removes files corresponding to empty files,
      // i.e. it needs to modify the collection being iterated over
      // causing ConcurrentModificationException
      Collection<Long> files = leaseToCheck.getFiles();
      Long[] leaseINodeIds = files.toArray(new Long[files.size()]);
      FSDirectory fsd = fsnamesystem.getFSDirectory();
      String p = null;
      String newHolder = getInternalLeaseHolder();
      for(Long id : leaseINodeIds) {
        try {
          INodesInPath iip = INodesInPath.fromINode(fsd.getInode(id));
          p = iip.getPath();
          // Sanity check to make sure the path is correct
          if (!p.startsWith("/")) {
            throw new IOException("Invalid path in the lease " + p);
          }
          final INodeFile lastINode = iip.getLastINode().asFile();
          if (fsnamesystem.isFileDeleted(lastINode)) {
            // INode referred by the lease could have been deleted.
            removeLease(lastINode.getId());
            continue;
          }
          ImmutablePair<Boolean, Boolean> releaseLeasePair =
              ImmutablePair.of(false, false);
          try {
            releaseLeasePair = fsnamesystem.internalReleaseLease(
                leaseToCheck, p, iip, newHolder);
          } catch (IOException e) {
            LOG.warn("Cannot release the path {} in the lease {}. It will be "
                + "retried.", p, leaseToCheck, e);
            continue;
          }
          if (LOG.isDebugEnabled()) {
            if (releaseLeasePair.getLeft()) {
              LOG.debug("Lease recovery for inode {} is complete. File closed"
                  + ".", id);
            } else {
              LOG.debug("Started block recovery {} lease {}", p, leaseToCheck);
            }
          }
          // If a lease recovery happened or file has been closed, we need to sync later.
          boolean isNeedSync = releaseLeasePair.getRight() || releaseLeasePair.getLeft();
          if (!needSync && isNeedSync) {
            needSync = true;
          }
        } catch (IOException e) {
          LOG.warn("Removing lease with an invalid path: {},{}", p,
              leaseToCheck, e);
          removing.add(id);
        }
        if (isMaxLockHoldToReleaseLease(start)) {
          LOG.debug("Breaking out of checkLeases after {} ms.",
              fsnamesystem.getMaxLockHoldToReleaseLeaseMs());
          break;
        }
      }

      for (Long id : removing) {
        removeLease(leaseToCheck, id);
      }
    }
    return needSync;
  }


  /** @return true if max lock hold is reached */
  private boolean isMaxLockHoldToReleaseLease(long start) {
    return monotonicNow() - start >
        fsnamesystem.getMaxLockHoldToReleaseLeaseMs();
  }

  @Override
  public synchronized String toString() {
    return getClass().getSimpleName() + "= {"
        + "\n leases=" + leases
        + "\n leasesById=" + leasesById
        + "\n}";
  }

  void startMonitor() {
    Preconditions.checkState(lmthread == null,
        "Lease Monitor already running");
    shouldRunMonitor = true;
    lmthread = new Daemon(new Monitor());
    lmthread.start();
  }
  
  void stopMonitor() {
    if (lmthread != null) {
      shouldRunMonitor = false;
      try {
        lmthread.interrupt();
        lmthread.join(3000);
      } catch (InterruptedException ie) {
        LOG.warn("Encountered exception ", ie);
      }
      lmthread = null;
    }
  }

  /**
   * Trigger the currently-running Lease monitor to re-check
   * its leases immediately. This is for use by unit tests.
   */
  @VisibleForTesting
  public void triggerMonitorCheckNow() {
    Preconditions.checkState(lmthread != null,
        "Lease monitor is not running");
    lmthread.interrupt();
  }

  @VisibleForTesting
  public void runLeaseChecks() {
    checkLeases();
  }

}
