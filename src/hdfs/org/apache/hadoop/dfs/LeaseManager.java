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
package org.apache.hadoop.dfs;

import java.io.IOException;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * LeaseManager does the lease housekeeping for writing on files.   
 * This class also provides useful static methods for lease recovery.
 * 
 * Lease Recovery Algorithm
 * 1) Namenode retrieves lease information
 * 2) For each file f in the lease, consider the last block b of f
 * 2.1) Get the datanodes which contains b
 * 2.2) Assign one of the datanodes as the primary datanode p

 * 2.3) p obtains a new generation stamp form the namenode
 * 2.4) p get the block info from each datanode
 * 2.5) p computes the minimum block length
 * 2.6) p updates the datanodes, which have a valid generation stamp,
 *      with the new generation stamp and the minimum block length 
 * 2.7) p acknowledges the namenode the update results

 * 2.8) Namenode updates the BlockInfo
 * 2.9) Namenode removes f from the lease
 *      and removes the lease once all files have been removed
 * 2.10) Namenode commit changes to edit log
 */
class LeaseManager {
  static final Log LOG = LogFactory.getLog(LeaseManager.class);

  private final FSNamesystem fsnamesystem;

  private long softLimit = FSConstants.LEASE_SOFTLIMIT_PERIOD;
  private long hardLimit = FSConstants.LEASE_HARDLIMIT_PERIOD;

  //
  // Used for handling lock-leases
  // Mapping: leaseHolder -> Lease
  //
  private SortedMap<StringBytesWritable, Lease> leases = new TreeMap<StringBytesWritable, Lease>();
  // Set of: Lease
  private SortedSet<Lease> sortedLeases = new TreeSet<Lease>();

  // 
  // Map path names to leases. It is protected by the sortedLeases lock.
  // The map stores pathnames in lexicographical order.
  //
  private SortedMap<String, Lease> sortedLeasesByPath = new TreeMap<String, Lease>();

  LeaseManager(FSNamesystem fsnamesystem) {this.fsnamesystem = fsnamesystem;}

  Lease getLease(StringBytesWritable holder) throws IOException {
    return leases.get(holder);
  }
  
  SortedSet<Lease> getSortedLeases() {return sortedLeases;}

  /** @return the lease containing src */
  Lease getLeaseByPath(String src) {return sortedLeasesByPath.get(src);}

  /** list of blocks being recovered */
  private static Map<Block, Block> ongoingRecovery = new HashMap<Block, Block>();


  /** @return the number of leases currently in the system */
  synchronized int countLease() {return sortedLeases.size();}

  /** @return the number of paths contained in all leases */
  synchronized int countPath() {
    int count = 0;
    for(Lease lease : sortedLeases) {
      count += lease.getPaths().size();
    }
    return count;
  }
  
  /**
   * Adds (or re-adds) the lease for the specified file.
   */
  synchronized void addLease(StringBytesWritable holder, String src
      ) throws IOException {
    Lease lease = getLease(holder);
    if (lease == null) {
      lease = new Lease(holder);
      leases.put(holder, lease);
      sortedLeases.add(lease);
    } else {
      renewLease(lease);
    }
    sortedLeasesByPath.put(src, lease);
    lease.paths.add(new StringBytesWritable(src));
  }

  /**
   * Remove the specified lease and src.
   */
  synchronized void removeLease(Lease lease, String src) throws IOException {
    sortedLeasesByPath.remove(src);
    if (!lease.removePath(src)) {
      LOG.error(src + " not found in lease.paths (=" + lease.paths + ")");
    }

    if (!lease.hasPath()) {
      leases.remove(lease.holder);
      if (!sortedLeases.remove(lease)) {
        LOG.error(lease + " not found in sortedLeases");
      }
    }
  }

  /**
   * Remove the lease for the specified holder and src
   */
  synchronized void removeLease(StringBytesWritable holder, String src
      ) throws IOException {
    Lease lease = getLease(holder);
    if (lease != null) {
      removeLease(lease, src);
    }
  }

  /**
   * Finds the pathname for the specified pendingFile
   */
  synchronized String findPath(INodeFileUnderConstruction pendingFile
      ) throws IOException {
    Lease lease = getLease(pendingFile.clientName);
    if (lease != null) {
      String src = lease.findPath(pendingFile);
      if (src != null) {
        return src;
      }
    }
    throw new IOException("pendingFile (=" + pendingFile + ") not found."
        + "(lease=" + lease + ")");
  }

  /**
   * Renew the lease(s) held by the given client
   */
  synchronized void renewLease(String holder) throws IOException {
    renewLease(getLease(new StringBytesWritable(holder)));
  }
  synchronized void renewLease(Lease lease) {
    if (lease != null) {
      sortedLeases.remove(lease);
      lease.renew();
      sortedLeases.add(lease);
    }
  }

  /************************************************************
   * A Lease governs all the locks held by a single client.
   * For each client there's a corresponding lease, whose
   * timestamp is updated when the client periodically
   * checks in.  If the client dies and allows its lease to
   * expire, all the corresponding locks can be released.
   *************************************************************/
  class Lease implements Comparable<Lease> {
    private StringBytesWritable holder;
    private long lastUpdate;
    private Collection<StringBytesWritable> paths = new TreeSet<StringBytesWritable>();
  
    /** Only LeaseManager object can create a lease */
    private Lease(StringBytesWritable holder) throws IOException {
      this.holder = holder;
      renew();
    }
    /** Only LeaseManager object can renew a lease */
    private void renew() {
      this.lastUpdate = FSNamesystem.now();
    }

    /** @return true if the Hard Limit Timer has expired */
    public boolean expiredHardLimit() {
      return FSNamesystem.now() - lastUpdate > hardLimit;
    }

    /** @return true if the Soft Limit Timer has expired */
    public boolean expiredSoftLimit() {
      return FSNamesystem.now() - lastUpdate > softLimit;
    }

    /**
     * @return the path associated with the pendingFile and null if not found.
     */
    private String findPath(INodeFileUnderConstruction pendingFile) {
      for(Iterator<StringBytesWritable> i = paths.iterator(); i.hasNext(); ) {
        String src = i.next().toString();
        if (fsnamesystem.dir.getFileINode(src) == pendingFile) {
          return src;
        }
      }
      return null;
    }

    /** Does this lease contain any path? */
    boolean hasPath() {return !paths.isEmpty();}

    boolean removePath(String src) throws IOException {
      return paths.remove(new StringBytesWritable(src));
    }

    /** {@inheritDoc} */
    public String toString() {
      return "[Lease.  Holder: " + holder
          + ", pendingcreates: " + paths.size() + "]";
    }
  
    /** {@inheritDoc} */
    public int compareTo(Lease o) {
      Lease l1 = this;
      Lease l2 = o;
      long lu1 = l1.lastUpdate;
      long lu2 = l2.lastUpdate;
      if (lu1 < lu2) {
        return -1;
      } else if (lu1 > lu2) {
        return 1;
      } else {
        return l1.holder.compareTo(l2.holder);
      }
    }
  
    /** {@inheritDoc} */
    public boolean equals(Object o) {
      if (!(o instanceof Lease)) {
        return false;
      }
      Lease obj = (Lease) o;
      if (lastUpdate == obj.lastUpdate &&
          holder.equals(obj.holder)) {
        return true;
      }
      return false;
    }
  
    /** {@inheritDoc} */
    public int hashCode() {
      return holder.hashCode();
    }
    
    Collection<StringBytesWritable> getPaths() {
      return paths;
    }
    
    void replacePath(String oldpath, String newpath) throws IOException {
      paths.remove(new StringBytesWritable(oldpath));
      paths.add(new StringBytesWritable(newpath));
    }
  }

  synchronized void changeLease(String src, String dst,
      String overwrite, String replaceBy) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(getClass().getSimpleName() + ".changelease: " +
               " src=" + src + ", dest=" + dst + 
               ", overwrite=" + overwrite +
               ", replaceBy=" + replaceBy);
    }

    for(Map.Entry<String, Lease> entry : findLeaseWithPrefixPath(src, sortedLeasesByPath)) {
      final String oldpath = entry.getKey();
      final Lease lease = entry.getValue();
      final String newpath = oldpath.replaceFirst(
          java.util.regex.Pattern.quote(overwrite), replaceBy);
      if (LOG.isDebugEnabled()) {
        LOG.debug("changeLease: replacing " + oldpath + " with " + newpath);
      }
      lease.replacePath(oldpath, newpath);
      sortedLeasesByPath.remove(oldpath);
      sortedLeasesByPath.put(newpath, lease);
    }
  }

  synchronized void removeLeaseWithPrefixPath(String prefix) throws IOException {
    for(Map.Entry<String, Lease> entry : findLeaseWithPrefixPath(prefix, sortedLeasesByPath)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(LeaseManager.class.getSimpleName()
            + ".removeLeaseWithPrefixPath: entry=" + entry);
      }
      removeLease(entry.getValue(), entry.getKey());    
    }
  }

  static private List<Map.Entry<String, Lease>> findLeaseWithPrefixPath(
      String prefix, SortedMap<String, Lease> path2lease) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(LeaseManager.class.getSimpleName() + ".findLease: prefix=" + prefix);
    }

    List<Map.Entry<String, Lease>> entries = new ArrayList<Map.Entry<String, Lease>>();
    final int srclen = prefix.length();

    for(Map.Entry<String, Lease> entry : path2lease.tailMap(prefix).entrySet()) {
      final String p = entry.getKey();
      if (!p.startsWith(prefix)) {
        return entries;
      }
      if (p.length() == srclen || p.charAt(srclen) == Path.SEPARATOR_CHAR) {
        entries.add(entry);
      }
    }
    return entries;
  }

  void setLeasePeriod(long softLimit, long hardLimit) {
    this.softLimit = softLimit;
    this.hardLimit = hardLimit; 
  }
  
  Monitor createMonitor() {return new Monitor();}

  /******************************************************
   * Monitor checks for leases that have expired,
   * and disposes of them.
   ******************************************************/
  class Monitor implements Runnable {
    public void run() {
      try {
        while (fsnamesystem.isRunning()) {
          synchronized (fsnamesystem) {
            synchronized (LeaseManager.this) {
              Lease top;
              while ((sortedLeases.size() > 0) &&
                     ((top = sortedLeases.first()) != null)) {
                if (top.expiredHardLimit()) {
                  LOG.info("Lease Monitor: Removing lease " + top
                      + ", sortedLeases.size()=: " + sortedLeases.size());
                  for(StringBytesWritable s : top.paths) {
                    fsnamesystem.internalReleaseLease(top, s.getString());
                  }
                } else {
                  break;
                }
              }
            }
          }
          try {
            Thread.sleep(2000);
          } catch(InterruptedException ie) {
            if (LOG.isDebugEnabled()) {
              LOG.debug(getClass().getName() + " is interrupted", ie);
            }
          }
        }
      } catch(Exception e) {
        LOG.error("In " + getClass().getName(), e);
      }
    }
  }

  /** {@inheritDoc} */
  public String toString() {
    return getClass().getSimpleName() + "= {"
        + "\n leases=" + leases
        + "\n sortedLeases=" + sortedLeases
        + "\n sortedLeasesByPath=" + sortedLeasesByPath
        + "\n}";
  }

  /*
   * The following codes provides useful static methods for lease recovery.
   */
  /** A convenient class used in lease recovery */
  private static class BlockRecord { 
    final DatanodeID id;
    final InterDatanodeProtocol datanode;
    final Block block;
    
    BlockRecord(DatanodeID id, InterDatanodeProtocol datanode, Block block) {
      this.id = id;
      this.datanode = datanode;
      this.block = block;
    }

    public String toString() {
      return "block:" + block + " node:" + id;
    }
  }

  /**
   * Recover a list of blocks.
   * This method is invoked by the primary datanode.
   */
  static void recoverBlocks(Block[] blocks, DatanodeID[][] targets,
      DataNode primary, DatanodeProtocol namenode, Configuration conf) {
    for(int i = 0; i < blocks.length; i++) {
      try {
        DataNode.logRecoverBlock("NameNode", blocks[i], targets[i]);
        recoverBlock(blocks[i], targets[i], primary, namenode, conf, true);
      } catch (IOException e) {
        LOG.warn("recoverBlocks FAILED, blocks[" + i + "]=" + blocks[i], e);
      }
    }
  }

  /** Recover a block */
  static Block recoverBlock(Block block, DatanodeID[] datanodeids,
      DataNode primary, DatanodeProtocol namenode, Configuration conf,
      boolean closeFile) throws IOException {

    // If the block is already being recovered, then skip recovering it.
    // This can happen if the namenode and client start recovering the same
    // file at the same time.
    synchronized (ongoingRecovery) {
      Block tmp = new Block();
      tmp.set(block.blkid, block.len, GenerationStamp.WILDCARD_STAMP);
      if (ongoingRecovery.get(tmp) != null) {
        String msg = "Block " + block + " is already being recovered, " +
                     " ignoring this request to recover it.";
        LOG.info(msg);
        throw new IOException(msg);
      }
      ongoingRecovery.put(block, block);
    }
    try {
      List<BlockRecord> syncList = new ArrayList<BlockRecord>();
      long minlength = Long.MAX_VALUE;
      int errorCount = 0;

      //check generation stamps
      for(DatanodeID id : datanodeids) {
        try {
          InterDatanodeProtocol datanode = primary.dnRegistration.equals(id)?
              primary: DataNode.createInterDataNodeProtocolProxy(id, conf);
          BlockMetaDataInfo info = datanode.getBlockMetaDataInfo(block);
          if (info != null && info.getGenerationStamp() >= block.generationStamp) {
            syncList.add(new BlockRecord(id, datanode, new Block(info)));
            if (info.len < minlength) {
              minlength = info.len;
            }
          }
        } catch (IOException e) {
          ++errorCount;
          InterDatanodeProtocol.LOG.warn(
              "Failed to getBlockMetaDataInfo for block (=" + block 
              + ") from datanode (=" + id + ")", e);
        }
      }

      if (syncList.isEmpty() && errorCount > 0) {
        throw new IOException("All datanodes failed: block=" + block
            + ", datanodeids=" + Arrays.asList(datanodeids));
      }
      return syncBlock(block, minlength, syncList, namenode, closeFile);
    } finally {
      synchronized (ongoingRecovery) {
        ongoingRecovery.remove(block);
      }
    }
  }

  /** Block synchronization */
  private static Block syncBlock(Block block, long minlength,
      List<BlockRecord> syncList, DatanodeProtocol namenode,
      boolean closeFile) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("block=" + block + ", minlength=" + minlength
          + ", syncList=" + syncList + ", closeFile=" + closeFile);
    }

    //syncList.isEmpty() that all datanodes do not have the block
    //so the block can be deleted.
    if (syncList.isEmpty()) {
      namenode.commitBlockSynchronization(block, 0, 0, closeFile, true,
          DatanodeID.EMPTY_ARRAY);
      return null;
    }

    List<DatanodeID> successList = new ArrayList<DatanodeID>();

    long generationstamp = namenode.nextGenerationStamp(block);
    Block newblock = new Block(block.blkid, minlength, generationstamp);

    for(BlockRecord r : syncList) {
      try {
        r.datanode.updateBlock(r.block, newblock, closeFile);
        successList.add(r.id);
      } catch (IOException e) {
        InterDatanodeProtocol.LOG.warn("Failed to updateBlock (newblock="
            + newblock + ", datanode=" + r.id + ")", e);
      }
    }

    if (!successList.isEmpty()) {
      namenode.commitBlockSynchronization(block,
          newblock.generationStamp, newblock.len, closeFile, false,
          successList.toArray(new DatanodeID[successList.size()]));
      return newblock; // success
    }

    //failed
    StringBuilder b = new StringBuilder();
    for(BlockRecord r : syncList) {
      b.append("\n  " + r.id);
    }
    throw new IOException("Cannot recover " + block + ", none of these "
        + syncList.size() + " datanodes success {" + b + "\n}");  }
}
