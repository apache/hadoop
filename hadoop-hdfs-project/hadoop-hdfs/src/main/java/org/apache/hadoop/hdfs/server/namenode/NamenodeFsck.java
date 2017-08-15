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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.client.impl.BlockReaderFactory;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.RemotePeerFactory;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped.StorageAndBlockIndex;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicies;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.tracing.TraceUtils;
import org.apache.hadoop.util.Time;
import org.apache.htrace.core.Tracer;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class provides rudimentary checking of DFS volumes for errors and
 * sub-optimal conditions.
 * <p>The tool scans all files and directories, starting from an indicated
 *  root path. The following abnormal conditions are detected and handled:</p>
 * <ul>
 * <li>files with blocks that are completely missing from all datanodes.<br/>
 * In this case the tool can perform one of the following actions:
 *  <ul>
 *      <li>none ({@link #FIXING_NONE})</li>
 *      <li>move corrupted files to /lost+found directory on DFS
 *      ({@link #FIXING_MOVE}). Remaining data blocks are saved as a
 *      block chains, representing longest consecutive series of valid blocks.</li>
 *      <li>delete corrupted files ({@link #FIXING_DELETE})</li>
 *  </ul>
 *  </li>
 *  <li>detect files with under-replicated or over-replicated blocks</li>
 *  </ul>
 *  Additionally, the tool collects a detailed overall DFS statistics, and
 *  optionally can print detailed statistics on block locations and replication
 *  factors of each file.
 */
@InterfaceAudience.Private
public class NamenodeFsck implements DataEncryptionKeyFactory {
  public static final Log LOG = LogFactory.getLog(NameNode.class.getName());

  // return string marking fsck status
  public static final String CORRUPT_STATUS = "is CORRUPT";
  public static final String HEALTHY_STATUS = "is HEALTHY";
  public static final String DECOMMISSIONING_STATUS = "is DECOMMISSIONING";
  public static final String DECOMMISSIONED_STATUS = "is DECOMMISSIONED";
  public static final String ENTERING_MAINTENANCE_STATUS =
      "is ENTERING MAINTENANCE";
  public static final String IN_MAINTENANCE_STATUS = "is IN MAINTENANCE";
  public static final String NONEXISTENT_STATUS = "does not exist";
  public static final String FAILURE_STATUS = "FAILED";
  public static final String UNDEFINED = "undefined";

  private final NameNode namenode;
  private final BlockManager blockManager;
  private final NetworkTopology networktopology;
  private final int totalDatanodes;
  private final InetAddress remoteAddress;

  private long totalDirs = 0L;
  private long totalSymlinks = 0L;

  private String lostFound = null;
  private boolean lfInited = false;
  private boolean lfInitedOk = false;
  private boolean showFiles = false;
  private boolean showOpenFiles = false;
  private boolean showBlocks = false;
  private boolean showLocations = false;
  private boolean showRacks = false;
  private boolean showStoragePolcies = false;
  private boolean showprogress = false;
  private boolean showCorruptFileBlocks = false;

  private boolean showReplicaDetails = false;
  private boolean showUpgradeDomains = false;
  private boolean showMaintenanceState = false;
  private long staleInterval;
  private Tracer tracer;

  /**
   * True if we encountered an internal error during FSCK, such as not being
   * able to delete a corrupt file.
   */
  private boolean internalError = false;

  /**
   * True if the user specified the -move option.
   *
   * Whe this option is in effect, we will copy salvaged blocks into the lost
   * and found. */
  private boolean doMove = false;

  /**
   * True if the user specified the -delete option.
   *
   * Whe this option is in effect, we will delete corrupted files.
   */
  private boolean doDelete = false;

  String path = "/";

  private String blockIds = null;

  // We return back N files that are corrupt; the list of files returned is
  // ordered by block id; to allow continuation support, pass in the last block
  // # from previous call
  private final String[] currentCookie = new String[] { null };

  private final Configuration conf;
  private final PrintWriter out;
  private List<String> snapshottableDirs = null;

  private final BlockPlacementPolicies bpPolicies;
  private StoragePolicySummary storageTypeSummary = null;

  /**
   * Filesystem checker.
   * @param conf configuration (namenode config)
   * @param namenode namenode that this fsck is going to use
   * @param pmap key=value[] map passed to the http servlet as url parameters
   * @param out output stream to write the fsck output
   * @param totalDatanodes number of live datanodes
   * @param remoteAddress source address of the fsck request
   */
  NamenodeFsck(Configuration conf, NameNode namenode,
      NetworkTopology networktopology,
      Map<String,String[]> pmap, PrintWriter out,
      int totalDatanodes, InetAddress remoteAddress) {
    this.conf = conf;
    this.namenode = namenode;
    this.blockManager = namenode.getNamesystem().getBlockManager();
    this.networktopology = networktopology;
    this.out = out;
    this.totalDatanodes = totalDatanodes;
    this.remoteAddress = remoteAddress;
    this.bpPolicies = new BlockPlacementPolicies(conf, null,
        networktopology,
        namenode.getNamesystem().getBlockManager().getDatanodeManager()
        .getHost2DatanodeMap());
    this.staleInterval =
        conf.getLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
          DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);
    this.tracer = new Tracer.Builder("NamenodeFsck").
        conf(TraceUtils.wrapHadoopConf("namenode.fsck.htrace.", conf)).
        build();

    for (Iterator<String> it = pmap.keySet().iterator(); it.hasNext();) {
      String key = it.next();
      if (key.equals("path")) { this.path = pmap.get("path")[0]; }
      else if (key.equals("move")) { this.doMove = true; }
      else if (key.equals("delete")) { this.doDelete = true; }
      else if (key.equals("files")) { this.showFiles = true; }
      else if (key.equals("blocks")) { this.showBlocks = true; }
      else if (key.equals("locations")) { this.showLocations = true; }
      else if (key.equals("racks")) { this.showRacks = true; }
      else if (key.equals("replicadetails")) {
        this.showReplicaDetails = true;
      } else if (key.equals("upgradedomains")) {
        this.showUpgradeDomains = true;
      } else if (key.equals("maintenance")) {
        this.showMaintenanceState = true;
      } else if (key.equals("storagepolicies")) {
        this.showStoragePolcies = true;
      } else if (key.equals("showprogress")) {
        this.showprogress = true;
      } else if (key.equals("openforwrite")) {
        this.showOpenFiles = true;
      } else if (key.equals("listcorruptfileblocks")) {
        this.showCorruptFileBlocks = true;
      } else if (key.equals("startblockafter")) {
        this.currentCookie[0] = pmap.get("startblockafter")[0];
      } else if (key.equals("includeSnapshots")) {
        this.snapshottableDirs = new ArrayList<String>();
      } else if (key.equals("blockId")) {
        this.blockIds = pmap.get("blockId")[0];
      }
    }
  }

  /**
   * Check block information given a blockId number
   *
  */
  public void blockIdCK(String blockId) {

    if(blockId == null) {
      out.println("Please provide valid blockId!");
      return;
    }

    try {
      //get blockInfo
      Block block = new Block(Block.getBlockId(blockId));
      //find which file this block belongs to
      BlockInfo blockInfo = blockManager.getStoredBlock(block);
      if(blockInfo == null) {
        out.println("Block "+ blockId +" " + NONEXISTENT_STATUS);
        LOG.warn("Block "+ blockId + " " + NONEXISTENT_STATUS);
        return;
      }
      final INodeFile iNode = namenode.getNamesystem().getBlockCollection(blockInfo);
      NumberReplicas numberReplicas= blockManager.countNodes(blockInfo);
      out.println("Block Id: " + blockId);
      out.println("Block belongs to: "+iNode.getFullPathName());
      out.println("No. of Expected Replica: " +
          blockManager.getExpectedRedundancyNum(blockInfo));
      out.println("No. of live Replica: " + numberReplicas.liveReplicas());
      out.println("No. of excess Replica: " + numberReplicas.excessReplicas());
      out.println("No. of stale Replica: " +
          numberReplicas.replicasOnStaleNodes());
      out.println("No. of decommissioned Replica: "
          + numberReplicas.decommissioned());
      out.println("No. of decommissioning Replica: "
          + numberReplicas.decommissioning());
      if (this.showMaintenanceState) {
        out.println("No. of entering maintenance Replica: "
            + numberReplicas.liveEnteringMaintenanceReplicas());
        out.println("No. of in maintenance Replica: "
            + numberReplicas.maintenanceNotForReadReplicas());
      }
      out.println("No. of corrupted Replica: " +
          numberReplicas.corruptReplicas());
      //record datanodes that have corrupted block replica
      Collection<DatanodeDescriptor> corruptionRecord = null;
      if (blockManager.getCorruptReplicas(block) != null) {
        corruptionRecord = blockManager.getCorruptReplicas(block);
      }

      //report block replicas status on datanodes
      for(int idx = (blockInfo.numNodes()-1); idx >= 0; idx--) {
        DatanodeDescriptor dn = blockInfo.getDatanode(idx);
        out.print("Block replica on datanode/rack: " + dn.getHostName() +
            dn.getNetworkLocation() + " ");
        if (corruptionRecord != null && corruptionRecord.contains(dn)) {
          out.print(CORRUPT_STATUS + "\t ReasonCode: " +
              blockManager.getCorruptReason(block, dn));
        } else if (dn.isDecommissioned() ){
          out.print(DECOMMISSIONED_STATUS);
        } else if (dn.isDecommissionInProgress()) {
          out.print(DECOMMISSIONING_STATUS);
        } else if (this.showMaintenanceState && dn.isEnteringMaintenance()) {
          out.print(ENTERING_MAINTENANCE_STATUS);
        } else if (this.showMaintenanceState && dn.isInMaintenance()) {
          out.print(IN_MAINTENANCE_STATUS);
        } else {
          out.print(HEALTHY_STATUS);
        }
        out.print("\n");
      }
    } catch (Exception e){
      String errMsg = "Fsck on blockId '" + blockId;
      LOG.warn(errMsg, e);
      out.println(e.getMessage());
      out.print("\n\n" + errMsg);
      LOG.warn("Error in looking up block", e);
    }
  }

  /**
   * Check files on DFS, starting from the indicated path.
   */
  public void fsck() {
    final long startTime = Time.monotonicNow();
    try {
      if(blockIds != null) {
        String[] blocks = blockIds.split(" ");
        StringBuilder sb = new StringBuilder();
        sb.append("FSCK started by " +
            UserGroupInformation.getCurrentUser() + " from " +
            remoteAddress + " at " + new Date());
        out.println(sb);
        sb.append(" for blockIds: \n");
        for (String blk: blocks) {
          if(blk == null || !blk.contains(Block.BLOCK_FILE_PREFIX)) {
            out.println("Incorrect blockId format: " + blk);
            continue;
          }
          out.print("\n");
          blockIdCK(blk);
          sb.append(blk + "\n");
        }
        LOG.info(sb);
        namenode.getNamesystem().logFsckEvent("/", remoteAddress);
        out.flush();
        return;
      }

      String msg = "FSCK started by " + UserGroupInformation.getCurrentUser()
          + " from " + remoteAddress + " for path " + path + " at " + new Date();
      LOG.info(msg);
      out.println(msg);
      namenode.getNamesystem().logFsckEvent(path, remoteAddress);

      if (snapshottableDirs != null) {
        SnapshottableDirectoryStatus[] snapshotDirs =
            namenode.getRpcServer().getSnapshottableDirListing();
        if (snapshotDirs != null) {
          for (SnapshottableDirectoryStatus dir : snapshotDirs) {
            snapshottableDirs.add(dir.getFullPath().toString());
          }
        }
      }

      final HdfsFileStatus file = namenode.getRpcServer().getFileInfo(path);
      if (file != null) {

        if (showCorruptFileBlocks) {
          listCorruptFileBlocks();
          return;
        }

        if (this.showStoragePolcies) {
          storageTypeSummary = new StoragePolicySummary(
              namenode.getNamesystem().getBlockManager().getStoragePolicies());
        }

        Result replRes = new ReplicationResult(conf);
        Result ecRes = new ErasureCodingResult(conf);

        check(path, file, replRes, ecRes);

        out.print("\nStatus: ");
        out.println(replRes.isHealthy() && ecRes.isHealthy() ? "HEALTHY" : "CORRUPT");
        out.println(" Number of data-nodes:\t" + totalDatanodes);
        out.println(" Number of racks:\t\t" + networktopology.getNumOfRacks());
        out.println(" Total dirs:\t\t\t" + totalDirs);
        out.println(" Total symlinks:\t\t" + totalSymlinks);
        out.println("\nReplicated Blocks:");
        out.println(replRes);
        out.println("\nErasure Coded Block Groups:");
        out.println(ecRes);

        if (this.showStoragePolcies) {
          out.print(storageTypeSummary);
        }

        out.println("FSCK ended at " + new Date() + " in "
            + (Time.monotonicNow() - startTime + " milliseconds"));

        // If there were internal errors during the fsck operation, we want to
        // return FAILURE_STATUS, even if those errors were not immediately
        // fatal.  Otherwise many unit tests will pass even when there are bugs.
        if (internalError) {
          throw new IOException("fsck encountered internal errors!");
        }

        // DFSck client scans for the string HEALTHY/CORRUPT to check the status
        // of file system and return appropriate code. Changing the output
        // string might break testcases. Also note this must be the last line
        // of the report.
        if (replRes.isHealthy() && ecRes.isHealthy()) {
          out.print("\n\nThe filesystem under path '" + path + "' " + HEALTHY_STATUS);
        } else {
          out.print("\n\nThe filesystem under path '" + path + "' " + CORRUPT_STATUS);
        }

      } else {
        out.print("\n\nPath '" + path + "' " + NONEXISTENT_STATUS);
      }
    } catch (Exception e) {
      String errMsg = "Fsck on path '" + path + "' " + FAILURE_STATUS;
      LOG.warn(errMsg, e);
      out.println("FSCK ended at " + new Date() + " in "
          + (Time.monotonicNow() - startTime + " milliseconds"));
      out.println(e.getMessage());
      out.print("\n\n" + errMsg);
    } finally {
      out.close();
    }
  }

  private void listCorruptFileBlocks() throws IOException {
    final List<String> corrputBlocksFiles = namenode.getNamesystem()
        .listCorruptFileBlocksWithSnapshot(path, snapshottableDirs,
            currentCookie);
    int numCorruptFiles = corrputBlocksFiles.size();
    String filler;
    if (numCorruptFiles > 0) {
      filler = Integer.toString(numCorruptFiles);
    } else if (currentCookie[0].equals("0")) {
      filler = "no";
    } else {
      filler = "no more";
    }
    out.println("Cookie:\t" + currentCookie[0]);
    for (String s : corrputBlocksFiles) {
      out.println(s);
    }
    out.println("\n\nThe filesystem under path '" + path + "' has " + filler
        + " CORRUPT files");
    out.println();
  }

  @VisibleForTesting
  void check(String parent, HdfsFileStatus file, Result replRes, Result ecRes)
      throws IOException {
    String path = file.getFullName(parent);
    if (file.isDirectory()) {
      checkDir(path, replRes, ecRes);
      return;
    }
    if (file.isSymlink()) {
      if (showFiles) {
        out.println(path + " <symlink>");
      }
      totalSymlinks++;
      return;
    }
    LocatedBlocks blocks = getBlockLocations(path, file);
    if (blocks == null) { // the file is deleted
      return;
    }

    final Result r = file.getErasureCodingPolicy() != null ? ecRes: replRes;
    collectFileSummary(path, file, r, blocks);
    if (showprogress && (replRes.totalFiles + ecRes.totalFiles) % 100 == 0) {
      out.println();
      out.flush();
    }
    collectBlocksSummary(parent, file, r, blocks);
  }

  private void checkDir(String path, Result replRes, Result ecRes) throws IOException {
    if (snapshottableDirs != null && snapshottableDirs.contains(path)) {
      String snapshotPath = (path.endsWith(Path.SEPARATOR) ? path : path
          + Path.SEPARATOR)
          + HdfsConstants.DOT_SNAPSHOT_DIR;
      HdfsFileStatus snapshotFileInfo = namenode.getRpcServer().getFileInfo(
          snapshotPath);
      check(snapshotPath, snapshotFileInfo, replRes, ecRes);
    }
    byte[] lastReturnedName = HdfsFileStatus.EMPTY_NAME;
    DirectoryListing thisListing;
    if (showFiles) {
      out.println(path + " <dir>");
    }
    totalDirs++;
    do {
      assert lastReturnedName != null;
      thisListing = namenode.getRpcServer().getListing(
          path, lastReturnedName, false);
      if (thisListing == null) {
        return;
      }
      HdfsFileStatus[] files = thisListing.getPartialListing();
      for (int i = 0; i < files.length; i++) {
        check(path, files[i], replRes, ecRes);
      }
      lastReturnedName = thisListing.getLastName();
    } while (thisListing.hasMore());
  }

  private LocatedBlocks getBlockLocations(String path, HdfsFileStatus file)
      throws IOException {
    long fileLen = file.getLen();
    LocatedBlocks blocks = null;
    final FSNamesystem fsn = namenode.getNamesystem();
    fsn.readLock();
    try {
      blocks = FSDirStatAndListingOp.getBlockLocations(
          fsn.getFSDirectory(), fsn.getPermissionChecker(),
          path, 0, fileLen, false)
          .blocks;
    } catch (FileNotFoundException fnfe) {
      blocks = null;
    } finally {
      fsn.readUnlock("fsckGetBlockLocations");
    }
    return blocks;
  }

  private void collectFileSummary(String path, HdfsFileStatus file, Result res,
      LocatedBlocks blocks) throws IOException {
    long fileLen = file.getLen();
    boolean isOpen = blocks.isUnderConstruction();
    if (isOpen && !showOpenFiles) {
      // We collect these stats about open files to report with default options
      res.totalOpenFilesSize += fileLen;
      res.totalOpenFilesBlocks += blocks.locatedBlockCount();
      res.totalOpenFiles++;
      return;
    }
    res.totalFiles++;
    res.totalSize += fileLen;
    res.totalBlocks += blocks.locatedBlockCount();
    String redundancyPolicy;
    ErasureCodingPolicy ecPolicy = file.getErasureCodingPolicy();
    if (ecPolicy == null) { // a replicated file
      redundancyPolicy = "replicated: replication=" +
          file.getReplication() + ",";
    } else {
      redundancyPolicy = "erasure-coded: policy=" + ecPolicy.getName() + ",";
    }

    if (showOpenFiles && isOpen) {
      out.print(path + " " + fileLen + " bytes, " + redundancyPolicy + " " +
        blocks.locatedBlockCount() + " block(s), OPENFORWRITE: ");
    } else if (showFiles) {
      out.print(path + " " + fileLen + " bytes, " + redundancyPolicy + " " +
        blocks.locatedBlockCount() + " block(s): ");
    } else if (showprogress) {
      out.print('.');
    }
  }

  /**
   * Display info of each replica for replication block.
   * For striped block group, display info of each internal block.
   */
  private String getReplicaInfo(BlockInfo storedBlock) {
    if (!(showLocations || showRacks || showReplicaDetails ||
        showUpgradeDomains)) {
      return "";
    }
    final boolean isComplete = storedBlock.isComplete();
    Iterator<DatanodeStorageInfo> storagesItr;
    StringBuilder sb = new StringBuilder(" [");
    final boolean isStriped = storedBlock.isStriped();
    Map<DatanodeStorageInfo, Long> storage2Id = new HashMap<>();
    if (isComplete) {
      if (isStriped) {
        long blockId = storedBlock.getBlockId();
        Iterable<StorageAndBlockIndex> sis =
            ((BlockInfoStriped) storedBlock).getStorageAndIndexInfos();
        for (StorageAndBlockIndex si : sis) {
          storage2Id.put(si.getStorage(), blockId + si.getBlockIndex());
        }
      }
      storagesItr = storedBlock.getStorageInfos();
    } else {
      storagesItr = storedBlock.getUnderConstructionFeature()
          .getExpectedStorageLocationsIterator();
    }

    while (storagesItr.hasNext()) {
      DatanodeStorageInfo storage = storagesItr.next();
      if (isStriped && isComplete) {
        long index = storage2Id.get(storage);
        sb.append("blk_" + index + ":");
      }
      DatanodeDescriptor dnDesc = storage.getDatanodeDescriptor();
      if (showRacks) {
        sb.append(NodeBase.getPath(dnDesc));
      } else {
        sb.append(new DatanodeInfoWithStorage(dnDesc, storage.getStorageID(),
            storage.getStorageType()));
      }
      if (showUpgradeDomains) {
        String upgradeDomain = (dnDesc.getUpgradeDomain() != null) ?
            dnDesc.getUpgradeDomain() : UNDEFINED;
        sb.append("(ud=" + upgradeDomain +")");
      }
      if (showReplicaDetails) {
        Collection<DatanodeDescriptor> corruptReplicas =
            blockManager.getCorruptReplicas(storedBlock);
        sb.append("(");
        if (dnDesc.isDecommissioned()) {
          sb.append("DECOMMISSIONED)");
        } else if (dnDesc.isDecommissionInProgress()) {
          sb.append("DECOMMISSIONING)");
        } else if (this.showMaintenanceState &&
            dnDesc.isEnteringMaintenance()) {
          sb.append("ENTERING MAINTENANCE)");
        } else if (this.showMaintenanceState &&
            dnDesc.isInMaintenance()) {
          sb.append("IN MAINTENANCE)");
        } else if (corruptReplicas != null
            && corruptReplicas.contains(dnDesc)) {
          sb.append("CORRUPT)");
        } else if (blockManager.isExcess(dnDesc, storedBlock)) {
          sb.append("EXCESS)");
        } else if (dnDesc.isStale(this.staleInterval)) {
          sb.append("STALE_NODE)");
        } else if (storage.areBlockContentsStale()) {
          sb.append("STALE_BLOCK_CONTENT)");
        } else {
          sb.append("LIVE)");
        }
      }
      if (storagesItr.hasNext()) {
        sb.append(", ");
      }
    }
    sb.append(']');
    return sb.toString();
  }

  private void collectBlocksSummary(String parent, HdfsFileStatus file,
      Result res, LocatedBlocks blocks) throws IOException {
    String path = file.getFullName(parent);
    boolean isOpen = blocks.isUnderConstruction();
    if (isOpen && !showOpenFiles) {
      return;
    }
    int missing = 0;
    int corrupt = 0;
    long missize = 0;
    long corruptSize = 0;
    int underReplicatedPerFile = 0;
    int misReplicatedPerFile = 0;
    StringBuilder report = new StringBuilder();
    int blockNumber = 0;
    final LocatedBlock lastBlock = blocks.getLastLocatedBlock();
    for (LocatedBlock lBlk : blocks.getLocatedBlocks()) {
      ExtendedBlock block = lBlk.getBlock();
      if (!blocks.isLastBlockComplete() && lastBlock != null &&
          lastBlock.getBlock().equals(block)) {
        // this is the last block and this is not complete. ignore it since
        // it is under construction
        continue;
      }

      final BlockInfo storedBlock = blockManager.getStoredBlock(
          block.getLocalBlock());
      final int minReplication = blockManager.getMinStorageNum(storedBlock);
      // count decommissionedReplicas / decommissioningReplicas
      NumberReplicas numberReplicas = blockManager.countNodes(storedBlock);
      int decommissionedReplicas = numberReplicas.decommissioned();
      int decommissioningReplicas = numberReplicas.decommissioning();
      int enteringMaintenanceReplicas =
          numberReplicas.liveEnteringMaintenanceReplicas();
      int inMaintenanceReplicas =
          numberReplicas.maintenanceNotForReadReplicas();
      res.decommissionedReplicas +=  decommissionedReplicas;
      res.decommissioningReplicas += decommissioningReplicas;
      res.enteringMaintenanceReplicas += enteringMaintenanceReplicas;
      res.inMaintenanceReplicas += inMaintenanceReplicas;

      // count total replicas
      int liveReplicas = numberReplicas.liveReplicas();
      int totalReplicasPerBlock = liveReplicas + decommissionedReplicas
          + decommissioningReplicas
          + enteringMaintenanceReplicas
          + inMaintenanceReplicas;
      res.totalReplicas += totalReplicasPerBlock;

      boolean isMissing;
      if (storedBlock.isStriped()) {
        isMissing = totalReplicasPerBlock < minReplication;
      } else {
        isMissing = totalReplicasPerBlock == 0;
      }

      // count expected replicas
      short targetFileReplication;
      if (file.getErasureCodingPolicy() != null) {
        assert storedBlock instanceof BlockInfoStriped;
        targetFileReplication = ((BlockInfoStriped) storedBlock)
            .getRealTotalBlockNum();
      } else {
        targetFileReplication = file.getReplication();
      }
      res.numExpectedReplicas += targetFileReplication;

      // count under min repl'd blocks
      if(totalReplicasPerBlock < minReplication){
        res.numUnderMinReplicatedBlocks++;
      }

      // count excessive Replicas / over replicated blocks
      if (liveReplicas > targetFileReplication) {
        res.excessiveReplicas += (liveReplicas - targetFileReplication);
        res.numOverReplicatedBlocks += 1;
      }

      // count corrupt blocks
      boolean isCorrupt = lBlk.isCorrupt();
      if (isCorrupt) {
        res.addCorrupt(block.getNumBytes());
        corrupt++;
        corruptSize += block.getNumBytes();
        out.print("\n" + path + ": CORRUPT blockpool " +
            block.getBlockPoolId() + " block " + block.getBlockName() + "\n");
      }

      // count minimally replicated blocks
      if (totalReplicasPerBlock >= minReplication)
        res.numMinReplicatedBlocks++;

      // count missing replicas / under replicated blocks
      if (totalReplicasPerBlock < targetFileReplication && !isMissing) {
        res.missingReplicas += (targetFileReplication - totalReplicasPerBlock);
        res.numUnderReplicatedBlocks += 1;
        underReplicatedPerFile++;
        if (!showFiles) {
          out.print("\n" + path + ": ");
        }
        out.println(" Under replicated " + block + ". Target Replicas is "
            + targetFileReplication + " but found "
            + liveReplicas+ " live replica(s), "
            + decommissionedReplicas + " decommissioned replica(s), "
            + decommissioningReplicas + " decommissioning replica(s)"
            + (this.showMaintenanceState ? (enteringMaintenanceReplicas
            + ", entering maintenance replica(s) and " + inMaintenanceReplicas
            + " in maintenance replica(s).") : "."));
      }

      // count mis replicated blocks
      BlockPlacementStatus blockPlacementStatus = bpPolicies.getPolicy(
          lBlk.getBlockType()).verifyBlockPlacement(lBlk.getLocations(),
          targetFileReplication);
      if (!blockPlacementStatus.isPlacementPolicySatisfied()) {
        res.numMisReplicatedBlocks++;
        misReplicatedPerFile++;
        if (!showFiles) {
          if(underReplicatedPerFile == 0)
            out.println();
          out.print(path + ": ");
        }
        out.println(" Replica placement policy is violated for " +
                    block + ". " + blockPlacementStatus.getErrorDescription());
      }

      // count storage summary
      if (this.showStoragePolcies && lBlk.getStorageTypes() != null) {
        countStorageTypeSummary(file, lBlk);
      }

      // report
      String blkName = block.toString();
      report.append(blockNumber + ". " + blkName + " len=" +
          block.getNumBytes());
      if (isMissing && !isCorrupt) {
        // If the block is corrupted, it means all its available replicas are
        // corrupted in the case of replication, and it means the state of the
        // block group is unrecoverable due to some corrupted intenal blocks in
        // the case of EC. We don't mark it as missing given these available
        // replicas/internal-blocks might still be accessible as the block might
        // be incorrectly marked as corrupted by client machines.
        report.append(" MISSING!");
        res.addMissing(blkName, block.getNumBytes());
        missing++;
        missize += block.getNumBytes();
        if (storedBlock.isStriped()) {
          report.append(" Live_repl=" + liveReplicas);
          String info = getReplicaInfo(storedBlock);
          if (!info.isEmpty()){
            report.append(" ").append(info);
          }
        }
      } else {
        report.append(" Live_repl=" + liveReplicas);
        String info = getReplicaInfo(storedBlock);
        if (!info.isEmpty()){
          report.append(" ").append(info);
        }
      }
      report.append('\n');
      blockNumber++;
    }

    //display under construction block info.
    if (!blocks.isLastBlockComplete() && lastBlock != null) {
      ExtendedBlock block = lastBlock.getBlock();
      String blkName = block.toString();
      BlockInfo storedBlock = blockManager.getStoredBlock(
          block.getLocalBlock());
      DatanodeStorageInfo[] storages = storedBlock
          .getUnderConstructionFeature().getExpectedStorageLocations();
      report.append('\n');
      report.append("Under Construction Block:\n");
      report.append(blockNumber).append(". ").append(blkName);
      report.append(" len=").append(block.getNumBytes());
      report.append(" Expected_repl=" + storages.length);
      String info=getReplicaInfo(storedBlock);
      if (!info.isEmpty()){
        report.append(" ").append(info);
      }
    }

    // count corrupt file & move or delete if necessary
    if ((missing > 0) || (corrupt > 0)) {
      if (!showFiles) {
        if (missing > 0) {
          out.print("\n" + path + ": MISSING " + missing
              + " blocks of total size " + missize + " B.");
        }
        if (corrupt > 0) {
          out.print("\n" + path + ": CORRUPT " + corrupt
              + " blocks of total size " + corruptSize + " B.");
        }
      }
      res.corruptFiles++;
      if (isOpen) {
        LOG.info("Fsck: ignoring open file " + path);
      } else {
        if (doMove) copyBlocksToLostFound(parent, file, blocks);
        if (doDelete) deleteCorruptedFile(path);
      }
    }

    if (showFiles) {
      if (missing > 0 || corrupt > 0) {
        if (missing > 0) {
          out.print(" MISSING " + missing + " blocks of total size " +
              missize + " B\n");
        }
        if (corrupt > 0) {
          out.print(" CORRUPT " + corrupt + " blocks of total size " +
              corruptSize + " B\n");
        }
      } else if (underReplicatedPerFile == 0 && misReplicatedPerFile == 0) {
        out.print(" OK\n");
      }
      if (showBlocks) {
        out.print(report + "\n");
      }
    }
  }

  private void countStorageTypeSummary(HdfsFileStatus file, LocatedBlock lBlk) {
    StorageType[] storageTypes = lBlk.getStorageTypes();
    storageTypeSummary.add(Arrays.copyOf(storageTypes, storageTypes.length),
        namenode.getNamesystem().getBlockManager()
        .getStoragePolicy(file.getStoragePolicy()));
  }

  private void deleteCorruptedFile(String path) {
    try {
      namenode.getRpcServer().delete(path, true);
      LOG.info("Fsck: deleted corrupt file " + path);
    } catch (Exception e) {
      LOG.error("Fsck: error deleting corrupted file " + path, e);
      internalError = true;
    }
  }

  boolean hdfsPathExists(String path)
      throws AccessControlException, UnresolvedLinkException, IOException {
    try {
      HdfsFileStatus hfs = namenode.getRpcServer().getFileInfo(path);
      return (hfs != null);
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  private void copyBlocksToLostFound(String parent, HdfsFileStatus file,
        LocatedBlocks blocks) throws IOException {
    final DFSClient dfs = new DFSClient(DFSUtilClient.getNNAddress(conf), conf);
    final String fullName = file.getFullName(parent);
    OutputStream fos = null;
    try {
      if (!lfInited) {
        lostFoundInit(dfs);
      }
      if (!lfInitedOk) {
        throw new IOException("failed to initialize lost+found");
      }
      String target = lostFound + fullName;
      if (hdfsPathExists(target)) {
        LOG.warn("Fsck: can't copy the remains of " + fullName + " to " +
          "lost+found, because " + target + " already exists.");
        return;
      }
      if (!namenode.getRpcServer().mkdirs(
          target, file.getPermission(), true)) {
        throw new IOException("failed to create directory " + target);
      }
      // create chains
      int chain = 0;
      boolean copyError = false;
      for (LocatedBlock lBlk : blocks.getLocatedBlocks()) {
        LocatedBlock lblock = lBlk;
        DatanodeInfo[] locs = lblock.getLocations();
        if (locs == null || locs.length == 0) {
          if (fos != null) {
            fos.flush();
            fos.close();
            fos = null;
          }
          continue;
        }
        if (fos == null) {
          fos = dfs.create(target + "/" + chain, true);
          chain++;
        }

        // copy the block. It's a pity it's not abstracted from DFSInputStream ...
        try {
          copyBlock(dfs, lblock, fos);
        } catch (Exception e) {
          LOG.error("Fsck: could not copy block " + lblock.getBlock() +
              " to " + target, e);
          fos.flush();
          fos.close();
          fos = null;
          internalError = true;
          copyError = true;
        }
      }
      if (copyError) {
        LOG.warn("Fsck: there were errors copying the remains of the " +
          "corrupted file " + fullName + " to /lost+found");
      } else {
        LOG.info("Fsck: copied the remains of the corrupted file " +
          fullName + " to /lost+found");
      }
    } catch (Exception e) {
      LOG.error("copyBlocksToLostFound: error processing " + fullName, e);
      internalError = true;
    } finally {
      if (fos != null) fos.close();
      dfs.close();
    }
  }

  /*
   * XXX (ab) Bulk of this method is copied verbatim from {@link DFSClient}, which is
   * bad. Both places should be refactored to provide a method to copy blocks
   * around.
   */
  private void copyBlock(final DFSClient dfs, LocatedBlock lblock,
                         OutputStream fos) throws Exception {
    int failures = 0;
    InetSocketAddress targetAddr = null;
    TreeSet<DatanodeInfo> deadNodes = new TreeSet<DatanodeInfo>();
    BlockReader blockReader = null;
    ExtendedBlock block = lblock.getBlock();

    while (blockReader == null) {
      DatanodeInfo chosenNode;

      try {
        chosenNode = bestNode(dfs, lblock.getLocations(), deadNodes);
        targetAddr = NetUtils.createSocketAddr(chosenNode.getXferAddr());
      }  catch (IOException ie) {
        if (failures >= HdfsClientConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT) {
          throw new IOException("Could not obtain block " + lblock, ie);
        }
        LOG.info("Could not obtain block from any node:  " + ie);
        try {
          Thread.sleep(10000);
        }  catch (InterruptedException iex) {
        }
        deadNodes.clear();
        failures++;
        continue;
      }
      try {
        String file = BlockReaderFactory.getFileName(targetAddr,
            block.getBlockPoolId(), block.getBlockId());
        blockReader = new BlockReaderFactory(dfs.getConf()).
            setFileName(file).
            setBlock(block).
            setBlockToken(lblock.getBlockToken()).
            setStartOffset(0).
            setLength(block.getNumBytes()).
            setVerifyChecksum(true).
            setClientName("fsck").
            setDatanodeInfo(chosenNode).
            setInetSocketAddress(targetAddr).
            setCachingStrategy(CachingStrategy.newDropBehind()).
            setClientCacheContext(dfs.getClientContext()).
            setConfiguration(namenode.getConf()).
            setTracer(tracer).
            setRemotePeerFactory(new RemotePeerFactory() {
              @Override
              public Peer newConnectedPeer(InetSocketAddress addr,
                  Token<BlockTokenIdentifier> blockToken, DatanodeID datanodeId)
                  throws IOException {
                Peer peer = null;
                Socket s = NetUtils.getDefaultSocketFactory(conf).createSocket();
                try {
                  s.connect(addr, HdfsConstants.READ_TIMEOUT);
                  s.setSoTimeout(HdfsConstants.READ_TIMEOUT);
                  peer = DFSUtilClient.peerFromSocketAndKey(
                        dfs.getSaslDataTransferClient(), s, NamenodeFsck.this,
                        blockToken, datanodeId, HdfsConstants.READ_TIMEOUT);
                } finally {
                  if (peer == null) {
                    IOUtils.closeQuietly(s);
                  }
                }
                return peer;
              }
            }).
            build();
      }  catch (IOException ex) {
        // Put chosen node into dead list, continue
        LOG.info("Failed to connect to " + targetAddr + ":" + ex);
        deadNodes.add(chosenNode);
      }
    }
    byte[] buf = new byte[1024];
    int cnt = 0;
    boolean success = true;
    long bytesRead = 0;
    try {
      while ((cnt = blockReader.read(buf, 0, buf.length)) > 0) {
        fos.write(buf, 0, cnt);
        bytesRead += cnt;
      }
      if ( bytesRead != block.getNumBytes() ) {
        throw new IOException("Recorded block size is " + block.getNumBytes() +
                              ", but datanode returned " +bytesRead+" bytes");
      }
    } catch (Exception e) {
      LOG.error("Error reading block", e);
      success = false;
    } finally {
      blockReader.close();
    }
    if (!success) {
      throw new Exception("Could not copy block data for " + lblock.getBlock());
    }
  }

  @Override
  public DataEncryptionKey newDataEncryptionKey() throws IOException {
    return namenode.getRpcServer().getDataEncryptionKey();
  }

  /*
   * XXX (ab) See comment above for copyBlock().
   *
   * Pick the best node from which to stream the data.
   * That's the local one, if available.
   */
  private DatanodeInfo bestNode(DFSClient dfs, DatanodeInfo[] nodes,
                                TreeSet<DatanodeInfo> deadNodes) throws IOException {
    if ((nodes == null) ||
        (nodes.length - deadNodes.size() < 1)) {
      throw new IOException("No live nodes contain current block");
    }
    DatanodeInfo chosenNode;
    do {
      chosenNode = nodes[ThreadLocalRandom.current().nextInt(nodes.length)];
    } while (deadNodes.contains(chosenNode));
    return chosenNode;
  }

  private void lostFoundInit(DFSClient dfs) {
    lfInited = true;
    try {
      String lfName = "/lost+found";

      final HdfsFileStatus lfStatus = dfs.getFileInfo(lfName);
      if (lfStatus == null) { // not exists
        lfInitedOk = dfs.mkdirs(lfName, null, true);
        lostFound = lfName;
      } else if (!lfStatus.isDirectory()) { // exists but not a directory
        LOG.warn("Cannot use /lost+found : a regular file with this name exists.");
        lfInitedOk = false;
      }  else { // exists and is a directory
        lostFound = lfName;
        lfInitedOk = true;
      }
    }  catch (Exception e) {
      e.printStackTrace();
      lfInitedOk = false;
    }
    if (lostFound == null) {
      LOG.warn("Cannot initialize /lost+found .");
      lfInitedOk = false;
      internalError = true;
    }
  }

  /**
   * FsckResult of checking, plus overall DFS statistics.
   */
  @VisibleForTesting
  static class Result {
    final List<String> missingIds = new ArrayList<String>();
    long missingSize = 0L;
    long corruptFiles = 0L;
    long corruptBlocks = 0L;
    long corruptSize = 0L;
    long excessiveReplicas = 0L;
    long missingReplicas = 0L;
    long decommissionedReplicas = 0L;
    long decommissioningReplicas = 0L;
    long enteringMaintenanceReplicas = 0L;
    long inMaintenanceReplicas = 0L;
    long numUnderMinReplicatedBlocks = 0L;
    long numOverReplicatedBlocks = 0L;
    long numUnderReplicatedBlocks = 0L;
    long numMisReplicatedBlocks = 0L;  // blocks that do not satisfy block placement policy
    long numMinReplicatedBlocks = 0L;  // minimally replicatedblocks
    long totalBlocks = 0L;
    long numExpectedReplicas = 0L;
    long totalOpenFilesBlocks = 0L;
    long totalFiles = 0L;
    long totalOpenFiles = 0L;
    long totalSize = 0L;
    long totalOpenFilesSize = 0L;
    long totalReplicas = 0L;

    /**
     * DFS is considered healthy if there are no missing blocks.
     */
    boolean isHealthy() {
      return ((missingIds.size() == 0) && (corruptBlocks == 0));
    }

    /** Add a missing block name, plus its size. */
    void addMissing(String id, long size) {
      missingIds.add(id);
      missingSize += size;
    }

    /** Add a corrupt block. */
    void addCorrupt(long size) {
      corruptBlocks++;
      corruptSize += size;
    }

    /** Return the actual replication factor. */
    float getReplicationFactor() {
      if (totalBlocks == 0)
        return 0.0f;
      return (float) (totalReplicas) / (float) totalBlocks;
    }
  }

  @VisibleForTesting
  static class ReplicationResult extends Result {
    final short replication;
    final short minReplication;

    ReplicationResult(Configuration conf) {
      this.replication = (short)conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY,
                                            DFSConfigKeys.DFS_REPLICATION_DEFAULT);
      this.minReplication = (short)conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY,
                                            DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_DEFAULT);
    }

    @Override
    public String toString() {
      StringBuilder res = new StringBuilder();
      res.append(" Total size:\t").append(totalSize).append(" B");
      if (totalOpenFilesSize != 0) {
        res.append(" (Total open files size: ").append(totalOpenFilesSize)
            .append(" B)");
      }
      res.append("\n Total files:\t").append(totalFiles);
      if (totalOpenFiles != 0) {
        res.append(" (Files currently being written: ").append(totalOpenFiles)
            .append(")");
      }
      res.append("\n Total blocks (validated):\t").append(totalBlocks);
      if (totalBlocks > 0) {
        res.append(" (avg. block size ").append((totalSize / totalBlocks))
            .append(" B)");
      }
      if (totalOpenFilesBlocks != 0) {
        res.append(" (Total open file blocks (not validated): ").append(
            totalOpenFilesBlocks).append(")");
      }
      if (corruptFiles > 0 || numUnderMinReplicatedBlocks > 0) {
        res.append("\n  ********************************");
        if(numUnderMinReplicatedBlocks>0){
          res.append("\n  UNDER MIN REPL'D BLOCKS:\t").append(numUnderMinReplicatedBlocks);
          if(totalBlocks>0){
            res.append(" (").append(
                ((float) (numUnderMinReplicatedBlocks * 100) / (float) totalBlocks))
                .append(" %)");
          }
          res.append("\n  ").append(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY + ":\t")
             .append(minReplication);
        }
        if(corruptFiles>0) {
          res.append(
              "\n  CORRUPT FILES:\t").append(corruptFiles);
          if (missingSize > 0) {
            res.append("\n  MISSING BLOCKS:\t").append(missingIds.size()).append(
                "\n  MISSING SIZE:\t\t").append(missingSize).append(" B");
          }
          if (corruptBlocks > 0) {
            res.append("\n  CORRUPT BLOCKS: \t").append(corruptBlocks).append(
                "\n  CORRUPT SIZE:\t\t").append(corruptSize).append(" B");
          }
        }
        res.append("\n  ********************************");
      }
      res.append("\n Minimally replicated blocks:\t").append(
          numMinReplicatedBlocks);
      if (totalBlocks > 0) {
        res.append(" (").append(
            ((float) (numMinReplicatedBlocks * 100) / (float) totalBlocks))
            .append(" %)");
      }
      res.append("\n Over-replicated blocks:\t")
          .append(numOverReplicatedBlocks);
      if (totalBlocks > 0) {
        res.append(" (").append(
            ((float) (numOverReplicatedBlocks * 100) / (float) totalBlocks))
            .append(" %)");
      }
      res.append("\n Under-replicated blocks:\t").append(
          numUnderReplicatedBlocks);
      if (totalBlocks > 0) {
        res.append(" (").append(
            ((float) (numUnderReplicatedBlocks * 100) / (float) totalBlocks))
            .append(" %)");
      }
      res.append("\n Mis-replicated blocks:\t\t")
          .append(numMisReplicatedBlocks);
      if (totalBlocks > 0) {
        res.append(" (").append(
            ((float) (numMisReplicatedBlocks * 100) / (float) totalBlocks))
            .append(" %)");
      }
      res.append("\n Default replication factor:\t").append(replication)
          .append("\n Average block replication:\t").append(
              getReplicationFactor()).append("\n Missing blocks:\t\t").append(
              missingIds.size()).append("\n Corrupt blocks:\t\t").append(
              corruptBlocks).append("\n Missing replicas:\t\t").append(
              missingReplicas);
      if (totalReplicas > 0) {
        res.append(" (").append(
            ((float) (missingReplicas * 100) / (float) numExpectedReplicas)).append(
            " %)");
      }
      if (decommissionedReplicas > 0) {
        res.append("\n DecommissionedReplicas:\t").append(
            decommissionedReplicas);
      }
      if (decommissioningReplicas > 0) {
        res.append("\n DecommissioningReplicas:\t").append(
            decommissioningReplicas);
      }
      if (enteringMaintenanceReplicas > 0) {
        res.append("\n EnteringMaintenanceReplicas:\t").append(
            enteringMaintenanceReplicas);
      }
      if (inMaintenanceReplicas > 0) {
        res.append("\n InMaintenanceReplicas:\t").append(
            inMaintenanceReplicas);
      }
      return res.toString();
    }
  }

  @VisibleForTesting
  static class ErasureCodingResult extends Result {

    ErasureCodingResult(Configuration conf) {
    }

    @Override
    public String toString() {
      StringBuilder res = new StringBuilder();
      res.append(" Total size:\t").append(totalSize).append(" B");
      if (totalOpenFilesSize != 0) {
        res.append(" (Total open files size: ").append(totalOpenFilesSize)
            .append(" B)");
      }
      res.append("\n Total files:\t").append(totalFiles);
      if (totalOpenFiles != 0) {
        res.append(" (Files currently being written: ").append(totalOpenFiles)
            .append(")");
      }
      res.append("\n Total block groups (validated):\t").append(totalBlocks);
      if (totalBlocks > 0) {
        res.append(" (avg. block group size ").append((totalSize / totalBlocks))
            .append(" B)");
      }
      if (totalOpenFilesBlocks != 0) {
        res.append(" (Total open file block groups (not validated): ").append(
            totalOpenFilesBlocks).append(")");
      }
      if (corruptFiles > 0 || numUnderMinReplicatedBlocks > 0) {
        res.append("\n  ********************************");
        if(numUnderMinReplicatedBlocks>0){
          res.append("\n  UNRECOVERABLE BLOCK GROUPS:\t").append(numUnderMinReplicatedBlocks);
          if(totalBlocks>0){
            res.append(" (").append(
                ((float) (numUnderMinReplicatedBlocks * 100) / (float) totalBlocks))
                .append(" %)");
          }
        }
        if(corruptFiles>0) {
          res.append(
              "\n  CORRUPT FILES:\t").append(corruptFiles);
          if (missingSize > 0) {
            res.append("\n  MISSING BLOCK GROUPS:\t").append(missingIds.size()).append(
                "\n  MISSING SIZE:\t\t").append(missingSize).append(" B");
          }
          if (corruptBlocks > 0) {
            res.append("\n  CORRUPT BLOCK GROUPS: \t").append(corruptBlocks).append(
                "\n  CORRUPT SIZE:\t\t").append(corruptSize).append(" B");
          }
        }
        res.append("\n  ********************************");
      }
      res.append("\n Minimally erasure-coded block groups:\t").append(
          numMinReplicatedBlocks);
      if (totalBlocks > 0) {
        res.append(" (").append(
            ((float) (numMinReplicatedBlocks * 100) / (float) totalBlocks))
            .append(" %)");
      }
      res.append("\n Over-erasure-coded block groups:\t")
          .append(numOverReplicatedBlocks);
      if (totalBlocks > 0) {
        res.append(" (").append(
            ((float) (numOverReplicatedBlocks * 100) / (float) totalBlocks))
            .append(" %)");
      }
      res.append("\n Under-erasure-coded block groups:\t").append(
          numUnderReplicatedBlocks);
      if (totalBlocks > 0) {
        res.append(" (").append(
            ((float) (numUnderReplicatedBlocks * 100) / (float) totalBlocks))
            .append(" %)");
      }
      res.append("\n Unsatisfactory placement block groups:\t")
          .append(numMisReplicatedBlocks);
      if (totalBlocks > 0) {
        res.append(" (").append(
            ((float) (numMisReplicatedBlocks * 100) / (float) totalBlocks))
            .append(" %)");
      }
      res.append("\n Average block group size:\t").append(
          getReplicationFactor()).append("\n Missing block groups:\t\t").append(
          missingIds.size()).append("\n Corrupt block groups:\t\t").append(
          corruptBlocks).append("\n Missing internal blocks:\t").append(
          missingReplicas);
      if (totalReplicas > 0) {
        res.append(" (").append(
            ((float) (missingReplicas * 100) / (float) numExpectedReplicas)).append(
            " %)");
      }
      if (decommissionedReplicas > 0) {
        res.append("\n Decommissioned internal blocks:\t").append(
            decommissionedReplicas);
      }
      if (decommissioningReplicas > 0) {
        res.append("\n Decommissioning internal blocks:\t").append(
            decommissioningReplicas);
      }
      if (enteringMaintenanceReplicas > 0) {
        res.append("\n EnteringMaintenanceReplicas:\t").append(
            enteringMaintenanceReplicas);
      }
      if (inMaintenanceReplicas > 0) {
        res.append("\n InMaintenanceReplicas:\t").append(
            inMaintenanceReplicas);
      }
      return res.toString();
    }
  }
}
