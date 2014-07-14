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

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.BlockReaderFactory;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.RemotePeerFactory;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.net.TcpPeerServer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;

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
  public static final String NONEXISTENT_STATUS = "does not exist";
  public static final String FAILURE_STATUS = "FAILED";
  
  private final NameNode namenode;
  private final NetworkTopology networktopology;
  private final int totalDatanodes;
  private final short minReplication;
  private final InetAddress remoteAddress;

  private String lostFound = null;
  private boolean lfInited = false;
  private boolean lfInitedOk = false;
  private boolean showFiles = false;
  private boolean showOpenFiles = false;
  private boolean showBlocks = false;
  private boolean showLocations = false;
  private boolean showRacks = false;
  private boolean showCorruptFileBlocks = false;

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

  private String path = "/";

  // We return back N files that are corrupt; the list of files returned is
  // ordered by block id; to allow continuation support, pass in the last block
  // # from previous call
  private final String[] currentCookie = new String[] { null };

  private final Configuration conf;
  private final PrintWriter out;
  private List<String> snapshottableDirs = null;

  private final BlockPlacementPolicy bpPolicy;
  private final SaslDataTransferClient saslClient;

  /**
   * Filesystem checker.
   * @param conf configuration (namenode config)
   * @param namenode namenode that this fsck is going to use
   * @param pmap key=value[] map passed to the http servlet as url parameters
   * @param out output stream to write the fsck output
   * @param totalDatanodes number of live datanodes
   * @param minReplication minimum replication
   * @param remoteAddress source address of the fsck request
   */
  NamenodeFsck(Configuration conf, NameNode namenode,
      NetworkTopology networktopology, 
      Map<String,String[]> pmap, PrintWriter out,
      int totalDatanodes, short minReplication, InetAddress remoteAddress) {
    this.conf = conf;
    this.namenode = namenode;
    this.networktopology = networktopology;
    this.out = out;
    this.totalDatanodes = totalDatanodes;
    this.minReplication = minReplication;
    this.remoteAddress = remoteAddress;
    this.bpPolicy = BlockPlacementPolicy.getInstance(conf, null,
        networktopology,
        namenode.getNamesystem().getBlockManager().getDatanodeManager()
        .getHost2DatanodeMap());
    this.saslClient = new SaslDataTransferClient(
      DataTransferSaslUtil.getSaslPropertiesResolver(conf),
      TrustedChannelResolver.getInstance(conf),
      conf.getBoolean(
        IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY,
        IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT));
    
    for (Iterator<String> it = pmap.keySet().iterator(); it.hasNext();) {
      String key = it.next();
      if (key.equals("path")) { this.path = pmap.get("path")[0]; }
      else if (key.equals("move")) { this.doMove = true; }
      else if (key.equals("delete")) { this.doDelete = true; }
      else if (key.equals("files")) { this.showFiles = true; }
      else if (key.equals("blocks")) { this.showBlocks = true; }
      else if (key.equals("locations")) { this.showLocations = true; }
      else if (key.equals("racks")) { this.showRacks = true; }
      else if (key.equals("openforwrite")) {this.showOpenFiles = true; }
      else if (key.equals("listcorruptfileblocks")) {
        this.showCorruptFileBlocks = true;
      }
      else if (key.equals("startblockafter")) {
        this.currentCookie[0] = pmap.get("startblockafter")[0];
      } else if (key.equals("includeSnapshots")) {
        this.snapshottableDirs = new ArrayList<String>();
      }
    }
  }

  /**
   * Check files on DFS, starting from the indicated path.
   */
  public void fsck() {
    final long startTime = Time.now();
    try {
      String msg = "FSCK started by " + UserGroupInformation.getCurrentUser()
          + " from " + remoteAddress + " for path " + path + " at " + new Date();
      LOG.info(msg);
      out.println(msg);
      namenode.getNamesystem().logFsckEvent(path, remoteAddress);

      if (snapshottableDirs != null) {
        SnapshottableDirectoryStatus[] snapshotDirs = namenode.getRpcServer()
            .getSnapshottableDirListing();
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
        
        Result res = new Result(conf);

        check(path, file, res);

        out.println(res);
        out.println(" Number of data-nodes:\t\t" + totalDatanodes);
        out.println(" Number of racks:\t\t" + networktopology.getNumOfRacks());

        out.println("FSCK ended at " + new Date() + " in "
            + (Time.now() - startTime + " milliseconds"));

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
        if (res.isHealthy()) {
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
          + (Time.now() - startTime + " milliseconds"));
      out.println(e.getMessage());
      out.print("\n\n" + errMsg);
    } finally {
      out.close();
    }
  }

  private void listCorruptFileBlocks() throws IOException {
    Collection<FSNamesystem.CorruptFileBlockInfo> corruptFiles = namenode.
      getNamesystem().listCorruptFileBlocks(path, currentCookie);
    int numCorruptFiles = corruptFiles.size();
    String filler;
    if (numCorruptFiles > 0) {
      filler = Integer.toString(numCorruptFiles);
    } else if (currentCookie[0].equals("0")) {
      filler = "no";
    } else {
      filler = "no more";
    }
    out.println("Cookie:\t" + currentCookie[0]);
    for (FSNamesystem.CorruptFileBlockInfo c : corruptFiles) {
      out.println(c.toString());
    }
    out.println("\n\nThe filesystem under path '" + path + "' has " + filler
        + " CORRUPT files");
    out.println();
  }
  
  @VisibleForTesting
  void check(String parent, HdfsFileStatus file, Result res) throws IOException {
    String path = file.getFullName(parent);
    boolean isOpen = false;

    if (file.isDir()) {
      if (snapshottableDirs != null && snapshottableDirs.contains(path)) {
        String snapshotPath = (path.endsWith(Path.SEPARATOR) ? path : path
            + Path.SEPARATOR)
            + HdfsConstants.DOT_SNAPSHOT_DIR;
        HdfsFileStatus snapshotFileInfo = namenode.getRpcServer().getFileInfo(
            snapshotPath);
        check(snapshotPath, snapshotFileInfo, res);
      }
      byte[] lastReturnedName = HdfsFileStatus.EMPTY_NAME;
      DirectoryListing thisListing;
      if (showFiles) {
        out.println(path + " <dir>");
      }
      res.totalDirs++;
      do {
        assert lastReturnedName != null;
        thisListing = namenode.getRpcServer().getListing(
            path, lastReturnedName, false);
        if (thisListing == null) {
          return;
        }
        HdfsFileStatus[] files = thisListing.getPartialListing();
        for (int i = 0; i < files.length; i++) {
          check(path, files[i], res);
        }
        lastReturnedName = thisListing.getLastName();
      } while (thisListing.hasMore());
      return;
    }
    if (file.isSymlink()) {
      if (showFiles) {
        out.println(path + " <symlink>");
      }
      res.totalSymlinks++;
      return;
    }
    long fileLen = file.getLen();
    // Get block locations without updating the file access time 
    // and without block access tokens
    LocatedBlocks blocks;
    try {
      blocks = namenode.getNamesystem().getBlockLocations(path, 0,
          fileLen, false, false, false);
    } catch (FileNotFoundException fnfe) {
      blocks = null;
    }
    if (blocks == null) { // the file is deleted
      return;
    }
    isOpen = blocks.isUnderConstruction();
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
    if (showOpenFiles && isOpen) {
      out.print(path + " " + fileLen + " bytes, " +
        blocks.locatedBlockCount() + " block(s), OPENFORWRITE: ");
    } else if (showFiles) {
      out.print(path + " " + fileLen + " bytes, " +
        blocks.locatedBlockCount() + " block(s): ");
    } else {
      out.print('.');
    }
    if (res.totalFiles % 100 == 0) { out.println(); out.flush(); }
    int missing = 0;
    int corrupt = 0;
    long missize = 0;
    int underReplicatedPerFile = 0;
    int misReplicatedPerFile = 0;
    StringBuilder report = new StringBuilder();
    int i = 0;
    for (LocatedBlock lBlk : blocks.getLocatedBlocks()) {
      ExtendedBlock block = lBlk.getBlock();
      boolean isCorrupt = lBlk.isCorrupt();
      String blkName = block.toString();
      DatanodeInfo[] locs = lBlk.getLocations();
      NumberReplicas numberReplicas = namenode.getNamesystem().getBlockManager().countNodes(block.getLocalBlock());
      int liveReplicas = numberReplicas.liveReplicas();
      res.totalReplicas += liveReplicas;
      short targetFileReplication = file.getReplication();
      res.numExpectedReplicas += targetFileReplication;
      if (liveReplicas > targetFileReplication) {
        res.excessiveReplicas += (liveReplicas - targetFileReplication);
        res.numOverReplicatedBlocks += 1;
      }
      // Check if block is Corrupt
      if (isCorrupt) {
        corrupt++;
        res.corruptBlocks++;
        out.print("\n" + path + ": CORRUPT blockpool " + block.getBlockPoolId() + 
            " block " + block.getBlockName()+"\n");
      }
      if (liveReplicas >= minReplication)
        res.numMinReplicatedBlocks++;
      if (liveReplicas < targetFileReplication && liveReplicas > 0) {
        res.missingReplicas += (targetFileReplication - liveReplicas);
        res.numUnderReplicatedBlocks += 1;
        underReplicatedPerFile++;
        if (!showFiles) {
          out.print("\n" + path + ": ");
        }
        out.println(" Under replicated " + block +
                    ". Target Replicas is " +
                    targetFileReplication + " but found " +
                    liveReplicas + " replica(s).");
      }
      // verify block placement policy
      BlockPlacementStatus blockPlacementStatus = bpPolicy
          .verifyBlockPlacement(path, lBlk, targetFileReplication);
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
      report.append(i + ". " + blkName + " len=" + block.getNumBytes());
      if (liveReplicas == 0) {
        report.append(" MISSING!");
        res.addMissing(block.toString(), block.getNumBytes());
        missing++;
        missize += block.getNumBytes();
      } else {
        report.append(" repl=" + liveReplicas);
        if (showLocations || showRacks) {
          StringBuilder sb = new StringBuilder("[");
          for (int j = 0; j < locs.length; j++) {
            if (j > 0) { sb.append(", "); }
            if (showRacks)
              sb.append(NodeBase.getPath(locs[j]));
            else
              sb.append(locs[j]);
          }
          sb.append(']');
          report.append(" " + sb.toString());
        }
      }
      report.append('\n');
      i++;
    }
    if ((missing > 0) || (corrupt > 0)) {
      if (!showFiles && (missing > 0)) {
        out.print("\n" + path + ": MISSING " + missing
            + " blocks of total size " + missize + " B.");
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
      if (missing > 0) {
        out.print(" MISSING " + missing + " blocks of total size " + missize + " B\n");
      }  else if (underReplicatedPerFile == 0 && misReplicatedPerFile == 0) {
        out.print(" OK\n");
      }
      if (showBlocks) {
        out.print(report.toString() + "\n");
      }
    }
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
    final DFSClient dfs = new DFSClient(NameNode.getAddress(conf), conf);
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
          if (fos == null) {
            throw new IOException("Failed to copy " + fullName +
                " to /lost+found: could not store chain " + chain);
          }
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
  private void copyBlock(DFSClient dfs, LocatedBlock lblock,
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
        if (failures >= DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT) {
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
            setLength(-1).
            setVerifyChecksum(true).
            setClientName("fsck").
            setDatanodeInfo(chosenNode).
            setInetSocketAddress(targetAddr).
            setCachingStrategy(CachingStrategy.newDropBehind()).
            setClientCacheContext(dfs.getClientContext()).
            setConfiguration(namenode.conf).
            setRemotePeerFactory(new RemotePeerFactory() {
              @Override
              public Peer newConnectedPeer(InetSocketAddress addr,
                  Token<BlockTokenIdentifier> blockToken, DatanodeID datanodeId)
                  throws IOException {
                Peer peer = null;
                Socket s = NetUtils.getDefaultSocketFactory(conf).createSocket();
                try {
                  s.connect(addr, HdfsServerConstants.READ_TIMEOUT);
                  s.setSoTimeout(HdfsServerConstants.READ_TIMEOUT);
                  peer = TcpPeerServer.peerFromSocketAndKey(saslClient, s,
                        NamenodeFsck.this, blockToken, datanodeId);
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
      chosenNode = nodes[DFSUtil.getRandom().nextInt(nodes.length)];
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
      } else if (!lfStatus.isDir()) { // exists but not a directory
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
    long excessiveReplicas = 0L;
    long missingReplicas = 0L;
    long numOverReplicatedBlocks = 0L;
    long numUnderReplicatedBlocks = 0L;
    long numMisReplicatedBlocks = 0L;  // blocks that do not satisfy block placement policy
    long numMinReplicatedBlocks = 0L;  // minimally replicatedblocks
    long totalBlocks = 0L;
    long numExpectedReplicas = 0L;
    long totalOpenFilesBlocks = 0L;
    long totalFiles = 0L;
    long totalOpenFiles = 0L;
    long totalDirs = 0L;
    long totalSymlinks = 0L;
    long totalSize = 0L;
    long totalOpenFilesSize = 0L;
    long totalReplicas = 0L;

    final short replication;
    
    Result(Configuration conf) {
      this.replication = (short)conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 
                                            DFSConfigKeys.DFS_REPLICATION_DEFAULT);
    }
    
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
    
    /** Return the actual replication factor. */
    float getReplicationFactor() {
      if (totalBlocks == 0)
        return 0.0f;
      return (float) (totalReplicas) / (float) totalBlocks;
    }
    
    @Override
    public String toString() {
      StringBuilder res = new StringBuilder();
      res.append("Status: ").append((isHealthy() ? "HEALTHY" : "CORRUPT"))
          .append("\n Total size:\t").append(totalSize).append(" B");
      if (totalOpenFilesSize != 0) {
        res.append(" (Total open files size: ").append(totalOpenFilesSize)
            .append(" B)");
      }
      res.append("\n Total dirs:\t").append(totalDirs).append(
          "\n Total files:\t").append(totalFiles);
      res.append("\n Total symlinks:\t\t").append(totalSymlinks);
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
      if (corruptFiles > 0) {
        res.append("\n  ********************************").append(
            "\n  CORRUPT FILES:\t").append(corruptFiles);
        if (missingSize > 0) {
          res.append("\n  MISSING BLOCKS:\t").append(missingIds.size()).append(
              "\n  MISSING SIZE:\t\t").append(missingSize).append(" B");
        }
        if (corruptBlocks > 0) {
          res.append("\n  CORRUPT BLOCKS: \t").append(corruptBlocks);
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
              getReplicationFactor()).append("\n Corrupt blocks:\t\t").append(
              corruptBlocks).append("\n Missing replicas:\t\t").append(
              missingReplicas);
      if (totalReplicas > 0) {
        res.append(" (").append(
            ((float) (missingReplicas * 100) / (float) numExpectedReplicas)).append(
            " %)");
      }
      return res.toString();
    }
  }
}
