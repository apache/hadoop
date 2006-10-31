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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSOutputStream;
import org.apache.hadoop.io.UTF8;


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
 *
 * @author Andrzej Bialecki
 */
public class NamenodeFsck {
  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.dfs.NameNode");
  
  /** Don't attempt any fixing . */
  public static final int FIXING_NONE = 0;
  /** Move corrupted files to /lost+found . */
  public static final int FIXING_MOVE = 1;
  /** Delete corrupted files. */
  public static final int FIXING_DELETE = 2;
  
  private NameNode nn;
  private UTF8 lostFound = null;
  private boolean lfInited = false;
  private boolean lfInitedOk = false;
  private boolean showFiles = false;
  private boolean showBlocks = false;
  private boolean showLocations = false;
  private int fixing = FIXING_NONE;
  private String path = "/";
  
  private Configuration conf;
  private HttpServletResponse response;
  private PrintWriter out;
  
  /**
   * Filesystem checker.
   * @param conf configuration (namenode config)
   * @param nn namenode that this fsck is going to use
   * @param pmap key=value[] map that is passed to the http servlet as url parameters
   * @param response the object into which  this servelet writes the url contents
   * @throws IOException
   */
  public NamenodeFsck(Configuration conf,
      NameNode nn,
      Map<String,String[]> pmap,
      HttpServletResponse response) throws IOException {
    this.conf = conf;
    this.nn = nn;
    this.response = response;
    this.out = response.getWriter();
    for (Iterator<String> it = pmap.keySet().iterator(); it.hasNext();) {
      String key = it.next();
      if (key.equals("path")) { this.path = pmap.get("path")[0]; }
      else if (key.equals("move")) { this.fixing = FIXING_MOVE; }
      else if (key.equals("delete")) { this.fixing = FIXING_DELETE; }
      else if (key.equals("files")) { this.showFiles = true; }
      else if (key.equals("blocks")) { this.showBlocks = true; }
      else if (key.equals("locations")) { this.showLocations = true; }
    }
  }
  
  /**
   * Check files on DFS, starting from the indicated path.
   * @throws Exception
   */
  public void fsck() throws IOException {
    try {
      DFSFileInfo[] files = nn.getListing(path);
      FsckResult res = new FsckResult();
      res.setReplication((short) conf.getInt("dfs.replication", 3));
      if (files != null) {
        for (int i = 0; i < files.length; i++) {
          check(files[i], res);
        }
      }
      out.println(res);
      if (res.isHealthy()) {
        out.println("\n\nThe filesystem under path '" + path + "' is HEALTHY");
      }  else {
        out.println("\n\nThe filesystem under path '" + path + "' is CORRUPT");
      }
    } finally {
      out.close();
    }
  }
  
  private void check(DFSFileInfo file, FsckResult res) throws IOException {
    if (file.isDir()) {
      if (showFiles)
        out.println(file.getPath() + " <dir>");
      res.totalDirs++;
      DFSFileInfo[] files = nn.getListing(file.getPath());
      for (int i = 0; i < files.length; i++) {
        check(files[i], res);
      }
      return;
    }
    res.totalFiles++;
    res.totalSize += file.getLen();
    LocatedBlock[] blocks = nn.open(file.getPath());
    res.totalBlocks += blocks.length;
    if (showFiles) {
      out.print(file.getPath() + " " + file.getLen() + ", " + blocks.length + " block(s): ");
    }  else {
      out.print('.');
      out.flush();
      if (res.totalFiles % 100 == 0)        out.println();
    }
    int missing = 0;
    long missize = 0;
    StringBuffer report = new StringBuffer();
    for (int i = 0; i < blocks.length; i++) {
      Block block = blocks[i].getBlock();
      long id = block.getBlockId();
      DatanodeInfo[] locs = blocks[i].getLocations();
      short targetFileReplication = file.getReplication();
      if (locs.length > targetFileReplication) res.overReplicatedBlocks += (locs.length - targetFileReplication);
      if (locs.length < targetFileReplication && locs.length > 0) res.underReplicatedBlocks += (targetFileReplication - locs.length);
      report.append(i + ". " + id + " len=" + block.getNumBytes());
      if (locs == null || locs.length == 0) {
        report.append(" MISSING!");
        res.addMissing(block.getBlockName(), block.getNumBytes());
        missing++;
        missize += block.getNumBytes();
      } else {
        report.append(" repl=" + locs.length);
        if (showLocations) {
          StringBuffer sb = new StringBuffer("[");
          for (int j = 0; j < locs.length; j++) {
            if (j > 0) sb.append(", ");
            sb.append(locs[j]);
          }
          sb.append(']');
          report.append(" " + sb.toString());
        }
      }
      report.append('\n');
    }
    if (missing > 0) {
      if (!showFiles)
        out.println("\nMISSING " + missing + " blocks of total size " + missize + " B");
      res.corruptFiles++;
      switch(fixing) {
        case FIXING_NONE:
          break;
        case FIXING_MOVE:
          lostFoundMove(file, blocks);
          break;
        case FIXING_DELETE:
          nn.delete(file.getPath());
      }
    }
    if (showFiles) {
      if (missing > 0) {
        out.println(" MISSING " + missing + " blocks of total size " + missize + " B");
      }  else        out.println(" OK");
      if (showBlocks)        out.println(report.toString());
    }
  }
  
  private void lostFoundMove(DFSFileInfo file, LocatedBlock[] blocks)
  throws IOException {
    DFSClient dfs = new DFSClient(DataNode.createSocketAddr(
        conf.get("fs.default.name", "local")), conf);
    if (!lfInited) {
      lostFoundInit(dfs);
    }
    if (!lfInitedOk) {
      return;
    }
    String target = lostFound.toString() + file.getPath();
    String errmsg = "Failed to move " + file.getPath() + " to /lost+found";
    try {
      if (!nn.mkdirs(target)) {
        LOG.warn(errmsg);
        return;
      }
      // create chains
      int chain = 0;
      FSOutputStream fos = null;
      for (int i = 0; i < blocks.length; i++) {
        LocatedBlock lblock = blocks[i];
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
          fos = dfs.create(new UTF8(target.toString() + "/" + chain), true);
          if (fos != null) chain++;
        }
        if (fos == null) {
          LOG.warn(errmsg + ": could not store chain " + chain);
          // perhaps we should bail out here...
          // return;
          continue;
        }
        
        // copy the block. It's a pity it's not abstracted from DFSInputStream ...
        try {
          copyBlock(dfs, lblock, fos);
        } catch (Exception e) {
          e.printStackTrace();
          // something went wrong copying this block...
          LOG.warn(" - could not copy block " + lblock.getBlock().getBlockName() + " to " + target);
          fos.flush();
          fos.close();
          fos = null;
        }
      }
      if (fos != null) fos.close();
      LOG.warn("\n - moved corrupted file " + file.getPath() + " to /lost+found");
      dfs.delete(new UTF8(file.getPath()));
    }  catch (Exception e) {
      e.printStackTrace();
      LOG.warn(errmsg + ": " + e.getMessage());
    }
  }
      
  /*
   * XXX (ab) Bulk of this method is copied verbatim from {@link DFSClient}, which is
   * bad. Both places should be refactored to provide a method to copy blocks
   * around.
   */
      private void copyBlock(DFSClient dfs, LocatedBlock lblock,
          FSOutputStream fos) throws Exception {
    int failures = 0;
    InetSocketAddress targetAddr = null;
    TreeSet deadNodes = new TreeSet();
    Socket s = null;
    DataInputStream in = null;
    DataOutputStream out = null;
    while (s == null) {
      DatanodeInfo chosenNode;
      
      try {
        chosenNode = bestNode(dfs, lblock.getLocations(), deadNodes);
        targetAddr = DataNode.createSocketAddr(chosenNode.getName());
      }  catch (IOException ie) {
        if (failures >= DFSClient.MAX_BLOCK_ACQUIRE_FAILURES) {
          throw new IOException("Could not obtain block " + lblock);
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
        s = new Socket();
        s.connect(targetAddr, FSConstants.READ_TIMEOUT);
        s.setSoTimeout(FSConstants.READ_TIMEOUT);
        
        //
        // Xmit header info to datanode
        //
        out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
        out.write(FSConstants.OP_READSKIP_BLOCK);
        lblock.getBlock().write(out);
        out.writeLong(0L);
        out.flush();
        
        //
        // Get bytes in block, set streams
        //
        in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
        long curBlockSize = in.readLong();
        long amtSkipped = in.readLong();
        if (curBlockSize != lblock.getBlock().len) {
          throw new IOException("Recorded block size is " + lblock.getBlock().len + ", but datanode reports size of " + curBlockSize);
        }
        if (amtSkipped != 0L) {
          throw new IOException("Asked for offset of " + 0L + ", but only received offset of " + amtSkipped);
        }
      }  catch (IOException ex) {
        // Put chosen node into dead list, continue
        LOG.info("Failed to connect to " + targetAddr + ":" + ex);
        deadNodes.add(chosenNode);
        if (s != null) {
          try {
            s.close();
          } catch (IOException iex) {
          }
        }
        s = null;
      }
    }
    if (in == null) {
      throw new Exception("Could not open data stream for " + lblock.getBlock().getBlockName());
    }
    byte[] buf = new byte[1024];
    int cnt = 0;
    boolean success = true;
    try {
      while ((cnt = in.read(buf)) > 0) {
        fos.write(buf, 0, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      success = false;
    } finally {
      try {in.close(); } catch (Exception e1) {}
      try {out.close(); } catch (Exception e1) {}
      try {s.close(); } catch (Exception e1) {}
    }
    if (!success)
      throw new Exception("Could not copy block data for " + lblock.getBlock().getBlockName());
  }
      
  /*
   * XXX (ab) See comment above for copyBlock().
   *
   * Pick the best node from which to stream the data.
   * That's the local one, if available.
   */
      Random r = new Random();
  private DatanodeInfo bestNode(DFSClient dfs, DatanodeInfo[] nodes,
      TreeSet deadNodes) throws IOException {
    if ((nodes == null) ||
            (nodes.length - deadNodes.size() < 1)) {
      throw new IOException("No live nodes contain current block");
    }
    DatanodeInfo chosenNode = null;
    for (int i = 0; i < nodes.length; i++) {
      if (deadNodes.contains(nodes[i])) {
        continue;
      }
      String nodename = nodes[i].getName();
      int colon = nodename.indexOf(':');
      if (colon >= 0) {
        nodename = nodename.substring(0, colon);
      }
      if (dfs.localName.equals(nodename)) {
        chosenNode = nodes[i];
        break;
      }
    }
    if (chosenNode == null) {
      do  {
        chosenNode = nodes[Math.abs(r.nextInt())  % nodes.length];
      } while (deadNodes.contains(chosenNode));
    }
    return chosenNode;
  }
  
  private void lostFoundInit(DFSClient dfs) {
    lfInited = true;
    try {
      UTF8 lfName = new UTF8("/lost+found");
      // check that /lost+found exists
      if (!dfs.exists(lfName)) {
        lfInitedOk = dfs.mkdirs(lfName);
        lostFound = lfName;
      } else        if (!dfs.isDirectory(lfName)) {
          LOG.warn("Cannot use /lost+found : a regular file with this name exists.");
          lfInitedOk = false;
        }  else { // exists and isDirectory
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
    }
  }
  
  /**
   * @param args
   */
  public int run(String[] args) throws Exception {
    
    return 0;
  }
  
  /**
   * FsckResult of checking, plus overall DFS statistics.
   *
   * @author Andrzej Bialecki
   */
  public class FsckResult {
    private ArrayList missingIds = new ArrayList();
    private long missingSize = 0L;
    private long corruptFiles = 0L;
    private long overReplicatedBlocks = 0L;
    private long underReplicatedBlocks = 0L;
    private int replication = 0;
    private long totalBlocks = 0L;
    private long totalFiles = 0L;
    private long totalDirs = 0L;
    private long totalSize = 0L;
    
    /**
     * DFS is considered healthy if there are no missing blocks.
     */
    public boolean isHealthy() {
      return missingIds.size() == 0;
    }
    
    /** Add a missing block name, plus its size. */
    public void addMissing(String id, long size) {
      missingIds.add(id);
      missingSize += size;
    }
    
    /** Return a list of missing block names (as list of Strings). */
    public ArrayList getMissingIds() {
      return missingIds;
    }
    
    /** Return total size of missing data, in bytes. */
    public long getMissingSize() {
      return missingSize;
    }
    
    public void setMissingSize(long missingSize) {
      this.missingSize = missingSize;
    }
    
    /** Return the number of over-replicsted blocks. */
    public long getOverReplicatedBlocks() {
      return overReplicatedBlocks;
    }
    
    public void setOverReplicatedBlocks(long overReplicatedBlocks) {
      this.overReplicatedBlocks = overReplicatedBlocks;
    }
    
    /** Return the actual replication factor. */
    public float getReplicationFactor() {
      if (totalBlocks != 0)
        return (float) (totalBlocks * replication + overReplicatedBlocks - underReplicatedBlocks) / (float) totalBlocks;
      else
        return 0.0f;
    }
    
    /** Return the number of under-replicated blocks. Note: missing blocks are not counted here.*/
    public long getUnderReplicatedBlocks() {
      return underReplicatedBlocks;
    }
    
    public void setUnderReplicatedBlocks(long underReplicatedBlocks) {
      this.underReplicatedBlocks = underReplicatedBlocks;
    }
    
    /** Return total number of directories encountered during this scan. */
    public long getTotalDirs() {
      return totalDirs;
    }
    
    public void setTotalDirs(long totalDirs) {
      this.totalDirs = totalDirs;
    }
    
    /** Return total number of files encountered during this scan. */
    public long getTotalFiles() {
      return totalFiles;
    }
    
    public void setTotalFiles(long totalFiles) {
      this.totalFiles = totalFiles;
    }
    
    /** Return total size of scanned data, in bytes. */
    public long getTotalSize() {
      return totalSize;
    }
    
    public void setTotalSize(long totalSize) {
      this.totalSize = totalSize;
    }
    
    /** Return the intended replication factor, against which the over/under-
     * replicated blocks are counted. Note: this values comes from the current
     * Configuration supplied for the tool, so it may be different from the
     * value in DFS Configuration.
     */
    public int getReplication() {
      return replication;
    }
    
    public void setReplication(int replication) {
      this.replication = replication;
    }
    
    /** Return the total number of blocks in the scanned area. */
    public long getTotalBlocks() {
      return totalBlocks;
    }
    
    public void setTotalBlocks(long totalBlocks) {
      this.totalBlocks = totalBlocks;
    }
    
    public String toString() {
      StringBuffer res = new StringBuffer();
      res.append("Status: " + (isHealthy() ? "HEALTHY" : "CORRUPT"));
      res.append("\n Total size:\t" + totalSize + " B");
      res.append("\n Total blocks:\t" + totalBlocks);
      if (totalBlocks > 0) res.append(" (avg. block size "
          + (totalSize / totalBlocks) + " B)");
      res.append("\n Total dirs:\t" + totalDirs);
      res.append("\n Total files:\t" + totalFiles);
      if (missingSize > 0) {
        res.append("\n  ********************************");
        res.append("\n  CORRUPT FILES:\t" + corruptFiles);
        res.append("\n  MISSING BLOCKS:\t" + missingIds.size());
        res.append("\n  MISSING SIZE:\t\t" + missingSize + " B");
        res.append("\n  ********************************");
      }
      res.append("\n Over-replicated blocks:\t" + overReplicatedBlocks);
      if (totalBlocks > 0)        res.append(" (" + ((float) (overReplicatedBlocks * 100) / (float) totalBlocks) + " %)");
      res.append("\n Under-replicated blocks:\t" + underReplicatedBlocks);
      if (totalBlocks > 0)        res.append(" (" + ((float) (underReplicatedBlocks * 100) / (float) totalBlocks) + " %)");
      res.append("\n Target replication factor:\t" + replication);
      res.append("\n Real replication factor:\t" + getReplicationFactor());
      return res.toString();
    }
    
    /** Return the number of currupted files. */
    public long getCorruptFiles() {
      return corruptFiles;
    }
    
    public void setCorruptFiles(long corruptFiles) {
      this.corruptFiles = corruptFiles;
    }
  }
}
