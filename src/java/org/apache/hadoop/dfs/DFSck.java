/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Random;
import java.util.TreeSet;
import java.util.logging.Logger;

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
public class DFSck {
  private static final Logger LOG = Logger.getLogger(DFSck.class.getName());

  /** Don't attempt any fixing . */
  public static final int FIXING_NONE = 0;
  /** Move corrupted files to /lost+found . */
  public static final int FIXING_MOVE = 1;
  /** Delete corrupted files. */
  public static final int FIXING_DELETE = 2;
  
  private DFSClient dfs;
  private UTF8 lostFound = null;
  private boolean lfInited = false;
  private boolean lfInitedOk = false;
  private Configuration conf;
  private boolean showFiles = false;
  private boolean showBlocks = false;
  private boolean showLocations = false;
  private int fixing;
  
  /**
   * Filesystem checker.
   * @param conf current Configuration
   * @param fixing one of pre-defined values
   * @param showFiles show each file being checked
   * @param showBlocks for each file checked show its block information
   * @param showLocations for each block in each file show block locations
   * @throws Exception
   */
  public DFSck(Configuration conf, int fixing, boolean showFiles, boolean showBlocks, boolean showLocations) throws Exception {
    this.conf = conf;
    this.fixing = fixing;
    this.showFiles = showFiles;
    this.showBlocks = showBlocks;
    this.showLocations = showLocations;
    String fsName = conf.get("fs.default.name", "local");
    if (fsName.equals("local")) {
      throw new Exception("This tool only checks DFS, but your config uses 'local' FS.");
    }
    this.dfs = new DFSClient(DataNode.createSocketAddr(fsName), conf);
  }
  
  /**
   * Check files on DFS, starting from the indicated path.
   * @param path starting point
   * @return result of checking
   * @throws Exception
   */
  public Result fsck(String path) throws Exception {
    DFSFileInfo[] files = dfs.listFiles(new UTF8(path));
    Result res = new Result();
    res.setReplication(conf.getInt("dfs.replication", 3));
    for (int i = 0; i < files.length; i++) {
      check(files[i], res);
    }
    return res;
  }
  
  private void check(DFSFileInfo file, Result res) throws Exception {
    if (file.isDir()) {
      if (showFiles)
        System.out.println(file.getPath() + " <dir>");
      res.totalDirs++;
      DFSFileInfo[] files = dfs.listFiles(new UTF8(file.getPath()));
      for (int i = 0; i < files.length; i++) {
        check(files[i], res);
      }
      return;
    }
    res.totalFiles++;
    res.totalSize += file.getLen();
    LocatedBlock[] blocks = dfs.namenode.open(file.getPath());
    res.totalBlocks += blocks.length;
    if (showFiles) {
      System.out.print(file.getPath() + " " + file.getLen() + ", " + blocks.length + " block(s): ");
    } else {
      System.out.print('.');
      System.out.flush();
      if (res.totalFiles % 100 == 0) System.out.println();
    }
    int missing = 0;
    long missize = 0;
    StringBuffer report = new StringBuffer();
    for (int i = 0; i < blocks.length; i++) {
      Block block = blocks[i].getBlock();
      long id = block.getBlockId();
      DatanodeInfo[] locs = blocks[i].getLocations();
      if (locs.length > res.replication) res.overReplicatedBlocks += (locs.length - res.replication);
      if (locs.length < res.replication && locs.length > 0) res.underReplicatedBlocks += (res.replication - locs.length);
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
        System.out.println("\nMISSING " + missing + " blocks of total size " + missize + " B");
      res.corruptFiles++;
      switch (fixing) {
        case FIXING_NONE: // do nothing
          System.err.println("\n - ignoring corrupted " + file.getPath());
          break;
        case FIXING_MOVE:
          System.err.println("\n - moving to /lost+found: " + file.getPath());
          lostFoundMove(file, blocks);
          break;
        case FIXING_DELETE:
          System.err.println("\n - deleting corrupted " + file.getPath());
          dfs.delete(new UTF8(file.getPath()));
      }
    }
    if (showFiles) {
      if (missing > 0) {
        System.out.println(" MISSING " + missing + " blocks of total size " + missize + " B");
      } else System.out.println(" OK");
      if (showBlocks) System.out.println(report.toString());
    }
  }
  
  private void lostFoundMove(DFSFileInfo file, LocatedBlock[] blocks) {
    if (!lfInited) {
      lostFoundInit();
    }
    if (!lfInitedOk) {
      return;
    }
    UTF8 target = new UTF8(lostFound.toString() + file.getPath());
    String errmsg = "Failed to move " + file.getPath() + " to /lost+found";
    try {
      if (!dfs.mkdirs(target)) {
        System.err.println(errmsg);
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
          System.err.println(errmsg + ": could not store chain " + chain);
          // perhaps we should bail out here...
          // return;
          continue;
        }
        
        // copy the block. It's a pity it's not abstracted from DFSInputStream ...
        try {
          copyBlock(lblock, fos);
        } catch (Exception e) {
          e.printStackTrace();
          // something went wrong copying this block...
          System.err.println(" - could not copy block " + lblock.getBlock().getBlockName() + " to " + target);
          fos.flush();
          fos.close();
          fos = null;
        }
      }
      if (fos != null) fos.close();
      System.err.println("\n - moved corrupted file " + file.getPath() + " to /lost+found");
      dfs.delete(new UTF8(file.getPath()));
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println(errmsg + ": " + e.getMessage());
    }
  }
  
  /*
   * XXX (ab) Bulk of this method is copied verbatim from {@link DFSClient}, which is
   * bad. Both places should be refactored to provide a method to copy blocks
   * around.
   */
  private void copyBlock(LocatedBlock lblock, FSOutputStream fos) throws Exception {
    int failures = 0;
    InetSocketAddress targetAddr = null;
    TreeSet deadNodes = new TreeSet();
    Socket s = null;
    DataInputStream in = null;
    DataOutputStream out = null;
    while (s == null) {
        DatanodeInfo chosenNode;

        try {
            chosenNode = bestNode(lblock.getLocations(), deadNodes);
            targetAddr = DataNode.createSocketAddr(chosenNode.getName().toString());
        } catch (IOException ie) {
            if (failures >= DFSClient.MAX_BLOCK_ACQUIRE_FAILURES) {
                throw new IOException("Could not obtain block " + lblock);
            }
            LOG.info("Could not obtain block from any node:  " + ie);
            try {
                Thread.sleep(10000);
            } catch (InterruptedException iex) {
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
        } catch (IOException ex) {
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
      try {in.close(); } catch (Exception e1) {};
      try {out.close(); } catch (Exception e1) {};
      try {s.close(); } catch (Exception e1) {};
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
  private DatanodeInfo bestNode(DatanodeInfo nodes[], TreeSet deadNodes) throws IOException {
      if ((nodes == null) || 
          (nodes.length - deadNodes.size() < 1)) {
          throw new IOException("No live nodes contain current block");
      }
      DatanodeInfo chosenNode = null;
      for (int i = 0; i < nodes.length; i++) {
          if (deadNodes.contains(nodes[i])) {
              continue;
          }
          String nodename = nodes[i].getName().toString();
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
          do {
              chosenNode = nodes[Math.abs(r.nextInt()) % nodes.length];
          } while (deadNodes.contains(chosenNode));
      }
      return chosenNode;
  }

  private void lostFoundInit() {
    lfInited = true;
    try {
      UTF8 lfName = new UTF8("/lost+found");
      // check that /lost+found exists
      if (!dfs.exists(lfName)) {
        lfInitedOk = dfs.mkdirs(lfName);
        lostFound = lfName;
      } else if (!dfs.isDirectory(lfName)) {
        System.err.println("Cannot use /lost+found : a regular file with this name exists.");
        lfInitedOk = false;
      } else { // exists and isDirectory
        lostFound = lfName;
        lfInitedOk = true;
      }
    } catch (Exception e) {
      e.printStackTrace();
      lfInitedOk = false;
    }
    if (lostFound == null) {
      System.err.println("Cannot initialize /lost+found .");
      lfInitedOk = false;
    }
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println("Usage: DFSck <path> [-move | -delete] [-files] [-blocks [-locations]]");
      System.err.println("\t<path>\tstart checking from this path");
      System.err.println("\t-move\tmove corrupted files to /lost+found");
      System.err.println("\t-delete\tdelete corrupted files");
      System.err.println("\t-files\tprint out files being checked");
      System.err.println("\t-blocks\tprint out block report");
      System.err.println("\t-locations\tprint out locations for every block");
      return;
    }
    Configuration conf = new Configuration();
    String path = args[0];
    boolean showFiles = false;
    boolean showBlocks = false;
    boolean showLocations = false;
    int fixing = FIXING_NONE;
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-files")) showFiles = true;
      if (args[i].equals("-blocks")) showBlocks = true;
      if (args[i].equals("-locations")) showLocations = true;
      if (args[i].equals("-move")) fixing = FIXING_MOVE;
      if (args[i].equals("-delete")) fixing = FIXING_DELETE;
    }
    DFSck fsck = new DFSck(conf, fixing, showFiles, showBlocks, showLocations);
    Result res = fsck.fsck(path);
    System.out.println();
    System.out.println(res);
    if (res.isHealthy()) {
      System.out.println("\n\nThe filesystem under path '" + args[0] + "' is HEALTHY");
    } else {
      System.out.println("\n\nThe filesystem under path '" + args[0] + "' is CORRUPT");
    }
  }

  /**
   * Result of checking, plus overall DFS statistics.
   * @author Andrzej Bialecki
   */
  public static class Result {
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
     * @return
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
      return (float)(totalBlocks * replication + overReplicatedBlocks - underReplicatedBlocks) / (float)totalBlocks;
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
      res.append("\n Total blocks:\t" + totalBlocks + " (avg. block size "
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
      res.append("\n Over-replicated blocks:\t" + overReplicatedBlocks
              + " (" + ((float)(overReplicatedBlocks * 100) / (float)totalBlocks)
              + " %)");
      res.append("\n Under-replicated blocks:\t" + underReplicatedBlocks
              + " (" + ((float)(underReplicatedBlocks * 100) / (float)totalBlocks)
              + " %)");
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
