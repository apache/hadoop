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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.DU;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.mortbay.log.Log;

/**************************************************
 * FSDataset manages a set of data blocks.  Each block
 * has a unique name and an extent on disk.
 *
 ***************************************************/
public class FSDataset implements FSConstants, FSDatasetInterface {


  /**
   * A node type that can be built into a tree reflecting the
   * hierarchy of blocks on the local disk.
   */
  class FSDir {
    File dir;
    int numBlocks = 0;
    FSDir children[];
    int lastChildIdx = 0;
    /**
     */
    public FSDir(File dir) 
      throws IOException {
      this.dir = dir;
      this.children = null;
      if (!dir.exists()) {
        if (!dir.mkdirs()) {
          throw new IOException("Mkdirs failed to create " + 
                                dir.toString());
        }
      } else {
        File[] files = dir.listFiles();
        int numChildren = 0;
        for (int idx = 0; idx < files.length; idx++) {
          if (files[idx].isDirectory()) {
            numChildren++;
          } else if (Block.isBlockFilename(files[idx])) {
            numBlocks++;
          }
        }
        if (numChildren > 0) {
          children = new FSDir[numChildren];
          int curdir = 0;
          for (int idx = 0; idx < files.length; idx++) {
            if (files[idx].isDirectory()) {
              children[curdir] = new FSDir(files[idx]);
              curdir++;
            }
          }
        }
      }
    }
        
    public File addBlock(Block b, File src) throws IOException {
      //First try without creating subdirectories
      File file = addBlock(b, src, false, false);          
      return (file != null) ? file : addBlock(b, src, true, true);
    }

    private File addBlock(Block b, File src, boolean createOk, 
                          boolean resetIdx) throws IOException {
      if (numBlocks < maxBlocksPerDir) {
        File dest = new File(dir, b.getBlockName());
        File metaData = getMetaFile( src, b );
        File newmeta = getMetaFile(dest, b);
        if ( ! metaData.renameTo( newmeta ) ||
            ! src.renameTo( dest ) ) {
          throw new IOException( "could not move files for " + b +
                                 " from tmp to " + 
                                 dest.getAbsolutePath() );
        }
        if (DataNode.LOG.isDebugEnabled()) {
          DataNode.LOG.debug("addBlock: Moved " + metaData + " to " + newmeta);
          DataNode.LOG.debug("addBlock: Moved " + src + " to " + dest);
        }

        numBlocks += 1;
        return dest;
      }
            
      if (lastChildIdx < 0 && resetIdx) {
        //reset so that all children will be checked
        lastChildIdx = random.nextInt(children.length);              
      }
            
      if (lastChildIdx >= 0 && children != null) {
        //Check if any child-tree has room for a block.
        for (int i=0; i < children.length; i++) {
          int idx = (lastChildIdx + i)%children.length;
          File file = children[idx].addBlock(b, src, false, resetIdx);
          if (file != null) {
            lastChildIdx = idx;
            return file; 
          }
        }
        lastChildIdx = -1;
      }
            
      if (!createOk) {
        return null;
      }
            
      if (children == null || children.length == 0) {
        children = new FSDir[maxBlocksPerDir];
        for (int idx = 0; idx < maxBlocksPerDir; idx++) {
          children[idx] = new FSDir(new File(dir, DataStorage.BLOCK_SUBDIR_PREFIX+idx));
        }
      }
            
      //now pick a child randomly for creating a new set of subdirs.
      lastChildIdx = random.nextInt(children.length);
      return children[ lastChildIdx ].addBlock(b, src, true, false); 
    }

    /** Find the metadata file for the specified block file.
     * Return the generation stamp from the name of the metafile.
     */
    long getGenerationStampFromFile(File[] listdir, File blockFile) {
      String blockName = blockFile.getName();
      for (int j = 0; j < listdir.length; j++) {
        String path = listdir[j].getName();
        if (!path.startsWith(blockName)) {
          continue;
        }
        String[] vals = path.split("_");
        if (vals.length != 3) {     // blk, blkid, genstamp.meta
          continue;
        }
        String[] str = vals[2].split("\\.");
        if (str.length != 2) {
          continue;
        }
        return Long.parseLong(str[0]);
      }
      DataNode.LOG.warn("Block " + blockFile + 
                        " does not have a metafile!");
      return Block.GRANDFATHER_GENERATION_STAMP;
    }

    /**
     * Populate the given blockSet with any child blocks
     * found at this node.
     */
    public void getBlockInfo(TreeSet<Block> blockSet) {
      if (children != null) {
        for (int i = 0; i < children.length; i++) {
          children[i].getBlockInfo(blockSet);
        }
      }

      File blockFiles[] = dir.listFiles();
      for (int i = 0; i < blockFiles.length; i++) {
        if (Block.isBlockFilename(blockFiles[i])) {
          long genStamp = getGenerationStampFromFile(blockFiles, blockFiles[i]);
          blockSet.add(new Block(blockFiles[i], blockFiles[i].length(), genStamp));
        }
      }
    }

    void getVolumeMap(HashMap<Block, DatanodeBlockInfo> volumeMap, FSVolume volume) {
      if (children != null) {
        for (int i = 0; i < children.length; i++) {
          children[i].getVolumeMap(volumeMap, volume);
        }
      }

      File blockFiles[] = dir.listFiles();
      for (int i = 0; i < blockFiles.length; i++) {
        if (Block.isBlockFilename(blockFiles[i])) {
          long genStamp = getGenerationStampFromFile(blockFiles, blockFiles[i]);
          volumeMap.put(new Block(blockFiles[i], blockFiles[i].length(), genStamp), 
                        new DatanodeBlockInfo(volume, blockFiles[i]));
        }
      }
    }
        
    /**
     * check if a data diretory is healthy
     * @throws DiskErrorException
     */
    public void checkDirTree() throws DiskErrorException {
      DiskChecker.checkDir(dir);
            
      if (children != null) {
        for (int i = 0; i < children.length; i++) {
          children[i].checkDirTree();
        }
      }
    }
        
    void clearPath(File f) {
      String root = dir.getAbsolutePath();
      String dir = f.getAbsolutePath();
      if (dir.startsWith(root)) {
        String[] dirNames = dir.substring(root.length()).
          split(File.separator + "subdir");
        if (clearPath(f, dirNames, 1))
          return;
      }
      clearPath(f, null, -1);
    }
        
    /*
     * dirNames is an array of string integers derived from
     * usual directory structure data/subdirN/subdirXY/subdirM ...
     * If dirName array is non-null, we only check the child at 
     * the children[dirNames[idx]]. This avoids iterating over
     * children in common case. If directory structure changes 
     * in later versions, we need to revisit this.
     */
    private boolean clearPath(File f, String[] dirNames, int idx) {
      if ((dirNames == null || idx == dirNames.length) &&
          dir.compareTo(f) == 0) {
        numBlocks--;
        return true;
      }
          
      if (dirNames != null) {
        //guess the child index from the directory name
        if (idx > (dirNames.length - 1) || children == null) {
          return false;
        }
        int childIdx; 
        try {
          childIdx = Integer.parseInt(dirNames[idx]);
        } catch (NumberFormatException ignored) {
          // layout changed? we could print a warning.
          return false;
        }
        return (childIdx >= 0 && childIdx < children.length) ?
          children[childIdx].clearPath(f, dirNames, idx+1) : false;
      }

      //guesses failed. back to blind iteration.
      if (children != null) {
        for(int i=0; i < children.length; i++) {
          if (children[i].clearPath(f, null, -1)){
            return true;
          }
        }
      }
      return false;
    }
        
    public String toString() {
      return "FSDir{" +
        "dir=" + dir +
        ", children=" + (children == null ? null : Arrays.asList(children)) +
        "}";
    }
  }

  class FSVolume {
    private FSDir dataDir;
    private File tmpDir;
    private File detachDir; // copy on write for blocks in snapshot
    private DF usage;
    private DU dfsUsage;
    private long reserved;

    
    FSVolume(File currentDir, Configuration conf) throws IOException {
      this.reserved = conf.getLong("dfs.datanode.du.reserved", 0);
      boolean supportAppends = conf.getBoolean("dfs.support.append", false);
      File parent = currentDir.getParentFile();

      this.detachDir = new File(parent, "detach");
      if (detachDir.exists()) {
        recoverDetachedBlocks(currentDir, detachDir);
      }

      // Files that were being written when the datanode was last shutdown
      // are now moved back to the data directory. It is possible that
      // in the future, we might want to do some sort of datanode-local
      // recovery for these blocks. For example, crc validation.
      //
      this.tmpDir = new File(parent, "tmp");
      if (tmpDir.exists()) {
        if (supportAppends) {
          recoverDetachedBlocks(currentDir, tmpDir);
        } else {
          FileUtil.fullyDelete(tmpDir);
        }
      }
      this.dataDir = new FSDir(currentDir);
      if (!tmpDir.mkdirs()) {
        if (!tmpDir.isDirectory()) {
          throw new IOException("Mkdirs failed to create " + tmpDir.toString());
        }
      }
      if (!detachDir.mkdirs()) {
        if (!detachDir.isDirectory()) {
          throw new IOException("Mkdirs failed to create " + detachDir.toString());
        }
      }
      this.usage = new DF(parent, conf);
      this.dfsUsage = new DU(parent, conf);
      this.dfsUsage.start();
    }

    void decDfsUsed(long value) {
      dfsUsage.decDfsUsed(value);
    }
    
    long getDfsUsed() throws IOException {
      return dfsUsage.getUsed();
    }
    
    long getCapacity() throws IOException {
      if (reserved > usage.getCapacity()) {
        return 0;
      }

      return usage.getCapacity()-reserved;
    }
      
    long getAvailable() throws IOException {
      long remaining = getCapacity()-getDfsUsed();
      long available = usage.getAvailable();
      if (remaining>available) {
        remaining = available;
      }
      return (remaining > 0) ? remaining : 0;
    }
      
    long getReserved(){
      return reserved;
    }
    
    String getMount() throws IOException {
      return usage.getMount();
    }
      
    File getDir() {
      return dataDir.dir;
    }
    
    /**
     * Temporary files. They get moved to the real block directory either when
     * the block is finalized or the datanode restarts.
     */
    File createTmpFile(Block b) throws IOException {
      File f = new File(tmpDir, b.getBlockName());
      return createTmpFile(b, f);
    }

    /**
     * Returns the name of the temporary file for this block.
     */
    File getTmpFile(Block b) throws IOException {
      File f = new File(tmpDir, b.getBlockName());
      return f;
    }

    /**
     * Files used for copy-on-write. They need recovery when datanode
     * restarts.
     */
    File createDetachFile(Block b, String filename) throws IOException {
      File f = new File(detachDir, filename);
      return createTmpFile(b, f);
    }

    private File createTmpFile(Block b, File f) throws IOException {
      if (f.exists()) {
        throw new IOException("Unexpected problem in creating temporary file for "+
                              b + ".  File " + f + " should not be present, but is.");
      }
      // Create the zero-length temp file
      //
      boolean fileCreated = false;
      try {
        fileCreated = f.createNewFile();
      } catch (IOException ioe) {
        throw (IOException)new IOException(DISK_ERROR +f).initCause(ioe);
      }
      if (!fileCreated) {
        throw new IOException("Unexpected problem in creating temporary file for "+
                              b + ".  File " + f + " should be creatable, but is already present.");
      }
      return f;
    }
      
    File addBlock(Block b, File f) throws IOException {
      File blockFile = dataDir.addBlock(b, f);
      File metaFile = getMetaFile( blockFile , b);
      dfsUsage.incDfsUsed(b.getNumBytes()+metaFile.length());
      return blockFile;
    }
      
    void checkDirs() throws DiskErrorException {
      dataDir.checkDirTree();
      DiskChecker.checkDir(tmpDir);
    }
      
    void getBlockInfo(TreeSet<Block> blockSet) {
      dataDir.getBlockInfo(blockSet);
    }
      
    void getVolumeMap(HashMap<Block, DatanodeBlockInfo> volumeMap) {
      dataDir.getVolumeMap(volumeMap, this);
    }
      
    void clearPath(File f) {
      dataDir.clearPath(f);
    }
      
    public String toString() {
      return dataDir.dir.getAbsolutePath();
    }

    /**
     * Recover detached files on datanode restart. If a detached block
     * does not exist in the original directory, then it is moved to the
     * original directory.
     */
    private void recoverDetachedBlocks(File dataDir, File dir) 
                                           throws IOException {
      File contents[] = dir.listFiles();
      if (contents == null) {
        return;
      }
      for (int i = 0; i < contents.length; i++) {
        if (!contents[i].isFile()) {
          throw new IOException ("Found " + contents[i] + " in " + dir +
                                 " but it is not a file.");
        }

        //
        // If the original block file still exists, then no recovery
        // is needed.
        //
        File blk = new File(dataDir, contents[i].getName());
        if (!blk.exists()) {
          if (!contents[i].renameTo(blk)) {
            throw new IOException("Unable to recover detached file " +
                                  contents[i]);
          }
          continue;
        }
        if (!contents[i].delete()) {
            throw new IOException("Unable to cleanup detached file " +
                                  contents[i]);
        }
      }
    }
  }
    
  static class FSVolumeSet {
    FSVolume[] volumes = null;
    int curVolume = 0;
      
    FSVolumeSet(FSVolume[] volumes) {
      this.volumes = volumes;
    }
    
    private int numberOfVolumes() {
      return volumes.length;
    }
      
    synchronized FSVolume getNextVolume(long blockSize) throws IOException {
      
      if(volumes.length < 1) {
        throw new DiskOutOfSpaceException("No more available volumes");
      }
      
      // since volumes could've been removed because of the failure
      // make sure we are not out of bounds
      if(curVolume >= volumes.length) {
        curVolume = 0;
      }
      
      int startVolume = curVolume;
      
      while (true) {
        FSVolume volume = volumes[curVolume];
        curVolume = (curVolume + 1) % volumes.length;
        if (volume.getAvailable() > blockSize) { return volume; }
        if (curVolume == startVolume) {
          throw new DiskOutOfSpaceException("Insufficient space for an additional block");
        }
      }
    }
      
    long getDfsUsed() throws IOException {
      long dfsUsed = 0L;
      for (int idx = 0; idx < volumes.length; idx++) {
        dfsUsed += volumes[idx].getDfsUsed();
      }
      return dfsUsed;
    }

    synchronized long getCapacity() throws IOException {
      long capacity = 0L;
      for (int idx = 0; idx < volumes.length; idx++) {
        capacity += volumes[idx].getCapacity();
      }
      return capacity;
    }
      
    synchronized long getRemaining() throws IOException {
      long remaining = 0L;
      for (int idx = 0; idx < volumes.length; idx++) {
        remaining += volumes[idx].getAvailable();
      }
      return remaining;
    }
      
    synchronized void getBlockInfo(TreeSet<Block> blockSet) {
      for (int idx = 0; idx < volumes.length; idx++) {
        volumes[idx].getBlockInfo(blockSet);
      }
    }
      
    synchronized void getVolumeMap(HashMap<Block, DatanodeBlockInfo> volumeMap) {
      for (int idx = 0; idx < volumes.length; idx++) {
        volumes[idx].getVolumeMap(volumeMap);
      }
    }
      
    /**
     * goes over all the volumes and checkDir eachone of them
     * if one throws DiskErrorException - removes from the list of active 
     * volumes. 
     * @return list of all the removed volumes
     */
    synchronized List<FSVolume> checkDirs() {
      
      ArrayList<FSVolume> removed_vols = null;  
      
      for (int idx = 0; idx < volumes.length; idx++) {
        FSVolume fsv = volumes[idx];
        try {
          fsv.checkDirs();
        } catch (DiskErrorException e) {
          DataNode.LOG.warn("Removing failed volume " + fsv + ": ",e);
          if(removed_vols == null) {
            removed_vols = new ArrayList<FSVolume>(1);
          }
          removed_vols.add(volumes[idx]);
          volumes[idx] = null; //remove the volume
        }
      }
      
      // repair array - copy non null elements
      int removed_size = (removed_vols==null)? 0 : removed_vols.size();
      if(removed_size > 0) {
        FSVolume fsvs[] = new FSVolume [volumes.length-removed_size];
        for(int idx=0,idy=0; idx<volumes.length; idx++) {
          if(volumes[idx] != null) {
            fsvs[idy] = volumes[idx];
            idy++;
          }
        }
        volumes = fsvs; // replace array of volumes
      }
      Log.info("Completed FSVolumeSet.checkDirs. Removed=" + removed_size + 
          "volumes. List of current volumes: " +   toString());
      
      return removed_vols;
    }
    
      
    public String toString() {
      StringBuffer sb = new StringBuffer();
      for (int idx = 0; idx < volumes.length; idx++) {
        sb.append(volumes[idx].toString());
        if (idx != volumes.length - 1) { sb.append(","); }
      }
      return sb.toString();
    }
  }
  
  //////////////////////////////////////////////////////
  //
  // FSDataSet
  //
  //////////////////////////////////////////////////////

  //Find better place?
  public static final String METADATA_EXTENSION = ".meta";
  public static final short METADATA_VERSION = 1;
    

  static class ActiveFile {
    final File file;
    final List<Thread> threads = new ArrayList<Thread>(2);

    ActiveFile(File f, List<Thread> list) {
      file = f;
      if (list != null) {
        threads.addAll(list);
      }
      threads.add(Thread.currentThread());
    }
    
    public String toString() {
      return getClass().getSimpleName() + "(file=" + file
          + ", threads=" + threads + ")";
    }
  } 
  
  static String getMetaFileName(String blockFileName, long genStamp) {
    return blockFileName + "_" + genStamp + METADATA_EXTENSION;
  }
  
  static File getMetaFile(File f , Block b) {
    return new File(getMetaFileName(f.getAbsolutePath(),
                                    b.getGenerationStamp())); 
  }
  protected File getMetaFile(Block b) throws IOException {
    return getMetaFile(getBlockFile(b), b);
  }

  /** Find the corresponding meta data file from a given block file */
  private static File findMetaFile(final File blockFile) throws IOException {
    final String prefix = blockFile.getName() + "_";
    final File parent = blockFile.getParentFile();
    File[] matches = parent.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return dir.equals(parent)
            && name.startsWith(prefix) && name.endsWith(METADATA_EXTENSION);
      }
    });

    if (matches == null || matches.length == 0) {
      throw new IOException("Meta file not found, blockFile=" + blockFile);
    }
    else if (matches.length > 1) {
      throw new IOException("Found more than one meta files: " 
          + Arrays.asList(matches));
    }
    return matches[0];
  }
  
  /** Find the corresponding meta data file from a given block file */
  private static long parseGenerationStamp(File blockFile, File metaFile
      ) throws IOException {
    String metaname = metaFile.getName();
    String gs = metaname.substring(blockFile.getName().length() + 1,
        metaname.length() - METADATA_EXTENSION.length());
    try {
      return Long.parseLong(gs);
    } catch(NumberFormatException nfe) {
      throw (IOException)new IOException("blockFile=" + blockFile
          + ", metaFile=" + metaFile).initCause(nfe);
    }
  }

  /** Return the block file for the given ID */ 
  public File findBlockFile(long blockId) {
    final Block b = new Block(blockId);
    File blockfile = null;
    ActiveFile activefile = ongoingCreates.get(b);
    if (activefile != null) {
      blockfile = activefile.file;
    }
    if (blockfile == null) {
      blockfile = getFile(b);
    }
    if (blockfile == null) {
      if (DataNode.LOG.isDebugEnabled()) {
        DataNode.LOG.debug("ongoingCreates=" + ongoingCreates);
        DataNode.LOG.debug("volumeMap=" + volumeMap);
      }
    }
    return blockfile;
  }

  /** {@inheritDoc} */
  public synchronized Block getStoredBlock(long blkid) throws IOException {
    File blockfile = findBlockFile(blkid);
    if (blockfile == null) {
      return null;
    }
    File metafile = findMetaFile(blockfile);
    return new Block(blkid, blockfile.length(),
        parseGenerationStamp(blockfile, metafile));
  }

  public boolean metaFileExists(Block b) throws IOException {
    return getMetaFile(b).exists();
  }
  
  public long getMetaDataLength(Block b) throws IOException {
    File checksumFile = getMetaFile( b );
    return checksumFile.length();
  }

  public MetaDataInputStream getMetaDataInputStream(Block b)
      throws IOException {
    File checksumFile = getMetaFile( b );
    return new MetaDataInputStream(new FileInputStream(checksumFile),
                                                    checksumFile.length());
  }

  FSVolumeSet volumes;
  private HashMap<Block,ActiveFile> ongoingCreates = new HashMap<Block,ActiveFile>();
  private int maxBlocksPerDir = 0;
  private HashMap<Block,DatanodeBlockInfo> volumeMap = null;
  static  Random random = new Random();
  private int validVolsRequired;
  
  /**
   * An FSDataset has a directory where it loads its data files.
   */
  public FSDataset(DataStorage storage, Configuration conf) throws IOException {
    this.maxBlocksPerDir = conf.getInt("dfs.datanode.numblocks", 64);
    // The number of volumes required for operation is the total number 
    // of volumes minus the number of failed volumes we can tolerate.
    final int volFailuresTolerated =
      conf.getInt("dfs.datanode.failed.volumes.tolerated",
                  0);
    this.validVolsRequired = storage.getNumStorageDirs() - volFailuresTolerated; 
    if (validVolsRequired < 1 ||
        validVolsRequired > storage.getNumStorageDirs()) {
      DataNode.LOG.error("Invalid value " + volFailuresTolerated + " for " +
          "dfs.datanode.failed.volumes.tolerated");
    }
    FSVolume[] volArray = new FSVolume[storage.getNumStorageDirs()];
    for (int idx = 0; idx < storage.getNumStorageDirs(); idx++) {
      volArray[idx] = new FSVolume(storage.getStorageDir(idx).getCurrentDir(), conf);
    }
    volumes = new FSVolumeSet(volArray);
    volumeMap = new HashMap<Block, DatanodeBlockInfo>();
    volumes.getVolumeMap(volumeMap);
    registerMBean(storage.getStorageID());
  }

  /**
   * Return the total space used by dfs datanode
   */
  public long getDfsUsed() throws IOException {
    return volumes.getDfsUsed();
  }

  /**
   * Return true - if there are still valid volumes on the DataNode. 
   */
  public boolean hasEnoughResource() {
    return volumes.numberOfVolumes() >= validVolsRequired; 
  }

  /**
   * Return total capacity, used and unused
   */
  public long getCapacity() throws IOException {
    return volumes.getCapacity();
  }

  /**
   * Return how many bytes can still be stored in the FSDataset
   */
  public long getRemaining() throws IOException {
    return volumes.getRemaining();
  }

  /**
   * Find the block's on-disk length
   */
  public long getLength(Block b) throws IOException {
    return getBlockFile(b).length();
  }

  /**
   * Get File name for a given block.
   */
  public synchronized File getBlockFile(Block b) throws IOException {
    File f = validateBlockFile(b);
    if(f == null) {
      if (InterDatanodeProtocol.LOG.isDebugEnabled()) {
        InterDatanodeProtocol.LOG.debug("b=" + b + ", volumeMap=" + volumeMap);
      }
      throw new IOException("Block " + b + " is not valid.");
    }
    return f;
  }
  
  public synchronized InputStream getBlockInputStream(Block b) throws IOException {
    return new FileInputStream(getBlockFile(b));
  }

  public synchronized InputStream getBlockInputStream(Block b, long seekOffset) throws IOException {

    File blockFile = getBlockFile(b);
    RandomAccessFile blockInFile = new RandomAccessFile(blockFile, "r");
    if (seekOffset > 0) {
      blockInFile.seek(seekOffset);
    }
    return new FileInputStream(blockInFile.getFD());
  }

  /**
   * Returns handles to the block file and its metadata file
   */
  public synchronized BlockInputStreams getTmpInputStreams(Block b, 
                          long blkOffset, long ckoff) throws IOException {

    DatanodeBlockInfo info = volumeMap.get(b);
    if (info == null) {
      throw new IOException("Block " + b + " does not exist in volumeMap.");
    }
    FSVolume v = info.getVolume();
    File blockFile = v.getTmpFile(b);
    RandomAccessFile blockInFile = new RandomAccessFile(blockFile, "r");
    if (blkOffset > 0) {
      blockInFile.seek(blkOffset);
    }
    File metaFile = getMetaFile(blockFile, b);
    RandomAccessFile metaInFile = new RandomAccessFile(metaFile, "r");
    if (ckoff > 0) {
      metaInFile.seek(ckoff);
    }
    return new BlockInputStreams(new FileInputStream(blockInFile.getFD()),
                                new FileInputStream(metaInFile.getFD()));
  }
    
  private BlockWriteStreams createBlockWriteStreams( File f , File metafile) throws IOException {
      return new BlockWriteStreams(new FileOutputStream(new RandomAccessFile( f , "rw" ).getFD()),
          new FileOutputStream( new RandomAccessFile( metafile , "rw" ).getFD() ));

  }

  /**
   * Make a copy of the block if this block is linked to an existing
   * snapshot. This ensures that modifying this block does not modify
   * data in any existing snapshots.
   * @param block Block
   * @param numLinks Detach if the number of links exceed this value
   * @throws IOException
   * @return - true if the specified block was detached
   */
  public boolean detachBlock(Block block, int numLinks) throws IOException {
    DatanodeBlockInfo info = null;

    synchronized (this) {
      info = volumeMap.get(block);
    }
    return info.detachBlock(block, numLinks);
  }

  static private <T> void updateBlockMap(Map<Block, T> blockmap,
      Block oldblock, Block newblock) throws IOException {
    if (blockmap.containsKey(oldblock)) {
      T value = blockmap.remove(oldblock);
      blockmap.put(newblock, value);
    }
  }

  /** {@inheritDoc} */
  public void updateBlock(Block oldblock, Block newblock) throws IOException {
    if (oldblock.getBlockId() != newblock.getBlockId()) {
      throw new IOException("Cannot update oldblock (=" + oldblock
          + ") to newblock (=" + newblock + ").");
    }
    
    for(;;) {
      final List<Thread> threads = tryUpdateBlock(oldblock, newblock);
      if (threads == null) {
        return;
      }

      // interrupt and wait for all ongoing create threads
      for(Thread t : threads) {
        t.interrupt();
      }
      for(Thread t : threads) {
        try {
          t.join();
        } catch (InterruptedException e) {
          DataNode.LOG.warn("interruptOngoingCreates: t=" + t, e);
        }
      }
    }
  }

  /**
   * Try to update an old block to a new block.
   * If there are ongoing create threads running for the old block,
   * the threads will be returned without updating the block. 
   * 
   * @return ongoing create threads if there is any. Otherwise, return null.
   */
  private synchronized List<Thread> tryUpdateBlock(
      Block oldblock, Block newblock) throws IOException {
    //check ongoing create threads
    final ActiveFile activefile = ongoingCreates.get(oldblock);
    if (activefile != null && !activefile.threads.isEmpty()) {
      //remove dead threads
      for(Iterator<Thread> i = activefile.threads.iterator(); i.hasNext(); ) {
        final Thread t = i.next();
        if (!t.isAlive()) {
          i.remove();
        }
      }

      //return living threads
      if (!activefile.threads.isEmpty()) {
        return new ArrayList<Thread>(activefile.threads);
      }
    }

    //No ongoing create threads is alive.  Update block.
    File blockFile = findBlockFile(oldblock.getBlockId());
    if (blockFile == null) {
      throw new IOException("Block " + oldblock + " does not exist.");
    }

    File oldMetaFile = findMetaFile(blockFile);
    long oldgs = parseGenerationStamp(blockFile, oldMetaFile);
    
    //rename meta file to a tmp file
    File tmpMetaFile = new File(oldMetaFile.getParent(),
        oldMetaFile.getName()+"_tmp" + newblock.getGenerationStamp());
    if (!oldMetaFile.renameTo(tmpMetaFile)){
      throw new IOException("Cannot rename block meta file to " + tmpMetaFile);
    }

    //update generation stamp
    if (oldgs > newblock.getGenerationStamp()) {
      throw new IOException("Cannot update block (id=" + newblock.getBlockId()
          + ") generation stamp from " + oldgs
          + " to " + newblock.getGenerationStamp());
    }
    
    //update length
    if (newblock.getNumBytes() > oldblock.getNumBytes()) {
      throw new IOException("Cannot update block file (=" + blockFile
          + ") length from " + oldblock.getNumBytes() + " to " + newblock.getNumBytes());
    }
    if (newblock.getNumBytes() < oldblock.getNumBytes()) {
      truncateBlock(blockFile, tmpMetaFile, oldblock.getNumBytes(), newblock.getNumBytes());
    }

    //rename the tmp file to the new meta file (with new generation stamp)
    File newMetaFile = getMetaFile(blockFile, newblock);
    if (!tmpMetaFile.renameTo(newMetaFile)) {
      throw new IOException("Cannot rename tmp meta file to " + newMetaFile);
    }

    updateBlockMap(ongoingCreates, oldblock, newblock);
    updateBlockMap(volumeMap, oldblock, newblock);

    // paranoia! verify that the contents of the stored block 
    // matches the block file on disk.
    validateBlockMetadata(newblock);
    return null;
  }

  static private void truncateBlock(File blockFile, File metaFile,
      long oldlen, long newlen) throws IOException {
    if (newlen == oldlen) {
      return;
    }
    if (newlen > oldlen) {
      throw new IOException("Cannout truncate block to from oldlen (=" + oldlen
          + ") to newlen (=" + newlen + ")");
    }

    DataChecksum dcs = BlockMetadataHeader.readHeader(metaFile).getChecksum(); 
    int checksumsize = dcs.getChecksumSize();
    int bpc = dcs.getBytesPerChecksum();
    long n = (newlen - 1)/bpc + 1;
    long newmetalen = BlockMetadataHeader.getHeaderSize() + n*checksumsize;
    long lastchunkoffset = (n - 1)*bpc;
    int lastchunksize = (int)(newlen - lastchunkoffset); 
    byte[] b = new byte[Math.max(lastchunksize, checksumsize)]; 

    RandomAccessFile blockRAF = new RandomAccessFile(blockFile, "rw");
    try {
      //truncate blockFile 
      blockRAF.setLength(newlen);
 
      //read last chunk
      blockRAF.seek(lastchunkoffset);
      blockRAF.readFully(b, 0, lastchunksize);
    } finally {
      blockRAF.close();
    }

    //compute checksum
    dcs.update(b, 0, lastchunksize);
    dcs.writeValue(b, 0, false);

    //update metaFile 
    RandomAccessFile metaRAF = new RandomAccessFile(metaFile, "rw");
    try {
      metaRAF.setLength(newmetalen);
      metaRAF.seek(newmetalen - checksumsize);
      metaRAF.write(b, 0, checksumsize);
    } finally {
      metaRAF.close();
    }
  }

  private final static String DISK_ERROR = "Possible disk error on file creation: ";
  /** Get the cause of an I/O exception if caused by a possible disk error
   * @param ioe an I/O exception
   * @return cause if the I/O exception is caused by a possible disk error;
   *         null otherwise.
   */ 
  static IOException getCauseIfDiskError(IOException ioe) {
    if (ioe.getMessage()!=null && ioe.getMessage().startsWith(DISK_ERROR)) {
      return (IOException)ioe.getCause();
    } else {
      return null;
    }
  }

  /**
   * Start writing to a block file
   * If isRecovery is true and the block pre-exists, then we kill all
      volumeMap.put(b, v);
      volumeMap.put(b, v);
   * other threads that might be writing to this block, and then reopen the file.
   */
  public BlockWriteStreams writeToBlock(Block b, boolean isRecovery) throws IOException {
    //
    // Make sure the block isn't a valid one - we're still creating it!
    //
    if (isValidBlock(b)) {
      if (!isRecovery) {
        throw new BlockAlreadyExistsException("Block " + b + " is valid, and cannot be written to.");
      }
      // If the block was successfully finalized because all packets
      // were successfully processed at the Datanode but the ack for
      // some of the packets were not received by the client. The client 
      // re-opens the connection and retries sending those packets.
      // The other reason is that an "append" is occurring to this block.
      detachBlock(b, 1);
    }
    long blockSize = b.getNumBytes();

    //
    // Serialize access to /tmp, and check if file already there.
    //
    File f = null;
    List<Thread> threads = null;
    synchronized (this) {
      //
      // Is it already in the create process?
      //
      ActiveFile activeFile = ongoingCreates.get(b);
      if (activeFile != null) {
        f = activeFile.file;
        threads = activeFile.threads;
        
        if (!isRecovery) {
          throw new BlockAlreadyExistsException("Block " + b +
                                  " has already been started (though not completed), and thus cannot be created.");
        } else {
          for (Thread thread:threads) {
            thread.interrupt();
          }
        }
        ongoingCreates.remove(b);
      }
      FSVolume v = null;
      if (!isRecovery) {
        v = volumes.getNextVolume(blockSize);
        // create temporary file to hold block in the designated volume
        f = createTmpFile(v, b);
        volumeMap.put(b, new DatanodeBlockInfo(v));
      } else if (f != null) {
        DataNode.LOG.info("Reopen already-open Block for append " + b);
        // create or reuse temporary file to hold block in the designated volume
        v = volumeMap.get(b).getVolume();
        volumeMap.put(b, new DatanodeBlockInfo(v));
      } else {
        // reopening block for appending to it.
        DataNode.LOG.info("Reopen Block for append " + b);
        v = volumeMap.get(b).getVolume();
        f = createTmpFile(v, b);
        File blkfile = getBlockFile(b);
        File oldmeta = getMetaFile(b);
        File newmeta = getMetaFile(f, b);

        // rename meta file to tmp directory
        DataNode.LOG.debug("Renaming " + oldmeta + " to " + newmeta);
        if (!oldmeta.renameTo(newmeta)) {
          throw new IOException("Block " + b + " reopen failed. " +
                                " Unable to move meta file  " + oldmeta +
                                " to tmp dir " + newmeta);
        }

        // rename block file to tmp directory
        DataNode.LOG.debug("Renaming " + blkfile + " to " + f);
        if (!blkfile.renameTo(f)) {
          if (!f.delete()) {
            throw new IOException("Block " + b + " reopen failed. " +
                                  " Unable to remove file " + f);
          }
          if (!blkfile.renameTo(f)) {
            throw new IOException("Block " + b + " reopen failed. " +
                                  " Unable to move block file " + blkfile +
                                  " to tmp dir " + f);
          }
        }
        volumeMap.put(b, new DatanodeBlockInfo(v));
      }
      if (f == null) {
        DataNode.LOG.warn("Block " + b + " reopen failed " +
                          " Unable to locate tmp file.");
        throw new IOException("Block " + b + " reopen failed " +
                              " Unable to locate tmp file.");
      }
      ongoingCreates.put(b, new ActiveFile(f, threads));
    }

    try {
      if (threads != null) {
        for (Thread thread:threads) {
          thread.join();
        }
      }
    } catch (InterruptedException e) {
      throw new IOException("Recovery waiting for thread interrupted.");
    }

    //
    // Finally, allow a writer to the block file
    // REMIND - mjc - make this a filter stream that enforces a max
    // block size, so clients can't go crazy
    //
    File metafile = getMetaFile(f, b);
    DataNode.LOG.debug("writeTo blockfile is " + f + " of size " + f.length());
    DataNode.LOG.debug("writeTo metafile is " + metafile + " of size " + metafile.length());
    return createBlockWriteStreams( f , metafile);
  }

  /**
   * Retrieves the offset in the block to which the
   * the next write will write data to.
   */
  public long getChannelPosition(Block b, BlockWriteStreams streams) 
                                 throws IOException {
    FileOutputStream file = (FileOutputStream) streams.dataOut;
    return file.getChannel().position();
  }

  /**
   * Sets the offset in the block to which the
   * the next write will write data to.
   */
  public void setChannelPosition(Block b, BlockWriteStreams streams, 
                                 long dataOffset, long ckOffset) 
                                 throws IOException {
    long size = 0;
    synchronized (this) {
      FSVolume vol = volumeMap.get(b).getVolume();
      size = vol.getTmpFile(b).length();
    }
    if (size < dataOffset) {
      String msg = "Trying to change block file offset of block " + b +
                     " to " + dataOffset +
                     " but actual size of file is " +
                     size;
      throw new IOException(msg);
    }
    FileOutputStream file = (FileOutputStream) streams.dataOut;
    file.getChannel().position(dataOffset);
    file = (FileOutputStream) streams.checksumOut;
    file.getChannel().position(ckOffset);
  }

  synchronized File createTmpFile( FSVolume vol, Block blk ) throws IOException {
    if ( vol == null ) {
      vol = volumeMap.get( blk ).getVolume();
      if ( vol == null ) {
        throw new IOException("Could not find volume for block " + blk);
      }
    }
    return vol.createTmpFile(blk);
  }

  //
  // REMIND - mjc - eventually we should have a timeout system
  // in place to clean up block files left by abandoned clients.
  // We should have some timer in place, so that if a blockfile
  // is created but non-valid, and has been idle for >48 hours,
  // we can GC it safely.
  //

  /**
   * Complete the block write!
   */
  public synchronized void finalizeBlock(Block b) throws IOException {
    ActiveFile activeFile = ongoingCreates.get(b);
    if (activeFile == null) {
      throw new IOException("Block " + b + " is already finalized.");
    }
    File f = activeFile.file;
    if (f == null || !f.exists()) {
      throw new IOException("No temporary file " + f + " for block " + b);
    }
    FSVolume v = volumeMap.get(b).getVolume();
    if (v == null) {
      throw new IOException("No volume for temporary file " + f + 
                            " for block " + b);
    }
        
    File dest = null;
    dest = v.addBlock(b, f);
    volumeMap.put(b, new DatanodeBlockInfo(v, dest));
    ongoingCreates.remove(b);
  }

  /**
   * Remove the temporary block file (if any)
   */
  public synchronized void unfinalizeBlock(Block b) throws IOException {
    // remove the block from in-memory data structure
    ActiveFile activefile = ongoingCreates.remove(b);
    if (activefile == null) {
      return;
    }
    volumeMap.remove(b);
    
    // delete the on-disk temp file
    if (delBlockFromDisk(activefile.file, getMetaFile(activefile.file, b), b)) {
      DataNode.LOG.warn("Block " + b + " unfinalized and removed. " );
    }
  }

  /**
   * Remove a block from disk
   * @param blockFile block file
   * @param metaFile block meta file
   * @param b a block
   * @return true if on-disk files are deleted; false otherwise
   */
  private boolean delBlockFromDisk(File blockFile, File metaFile, Block b) {
    if (blockFile == null) {
      DataNode.LOG.warn("No file exists for block: " + b);
      return true;
    }
    
    if (!blockFile.delete()) {
      DataNode.LOG.warn("Not able to delete the block file: " + blockFile);
      return false;
    } else { // remove the meta file
      if (metaFile != null && !metaFile.delete()) {
        DataNode.LOG.warn(
            "Not able to delete the meta block file: " + metaFile);
        return false;
      }
    }
    return true;
  }
  
  /**
   * Return a table of block data
   */
  public Block[] getBlockReport() {
    TreeSet<Block> blockSet = new TreeSet<Block>();
    volumes.getBlockInfo(blockSet);
    Block blockTable[] = new Block[blockSet.size()];
    int i = 0;
    for (Iterator<Block> it = blockSet.iterator(); it.hasNext(); i++) {
      blockTable[i] = it.next();
    }
    return blockTable;
  }

  /**
   * Check whether the given block is a valid one.
   */
  public boolean isValidBlock(Block b) {
    File f = null;;
    try {
      f = validateBlockFile(b);
    } catch(IOException e) {
      Log.warn("Block " + b + " is not valid:",e);
    }
    
    return f != null;
  }

  /**
   * Find the file corresponding to the block and return it if it exists.
   */
  File validateBlockFile(Block b) throws IOException {
    //Should we check for metadata file too?
    File f = getFile(b);
    
    if(f != null ) {
      if(f.exists())
        return f;
   
      // if file is not null, but doesn't exist - possibly disk failed
      DataNode datanode = DataNode.getDataNode();
      datanode.checkDiskError();
    }
    
    if (InterDatanodeProtocol.LOG.isDebugEnabled()) {
      InterDatanodeProtocol.LOG.debug("b=" + b + ", f=" + f);
    }
    return null;
  }

  /** {@inheritDoc} */
  public void validateBlockMetadata(Block b) throws IOException {
    DatanodeBlockInfo info = volumeMap.get(b);
    if (info == null) {
      throw new IOException("Block " + b + " does not exist in volumeMap.");
    }
    FSVolume v = info.getVolume();
    File tmp = v.getTmpFile(b);
    File f = getFile(b);
    if (f == null) {
      f = tmp;
    }
    if (f == null) {
      throw new IOException("Block " + b + " does not exist on disk.");
    }
    if (!f.exists()) {
      throw new IOException("Block " + b + 
                            " block file " + f +
                            " does not exist on disk.");
    }
    if (b.getNumBytes() != f.length()) {
      throw new IOException("Block " + b + 
                            " length is " + b.getNumBytes()  +
                            " does not match block file length " +
                            f.length());
    }
    File meta = getMetaFile(f, b);
    if (meta == null) {
      throw new IOException("Block " + b + 
                            " metafile does not exist.");
    }
    if (!meta.exists()) {
      throw new IOException("Block " + b + 
                            " metafile " + meta +
                            " does not exist on disk.");
    }
    if (meta.length() == 0) {
      throw new IOException("Block " + b + " metafile " + meta + " is empty.");
    }
    long stamp = parseGenerationStamp(f, meta);
    if (stamp != b.getGenerationStamp()) {
      throw new IOException("Block " + b + 
                            " genstamp is " + b.getGenerationStamp()  +
                            " does not match meta file stamp " +
                            stamp);
    }
  }

  /**
   * We're informed that a block is no longer valid.  We
   * could lazily garbage-collect the block, but why bother?
   * just get rid of it.
   */
  public void invalidate(Block invalidBlks[]) throws IOException {
    boolean error = false;
    for (int i = 0; i < invalidBlks.length; i++) {
      File f = null;
      FSVolume v;
      synchronized (this) {
        f = getFile(invalidBlks[i]);
        DatanodeBlockInfo dinfo = volumeMap.get(invalidBlks[i]);
        if (dinfo == null) {
          DataNode.LOG.warn("Unexpected error trying to delete block "
                           + invalidBlks[i] + 
                           ". BlockInfo not found in volumeMap.");
          error = true;
          continue;
        }
        v = dinfo.getVolume();
        if (f == null) {
          DataNode.LOG.warn("Unexpected error trying to delete block "
                            + invalidBlks[i] + 
                            ". Block not found in blockMap." +
                            ((v == null) ? " " : " Block found in volumeMap."));
          error = true;
          continue;
        }
        if (v == null) {
          DataNode.LOG.warn("Unexpected error trying to delete block "
                            + invalidBlks[i] + 
                            ". No volume for this block." +
                            " Block found in blockMap. " + f + ".");
          error = true;
          continue;
        }
        File parent = f.getParentFile();
        if (parent == null) {
          DataNode.LOG.warn("Unexpected error trying to delete block "
                            + invalidBlks[i] + 
                            ". Parent not found for file " + f + ".");
          error = true;
          continue;
        }
        v.clearPath(parent);
        volumeMap.remove(invalidBlks[i]);
      }
      File metaFile = getMetaFile( f, invalidBlks[i] );
      long blockSize = f.length()+metaFile.length();
      if ( !f.delete() || ( !metaFile.delete() && metaFile.exists() ) ) {
        DataNode.LOG.warn("Unexpected error trying to delete block "
                          + invalidBlks[i] + " at file " + f);
        error = true;
        continue;
      }
      v.decDfsUsed(blockSize);
      DataNode.LOG.info("Deleting block " + invalidBlks[i] + " file " + f);
      if (f.exists()) {
        //
        // This is a temporary check especially for hadoop-1220. 
        // This will go away in the future.
        //
        DataNode.LOG.info("File " + f + " was deleted but still exists!");
      }
    }
    if (error) {
      throw new IOException("Error in deleting blocks.");
    }
  }

  /**
   * Turn the block identifier into a filename.
   */
  public synchronized File getFile(Block b) {
    DatanodeBlockInfo info = volumeMap.get(b);
    if (info != null) {
      return info.getFile();
    }
    return null;
  }

  /**
   * check if a data directory is healthy
   * if some volumes failed - make sure to remove all the blocks that belong
   * to these volumes
   * @throws DiskErrorException
   */
  public void checkDataDir() throws DiskErrorException {
    long total_blocks=0, removed_blocks=0;
    List<FSVolume> failed_vols =  volumes.checkDirs();
    
    //if there no failed volumes return
    if(failed_vols == null) 
      return;
    
    // else 
    // remove related blocks
    long mlsec = System.currentTimeMillis();
    synchronized (this) {
      Iterator<Block> ib = volumeMap.keySet().iterator();
      while(ib.hasNext()) {
        Block b = ib.next();
        total_blocks ++;
        // check if the volume block belongs to still valid
        FSVolume vol = volumeMap.get(b).getVolume();
        for(FSVolume fv: failed_vols) {
          if(vol == fv) {
            DataNode.LOG.warn("removing block " + b.getBlockId() + " from vol " 
                + vol.dataDir.dir.getAbsolutePath());
            ib.remove();
            removed_blocks++;
            break;
          }
        }
      }
    } // end of sync
    mlsec = System.currentTimeMillis() - mlsec;
    DataNode.LOG.warn(">>>>>>>>>>>>Removed " + removed_blocks + " out of " + total_blocks +
        "(took " + mlsec + " millisecs)");

    // report the error
    StringBuilder sb = new StringBuilder();
    for(FSVolume fv : failed_vols) {
      sb.append(fv.dataDir.dir.getAbsolutePath() + ";");
    }

    throw  new DiskErrorException("DataNode failed volumes:" + sb);
  
  }
    

  public String toString() {
    return "FSDataset{dirpath='"+volumes+"'}";
  }

  private ObjectName mbeanName;
  private Random rand = new Random();
  
  /**
   * Register the FSDataset MBean using the name
   *        "hadoop:service=DataNode,name=FSDatasetState-<storageid>"
   */
  void registerMBean(final String storageId) {
    // We wrap to bypass standard mbean naming convetion.
    // This wraping can be removed in java 6 as it is more flexible in 
    // package naming for mbeans and their impl.
    StandardMBean bean;
    String storageName;
    if (storageId == null || storageId.equals("")) {// Temp fix for the uninitialized storage
      storageName = "UndefinedStorageId" + rand.nextInt();
    } else {
      storageName = storageId;
    }
    try {
      bean = new StandardMBean(this,FSDatasetMBean.class);
      mbeanName = MBeans.register("DataNode", "FSDatasetState-" + storageName, bean);
    } catch (NotCompliantMBeanException e) {
      e.printStackTrace();
    }
 
    DataNode.LOG.info("Registered FSDatasetStatusMBean");
  }

  public void shutdown() {
    if (mbeanName != null)
      MBeans.unregister(mbeanName);
    
    if(volumes != null) {
      for (FSVolume volume : volumes.volumes) {
        if(volume != null) {
          volume.dfsUsage.shutdown();
        }
      }
    }
  }

  public String getStorageInfo() {
    return toString();
  }
  
  /**
   * Class for representing the Datanode volume information
   */
  static class VolumeInfo {
    final String directory;
    final long usedSpace;
    final long freeSpace;
    final long reservedSpace;

    VolumeInfo(String dir, long usedSpace, long freeSpace, long reservedSpace) {
      this.directory = dir;
      this.usedSpace = usedSpace;
      this.freeSpace = freeSpace;
      this.reservedSpace = reservedSpace;
    }
  }  
  
  synchronized Collection<VolumeInfo> getVolumeInfo() {
    Collection<VolumeInfo> info = new ArrayList<VolumeInfo>();
    synchronized(volumes.volumes) {
      for (FSVolume volume : volumes.volumes) {
        long used = 0;
        try {
          used = volume.getDfsUsed();
        } catch (IOException e) {
          DataNode.LOG.warn(e.getMessage());
        }
        
        long free= 0;
        try {
          free = volume.getAvailable();
        } catch (IOException e) {
          DataNode.LOG.warn(e.getMessage());
        }
        
        info.add(new VolumeInfo(volume.toString(), used, free, 
            volume.getReserved()));
      }
      return info;
    }
  }
}
