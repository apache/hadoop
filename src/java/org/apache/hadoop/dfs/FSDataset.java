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

import java.io.*;
import java.util.*;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.dfs.datanode.metrics.FSDatasetMBean;

/**************************************************
 * FSDataset manages a set of data blocks.  Each block
 * has a unique name and an extent on disk.
 *
 ***************************************************/
class FSDataset implements FSConstants, FSDatasetInterface {


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
        File metaData = getMetaFile( src );
        if ( ! metaData.renameTo( getMetaFile(dest) ) ||
            ! src.renameTo( dest ) ) {
          throw new IOException( "could not move files for " + b +
                                 " from tmp to " + 
                                 dest.getAbsolutePath() );
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
          blockSet.add(new Block(blockFiles[i], blockFiles[i].length()));
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
        //We are not enforcing presense of metadata file
        if (Block.isBlockFilename(blockFiles[i])) {
          volumeMap.put(new Block(blockFiles[i], blockFiles[i].length()), 
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
    static final double USABLE_DISK_PCT_DEFAULT = 0.98f; 

    private FSDir dataDir;
    private File tmpDir;
    private File detachDir; // copy on write for blocks in snapshot
    private DF usage;
    private DU dfsUsage;
    private long reserved;
    private double usableDiskPct = USABLE_DISK_PCT_DEFAULT;

    
    FSVolume(File currentDir, Configuration conf) throws IOException {
      this.reserved = conf.getLong("dfs.datanode.du.reserved", 0);
      this.usableDiskPct = conf.getFloat("dfs.datanode.du.pct",
                                         (float) USABLE_DISK_PCT_DEFAULT);
      File parent = currentDir.getParentFile();

      this.detachDir = new File(parent, "detach");
      if (detachDir.exists()) {
        recoverDetachedBlocks(currentDir, detachDir);
      }
      this.dataDir = new FSDir(currentDir);
      this.tmpDir = new File(parent, "tmp");
      if (tmpDir.exists()) {
        FileUtil.fullyDelete(tmpDir);
      }
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
    }

    void decDfsUsed(long value) {
      dfsUsage.decDfsUsed(value);
    }
    
    long getDfsUsed() throws IOException {
      return dfsUsage.getUsed();
    }
    
    long getCapacity() throws IOException {
      return usage.getCapacity();
    }
      
    long getAvailable() throws IOException {
      long remaining = getCapacity()-getDfsUsed()-reserved;
      long available = usage.getAvailable();
      if (remaining>available) {
        remaining = available;
      }
      return (remaining > 0) ? (long)(remaining * usableDiskPct) : 0;
    }
      
    String getMount() throws IOException {
      return usage.getMount();
    }
      
    File getDir() {
      return dataDir.dir;
    }
    
    /**
     * Temporary files. They get deleted when the datanode restarts
     */
    File createTmpFile(Block b) throws IOException {
      File f = new File(tmpDir, b.getBlockName());
      return createTmpFile(b, f);
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
      if (!f.createNewFile()) {
        throw new IOException("Unexpected problem in creating temporary file for "+
                              b + ".  File " + f + " should be creatable, but is already present.");
      }
      return f;
    }
      
    File addBlock(Block b, File f) throws IOException {
      File blockFile = dataDir.addBlock(b, f);
      File metaFile = getMetaFile( blockFile );
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
      
    synchronized FSVolume getNextVolume(long blockSize) throws IOException {
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
      
    synchronized void checkDirs() throws DiskErrorException {
      for (int idx = 0; idx < volumes.length; idx++) {
        volumes[idx].checkDirs();
      }
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
    
  static File getMetaFile( File f ) {
    return new File( f.getAbsolutePath() + METADATA_EXTENSION );
  }

  static class ActiveFile {
    File file;
    List<Thread> threads = new ArrayList<Thread>(2);

    ActiveFile(File f, List<Thread> list) {
      file = f;
      if (list != null) {
        threads.addAll(list);
      }
      threads.add(Thread.currentThread());
    }
  } 
  
  protected File getMetaFile(Block b) throws IOException {
    File blockFile = getBlockFile( b );
    return new File( blockFile.getAbsolutePath() + METADATA_EXTENSION ); 
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
  
  long blockWriteTimeout = 3600 * 1000;
  
  /**
   * An FSDataset has a directory where it loads its data files.
   */
  public FSDataset(DataStorage storage, Configuration conf) throws IOException {
    this.maxBlocksPerDir = conf.getInt("dfs.datanode.numblocks", 64);
    FSVolume[] volArray = new FSVolume[storage.getNumStorageDirs()];
    for (int idx = 0; idx < storage.getNumStorageDirs(); idx++) {
      volArray[idx] = new FSVolume(storage.getStorageDir(idx).getCurrentDir(), conf);
    }
    volumes = new FSVolumeSet(volArray);
    volumeMap = new HashMap<Block, DatanodeBlockInfo>();
    volumes.getVolumeMap(volumeMap);
    blockWriteTimeout = Math.max(
         conf.getInt("dfs.datanode.block.write.timeout.sec", 3600), 1) * 1000;
    registerMBean(storage.getStorageID());
  }

  /**
   * Return the total space used by dfs datanode
   */
  public long getDfsUsed() throws IOException {
    return volumes.getDfsUsed();
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
    if (!isValidBlock(b)) {
      throw new IOException("Block " + b + " is not valid.");
    }
    File f = getFile(b);
    return f.length();
  }

  /**
   * Get File name for a given block.
   */
  protected synchronized File getBlockFile(Block b) throws IOException {
    if (!isValidBlock(b)) {
      throw new IOException("Block " + b + " is not valid.");
    }
    return getFile(b);
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
    
  BlockWriteStreams createBlockWriteStreams( File f ) throws IOException {
      return new BlockWriteStreams(new FileOutputStream(new RandomAccessFile( f , "rw" ).getFD()),
          new FileOutputStream( new RandomAccessFile( getMetaFile( f ) , "rw" ).getFD() ));

  }

  /**
   * Make a copy of the block if this block is linked to an existing
   * snapshot. This ensures that modifying this block does not modify
   * data in any existing snapshots.
   * @param b Block
   * @param numLinks Detach if the number of links exceed this value
   * @throws IOException
   * @return - true if the specified block was detached
   */
  boolean detachBlock(Block block, int numLinks) throws IOException {
    DatanodeBlockInfo info = null;

    synchronized (this) {
      info = volumeMap.get(block);
    }
    return info.detachBlock(block, numLinks);
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
        throw new IOException("Block " + b + " is valid, and cannot be written to.");
      }
      // If the block was succesfully finalized because all packets
      // were successfully processed at the Datanode but the ack for
      // some of the packets were not received by the client. The client 
      // re-opens the connection and retries sending those packets.
      // 
      DataNode.LOG.info("Reopen Block " + b);
      return null;
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
          // check how old is the temp file - wait 1 hour
          if ((System.currentTimeMillis() - f.lastModified()) < 
              blockWriteTimeout) {
            throw new IOException("Block " + b +
                                  " has already been started (though not completed), and thus cannot be created.");
          } else {
            // stale temp file - remove
            if (!f.delete()) {
              throw new IOException("Can't write the block - unable to remove stale temp file " + f);
            }
            f = null;
          }
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
        // Do not insert temporary file into volume map.
        f = createTmpFile(v, b);
        volumeMap.put(b, new DatanodeBlockInfo(v));
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
    return createBlockWriteStreams( f );
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
    File f = ongoingCreates.get(b).file;
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
    ongoingCreates.remove(b);
    volumeMap.remove(b);
    DataNode.LOG.warn("Block " + b + " unfinalized and removed. " );
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
    //Should we check for metadata file too?
    File f = getFile(b);
    return (f!= null && f.exists());
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
        v = volumeMap.get(invalidBlks[i]).getVolume();
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
      File metaFile = getMetaFile( f );
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
  synchronized File getFile(Block b) {
    DatanodeBlockInfo info = volumeMap.get(b);
    if (info != null) {
      return info.getFile();
    }
    return null;
  }

  /**
   * check if a data diretory is healthy
   * @throws DiskErrorException
   */
  public void checkDataDir() throws DiskErrorException {
    volumes.checkDirs();
  }
    

  public String toString() {
    return "FSDataset{dirpath='"+volumes+"'}";
  }

  private ObjectName mbeanName;
  private Random rand = new Random();
  /**
   * Register the FSDataset MBean
   */
  void registerMBean(final String storageId) {
    // We wrap to bypass standard mbean naming convetion.
    // This wraping can be removed in java 6 as it is more flexible in 
    // package naming for mbeans and their impl.
    StandardMBean bean;
    String serverName;
    if (storageId.equals("")) {// Temp fix for the uninitialized storage
      serverName = "DataNode-UndefinedStorageId" + rand.nextInt();
    } else {
      serverName = "DataNode-" + storageId;
    }
    try {
      bean = new StandardMBean(this,FSDatasetMBean.class);
      mbeanName = MBeanUtil.registerMBean(serverName, "FSDatasetStatus", bean);
    } catch (NotCompliantMBeanException e) {
      e.printStackTrace();
    }
 
    DataNode.LOG.info("Registered FSDatasetStatusMBean");
  }

  public void shutdown() {
    if (mbeanName != null)
      MBeanUtil.unregisterMBean(mbeanName);
  }

  public String getStorageInfo() {
    return toString();
  }
  
  public long getBlockSize(Block b) {
    return getFile(b).length();
  }
}
