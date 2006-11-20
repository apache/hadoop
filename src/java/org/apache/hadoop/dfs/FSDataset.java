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

import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.conf.*;

/**************************************************
 * FSDataset manages a set of data blocks.  Each block
 * has a unique name and an extent on disk.
 *
 * @author Mike Cafarella
 ***************************************************/
class FSDataset implements FSConstants {


  /**
     * A node type that can be built into a tree reflecting the
     * hierarchy of blocks on the local disk.
     */
    class FSDir {
        File dir;
        int numBlocks = 0;
        int myIdx = 0;
        FSDir children[];
        FSDir siblings[];

        /**
         */
        public FSDir(File dir, int myIdx, FSDir[] siblings) 
            throws IOException {
            this.dir = dir;
            this.myIdx = myIdx;
            this.siblings = siblings;
            this.children = null;
            if (! dir.exists()) {
              if (! dir.mkdirs()) {
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
                    children[curdir] = new FSDir(files[idx], curdir, children);
                    curdir++;
                  }
                }
              }
            }
        }

        /**
         */
        public File addBlock(Block b, File src) throws IOException {
            if (numBlocks < maxBlocksPerDir) {
              File dest = new File(dir, b.getBlockName());
              src.renameTo(dest);
              numBlocks += 1;
              return dest;
            } else {
              if (siblings != null && myIdx != (siblings.length-1)) {
                File dest = siblings[myIdx+1].addBlock(b, src);
                if (dest != null) { return dest; }
              }
              if (children == null) {
                children = new FSDir[maxBlocksPerDir];
                for (int idx = 0; idx < maxBlocksPerDir; idx++) {
                  children[idx] = new FSDir(
                      new File(dir, "subdir"+idx), idx, children);
                }
              }
              return children[0].addBlock(b, src);
            }
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


        void getVolumeMap(HashMap<Block, FSVolume> volumeMap, FSVolume volume) {
          if (children != null) {
                for (int i = 0; i < children.length; i++) {
                    children[i].getVolumeMap(volumeMap, volume);
                }
            }

            File blockFiles[] = dir.listFiles();
            for (int i = 0; i < blockFiles.length; i++) {
                if (Block.isBlockFilename(blockFiles[i])) {
                    volumeMap.put(new Block(blockFiles[i], blockFiles[i].length()), volume);
                }
            }
        }
        
        void getBlockMap(HashMap<Block, File> blockMap) {
          if (children != null) {
                for (int i = 0; i < children.length; i++) {
                    children[i].getBlockMap(blockMap);
                }
            }

            File blockFiles[] = dir.listFiles();
            for (int i = 0; i < blockFiles.length; i++) {
                if (Block.isBlockFilename(blockFiles[i])) {
                    blockMap.put(new Block(blockFiles[i], blockFiles[i].length()), blockFiles[i]);
                }
            }
        }
        /**
         * check if a data diretory is healthy
         * @throws DiskErrorException
         * @author hairong
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
          if (dir.compareTo(f) == 0) numBlocks--;
          else {
            if ((siblings != null) && (myIdx != (siblings.length - 1)))
              siblings[myIdx + 1].clearPath(f);
            else if (children != null)
              children[0].clearPath(f);
          }
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

      private File dir;
      private FSDir dataDir;
      private File tmpDir;
      private DF usage;
      private long reserved;
      private double usableDiskPct = USABLE_DISK_PCT_DEFAULT;
    
      FSVolume(File dir, Configuration conf) throws IOException {
        this.reserved = conf.getLong("dfs.datanode.du.reserved", 0);
        this.usableDiskPct = conf.getFloat("dfs.datanode.du.pct",
            (float) USABLE_DISK_PCT_DEFAULT);
        this.dir = dir;
        this.dataDir = new FSDir(new File(dir, "data"), 0, null);
        this.tmpDir = new File(dir, "tmp");
        if (tmpDir.exists()) {
          FileUtil.fullyDelete(tmpDir);
        }
        if (!tmpDir.mkdirs()) {
          if (!tmpDir.isDirectory()) {
            throw new IOException("Mkdirs failed to create " + tmpDir.toString());
          }
        }
        this.usage = new DF(dir, conf);
      }
      
      long getCapacity() throws IOException {
        return usage.getCapacity();
      }
      
      long getAvailable() throws IOException {
        return ((long) Math.round(usableDiskPct *
                usage.getAvailable()) - reserved);
      }
      
      String getMount() throws IOException {
        return usage.getMount();
      }
      
      File createTmpFile(Block b) throws IOException {
        File f = new File(tmpDir, b.getBlockName());
        try {
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
        } catch (IOException ie) {
          System.out.println("Exception!  " + ie);
          throw ie;
        }
        reserved -= b.getNumBytes();
        return f;
      }
      
      File addBlock(Block b, File f) throws IOException {
        return dataDir.addBlock(b, f);
      }
      
      void checkDirs() throws DiskErrorException {
        dataDir.checkDirTree();
        DiskChecker.checkDir(tmpDir);
      }
      
      void getBlockInfo(TreeSet<Block> blockSet) {
        dataDir.getBlockInfo(blockSet);
      }
      
      void getVolumeMap(HashMap<Block, FSVolume> volumeMap) {
        dataDir.getVolumeMap(volumeMap, this);
      }
      
      void getBlockMap(HashMap<Block, File> blockMap) {
        dataDir.getBlockMap(blockMap);
      }
      
      void clearPath(File f) {
        dataDir.clearPath(f);
      }
      
      public String toString() {
        return dir.getAbsolutePath();
      }
    }
    
    class FSVolumeSet {
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
          if (volume.getAvailable() >= blockSize) { return volume; }
          if (curVolume == startVolume) {
            throw new DiskOutOfSpaceException("Insufficient space for an additional block");
          }
        }
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
      
      synchronized void getVolumeMap(HashMap<Block, FSVolume> volumeMap) {
        for (int idx = 0; idx < volumes.length; idx++) {
          volumes[idx].getVolumeMap(volumeMap);
        }
      }
      
      synchronized void getBlockMap(HashMap<Block, File> blockMap) {
        for (int idx = 0; idx < volumes.length; idx++) {
          volumes[idx].getBlockMap(blockMap);
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

    FSVolumeSet volumes;
    private HashMap<Block,File> ongoingCreates = new HashMap<Block,File>();
    private int maxBlocksPerDir = 0;
    private HashMap<Block,FSVolume> volumeMap = null;
    private HashMap<Block,File> blockMap = null;

    /**
     * An FSDataset has a directory where it loads its data files.
     */
    public FSDataset(File[] dirs, Configuration conf) throws IOException {
    	this.maxBlocksPerDir = conf.getInt("dfs.datanode.numblocks", 64);
        FSVolume[] volArray = new FSVolume[dirs.length];
        for (int idx = 0; idx < dirs.length; idx++) {
          volArray[idx] = new FSVolume(dirs[idx], conf);
        }
        volumes = new FSVolumeSet(volArray);
        volumeMap = new HashMap<Block,FSVolume>();
        volumes.getVolumeMap(volumeMap);
        blockMap = new HashMap<Block,File>();
        volumes.getBlockMap(blockMap);
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
        if (! isValidBlock(b)) {
            throw new IOException("Block " + b + " is not valid.");
        }
        File f = getFile(b);
        return f.length();
    }

    /**
     * Get a stream of data from the indicated block.
     */
    public synchronized InputStream getBlockData(Block b) throws IOException {
        if (! isValidBlock(b)) {
            throw new IOException("Block " + b + " is not valid.");
        }
        // File should be opened with the lock.
        return new FileInputStream(getFile(b));
    }

    /**
     * Start writing to a block file
     */
    public OutputStream writeToBlock(Block b) throws IOException {
        //
        // Make sure the block isn't a valid one - we're still creating it!
        //
        if (isValidBlock(b)) {
            throw new IOException("Block " + b + " is valid, and cannot be written to.");
        }
        long blockSize = b.getNumBytes();

        //
        // Serialize access to /tmp, and check if file already there.
        //
        File f = null;
        synchronized ( this ) {
            //
            // Is it already in the create process?
            //
            if (ongoingCreates.containsKey(b)) {
                throw new IOException("Block " + b +
                    " has already been started (though not completed), and thus cannot be created.");
            }
            FSVolume v = null;
            synchronized ( volumes ) {
              v = volumes.getNextVolume(blockSize);
              // create temporary file to hold block in the designated volume
              f = v.createTmpFile(b);
            }
            ongoingCreates.put(b, f);
            volumeMap.put(b, v);
        }

        //
        // Finally, allow a writer to the block file
        // REMIND - mjc - make this a filter stream that enforces a max
        // block size, so clients can't go crazy
        //
        return new FileOutputStream(f);
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
        File f = ongoingCreates.get(b);
        if (f == null || ! f.exists()) {
          throw new IOException("No temporary file " + f + " for block " + b);
        }
        long finalLen = f.length();
        b.setNumBytes(finalLen);
        FSVolume v = volumeMap.get(b);
        
        File dest = null;
        synchronized ( volumes ) {
          dest = v.addBlock(b, f);
        }
        blockMap.put(b, dest);
        ongoingCreates.remove(b);
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
        File f = getFile(b);
        return (f!= null && f.exists());
    }

    /**
     * We're informed that a block is no longer valid.  We
     * could lazily garbage-collect the block, but why bother?
     * just get rid of it.
     */
    public void invalidate(Block invalidBlks[]) throws IOException {
      for (int i = 0; i < invalidBlks.length; i++) {
        File f;
        synchronized (this) {
          f = getFile(invalidBlks[i]);
          FSVolume v = volumeMap.get(invalidBlks[i]);
          v.clearPath(f.getParentFile());
          blockMap.remove(invalidBlks[i]);
          volumeMap.remove(invalidBlks[i]);
        }
        if (!f.delete()) {
            throw new IOException("Unexpected error trying to delete block "
                                  + invalidBlks[i] + " at file " + f);
        }
        DataNode.LOG.info("Deleting block " + invalidBlks[i]);
      }
    }

    /**
     * Turn the block identifier into a filename.
     */
    synchronized File getFile(Block b) {
      return blockMap.get(b);
    }

    /**
     * check if a data diretory is healthy
     * @throws DiskErrorException
     * @author hairong
     */
    void checkDataDir() throws DiskErrorException {
        volumes.checkDirs();
    }
    

    public String toString() {
      return "FSDataset{dirpath='"+volumes+"'}";
    }

}
