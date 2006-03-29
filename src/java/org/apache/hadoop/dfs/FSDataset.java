/**
 * Copyright 2005 The Apache Software Foundation
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

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

/**************************************************
 * FSDataset manages a set of data blocks.  Each block
 * has a unique name and an extent on disk.
 *
 * @author Mike Cafarella
 ***************************************************/
class FSDataset implements FSConstants {
    static final double USABLE_DISK_PCT = 0.98;

  /**
     * A node type that can be built into a tree reflecting the
     * hierarchy of blocks on the local disk.
     */
    class FSDir {
        File dir;
        FSDir children[];

        /**
         */
        public FSDir(File dir) {
            this.dir = dir;
            this.children = null;
        }

        /**
         */
        public File getDirName() {
            return dir;
        }

        /**
         */
        public FSDir[] getChildren() {
            return children;
        }

        /**
         */
        public void addBlock(Block b, File src) {
            addBlock(b, src, b.getBlockId(), 0);
        }

        /**
         */
        void addBlock(Block b, File src, long blkid, int depth) {
            //
            // Add to the local dir, if no child dirs
            //
            if (children == null) {
                src.renameTo(new File(dir, b.getBlockName()));

                //
                // Test whether this dir's contents should be busted 
                // up into subdirs.
                //

                // REMIND - mjc - sometime soon, we'll want this code
                // working.  It prevents the datablocks from all going
                // into a single huge directory.
                /**
                File localFiles[] = dir.listFiles();
                if (localFiles.length == 16) {
                    //
                    // Create all the necessary subdirs
                    //
                    this.children = new FSDir[16];
                    for (int i = 0; i < children.length; i++) {
                        String str = Integer.toBinaryString(i);
                        try {
                            File subdir = new File(dir, "dir_" + str);
                            subdir.mkdir();
                            children[i] = new FSDir(subdir);
                        } catch (StringIndexOutOfBoundsException excep) {
                            excep.printStackTrace();
                            System.out.println("Ran into problem when i == " + i + " an str = " + str);
                        }
                    }

                    //
                    // Move existing files into new dirs
                    //
                    for (int i = 0; i < localFiles.length; i++) {
                        Block srcB = new Block(localFiles[i]);
                        File dst = getBlockFilename(srcB, blkid, depth);
                        if (!src.renameTo(dst)) {
                            System.out.println("Unexpected problem in renaming " + src);
                        }
                    }
                }
                **/
            } else {
                // Find subdir
                children[getHalfByte(blkid, depth)].addBlock(b, src, blkid, depth+1);
            }
        }

        /**
         * Fill in the given blockSet with any child blocks
         * found at this node.
         */
        public void getBlockInfo(TreeSet blockSet) {
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

        /**
         * Find the file that corresponds to the given Block
         */
        public File getBlockFilename(Block b) {
            return getBlockFilename(b, b.getBlockId(), 0);
        }

        /**
         * Helper method to find file for a Block
         */         
        private File getBlockFilename(Block b, long blkid, int depth) {
            if (children == null) {
                return new File(dir, b.getBlockName());
            } else {
                // 
                // Lift the 4 bits starting at depth, going left->right.
                // That means there are 2^4 possible children, or 16.
                // The max depth is thus ((len(long) / 4) == 16).
                //
                return children[getHalfByte(blkid, depth)].getBlockFilename(b, blkid, depth+1);
            }
        }

        /**
         * Returns a number 0-15, inclusive.  Pulls out the right
         * half-byte from the indicated long.
         */
        private int getHalfByte(long blkid, int halfByteIndex) {
            blkid = blkid >> ((15 - halfByteIndex) * 4);
            return (int) ((0x000000000000000F) & blkid);
        }

        public String toString() {
          return "FSDir{" +
              "dir=" + dir +
              ", children=" + (children == null ? null : Arrays.asList(children)) +
              "}";
        }
    }

    //////////////////////////////////////////////////////
    //
    // FSDataSet
    //
    //////////////////////////////////////////////////////

    DF diskUsage;
    File data = null, tmp = null;
    long reserved = 0;
    FSDir dirTree;
    TreeSet ongoingCreates = new TreeSet();

    /**
     * An FSDataset has a directory where it loads its data files.
     */
    public FSDataset(File dir, Configuration conf) throws IOException {
        diskUsage = new DF( dir.getCanonicalPath(), conf); 
        this.data = new File(dir, "data");
        if (! data.exists()) {
            data.mkdirs();
        }
        this.tmp = new File(dir, "tmp");
        if (tmp.exists()) {
            FileUtil.fullyDelete(tmp, conf);
        }
        this.tmp.mkdirs();
        this.dirTree = new FSDir(data);
    }

    /**
     * Return total capacity, used and unused
     */
    public long getCapacity() throws IOException {
        return diskUsage.getCapacity();
    }

    /**
     * Return how many bytes can still be stored in the FSDataset
     */
    public long getRemaining() throws IOException {
        return ((long) Math.round(USABLE_DISK_PCT * diskUsage.getAvailable())) - reserved;
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
    public InputStream getBlockData(Block b) throws IOException {
        if (! isValidBlock(b)) {
            throw new IOException("Block " + b + " is not valid.");
        }
        return new FileInputStream(getFile(b));
    }

    /**
     * A Block b will be coming soon!
     */
    public boolean startBlock(Block b) throws IOException {
        //
        // Make sure the block isn't 'valid'
        //
        if (isValidBlock(b)) {
            throw new IOException("Block " + b + " is valid, and cannot be created.");
        }
        return true;
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

        //
        // Serialize access to /tmp, and check if file already there.
        //
        File f = null;
        synchronized (ongoingCreates) {
            //
            // Is it already in the create process?
            //
            if (ongoingCreates.contains(b)) {
                throw new IOException("Block " + b + " has already been started (though not completed), and thus cannot be created.");
            }

            //
            // Check if we have too little space
            //
            if (getRemaining() < BLOCK_SIZE) {
                throw new IOException("Insufficient space for an additional block");
            }

            //
            // OK, all's well.  Register the create, adjust 
            // 'reserved' size, & create file
            //
            ongoingCreates.add(b);
            reserved += BLOCK_SIZE;
            f = getTmpFile(b);
	    try {
		if (f.exists()) {
		    throw new IOException("Unexpected problem in startBlock() for " + b + ".  File " + f + " should not be present, but is.");
		}

		//
		// Create the zero-length temp file
		//
		if (!f.createNewFile()) {
		    throw new IOException("Unexpected problem in startBlock() for " + b + ".  File " + f + " should be creatable, but is already present.");
		}
	    } catch (IOException ie) {
                System.out.println("Exception!  " + ie);
		ongoingCreates.remove(b);		
		reserved -= BLOCK_SIZE;
                throw ie;
	    }
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
    public void finalizeBlock(Block b) throws IOException {
        File f = getTmpFile(b);
        if (! f.exists()) {
            throw new IOException("No temporary file " + f + " for block " + b);
        }
        
        synchronized (ongoingCreates) {
            //
            // Make sure still registered as ongoing
            //
            if (! ongoingCreates.contains(b)) {
                throw new IOException("Tried to finalize block " + b + ", but not in ongoingCreates table");
            }

            long finalLen = f.length();
            b.setNumBytes(finalLen);

            //
            // Move the file
            // (REMIND - mjc - shame to move the file within a synch
            // section!  Maybe remove this?)
            //
            dirTree.addBlock(b, f);

            //
            // Done, so deregister from ongoingCreates
            //
            if (! ongoingCreates.remove(b)) {
                throw new IOException("Tried to finalize block " + b + ", but could not find it in ongoingCreates after file-move!");
            } 
            reserved -= BLOCK_SIZE;
        }
    }

    /**
     * Return a table of block data
     */
    public Block[] getBlockReport() {
        TreeSet blockSet = new TreeSet();
        dirTree.getBlockInfo(blockSet);
        Block blockTable[] = new Block[blockSet.size()];
        int i = 0;
        for (Iterator it = blockSet.iterator(); it.hasNext(); i++) {
            blockTable[i] = (Block) it.next();
        }
        return blockTable;
    }

    /**
     * Check whether the given block is a valid one.
     */
    public boolean isValidBlock(Block b) {
        File f = getFile(b);
        if (f.exists()) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * We're informed that a block is no longer valid.  We
     * could lazily garbage-collect the block, but why bother?
     * just get rid of it.
     */
    public void invalidate(Block invalidBlks[]) throws IOException {
        for (int i = 0; i < invalidBlks.length; i++) {
            File f = getFile(invalidBlks[i]);

            // long len = f.length();
            if (!f.delete()) {
                throw new IOException("Unexpected error trying to delete block " + invalidBlks[i] + " at file " + f);
            }
        }
    }

    /**
     * Turn the block identifier into a filename.
     */
    File getFile(Block b) {
        // REMIND - mjc - should cache this result for performance
        return dirTree.getBlockFilename(b);
    }

    /**
     * Get the temp file, if this block is still being created.
     */
    File getTmpFile(Block b) {
        // REMIND - mjc - should cache this result for performance
        return new File(tmp, b.getBlockName());
    }

    public String toString() {
      return "FSDataset{" +
        "dirpath='" + diskUsage.getDirPath() + "'" +
        "}";
    }

}
