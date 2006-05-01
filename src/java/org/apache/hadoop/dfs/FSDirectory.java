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

import org.apache.hadoop.io.*;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

/*************************************************
 * FSDirectory stores the filesystem directory state.
 * It handles writing/loading values to disk, and logging
 * changes as we go.
 *
 * It keeps the filename->blockset mapping always-current
 * and logged to disk.
 * 
 * @author Mike Cafarella
 *************************************************/
class FSDirectory implements FSConstants {
    // Version is reflected in the dfs image and edit log files.
    // Versions are negative. 
    // Decrement DFS_CURRENT_VERSION to define a new version.
    private static final int DFS_CURRENT_VERSION = -1;
    private static final String FS_IMAGE = "fsimage";
    private static final String NEW_FS_IMAGE = "fsimage.new";
    private static final String OLD_FS_IMAGE = "fsimage.old";

    private static final byte OP_ADD = 0;
    private static final byte OP_RENAME = 1;
    private static final byte OP_DELETE = 2;
    private static final byte OP_MKDIR = 3;
    private static final byte OP_SET_REPLICATION = 4;

    /******************************************************
     * We keep an in-memory representation of the file/block
     * hierarchy.
     ******************************************************/
    class INode {
        private String name;
        private INode parent;
        private TreeMap children = new TreeMap();
        private Block blocks[];
        private short blockReplication;

        /**
         */
        INode(String name, Block blocks[], short replication) {
            this.name = name;
            this.parent = null;
            this.blocks = blocks;
            this.blockReplication = replication;
        }

        /**
         */
        INode(String name) {
            this.name = name;
            this.parent = null;
            this.blocks = null;
            this.blockReplication = 0;
        }

        /**
         * Check whether it's a directory
         */
        synchronized public boolean isDir() {
          return (blocks == null);
        }
        
        /**
         * Get block replication for the file 
         * @return block replication
         */
        public short getReplication() {
          return this.blockReplication;
        }

        /**
         * This is the external interface
         */
        INode getNode(String target) {
            if ( target == null || 
                ! target.startsWith("/") || target.length() == 0) {
                return null;
            } else if (parent == null && "/".equals(target)) {
                return this;
            } else {
                Vector components = new Vector();
                int start = 0;
                int slashid = 0;
                while (start < target.length() && (slashid = target.indexOf('/', start)) >= 0) {
                    components.add(target.substring(start, slashid));
                    start = slashid + 1;
                }
                if (start < target.length()) {
                    components.add(target.substring(start));
                }
                return getNode(components, 0);
            }
        }

        /**
         */
        INode getNode(Vector components, int index) {
            if (! name.equals((String) components.elementAt(index))) {
                return null;
            }
            if (index == components.size()-1) {
                return this;
            }

            // Check with children
            INode child = this.getChild((String)components.elementAt(index+1));
            if (child == null) {
                return null;
            } else {
                return child.getNode(components, index+1);
            }
        }
        
        INode getChild( String name) {
          return (INode) children.get( name );
        }

        /**
         * Add new INode to the file tree.
         * Find the parent and insert 
         * 
         * @param path file path
         * @param newNode INode to be added
         * @return null if the node already exists; inserted INode, otherwise
         * @author shv
         */
        INode addNode(String path, INode newNode) {
          File target = new File( path );
          // find parent
          Path parent = new Path(path).getParent();
          if (parent == null)
            return null;
          INode parentNode = getNode(parent.toString());
          if (parentNode == null)
            return null;
          // check whether the parent already has a node with that name
          String name = newNode.name = target.getName();
          if( parentNode.getChild( name ) != null )
            return null;
          // insert into the parent children list
          parentNode.children.put(name, newNode);
          newNode.parent = parentNode;
          return newNode;
        }

        /**
         */
        boolean removeNode() {
            if (parent == null) {
                return false;
            } else {
                parent.children.remove(name);
                return true;
            }
        }

        /**
         * Collect all the blocks at this INode and all its children.
         * This operation is performed after a node is removed from the tree,
         * and we want to GC all the blocks at this node and below.
         */
        void collectSubtreeBlocks(Vector v) {
            if (blocks != null) {
                for (int i = 0; i < blocks.length; i++) {
                    v.add(blocks[i]);
                }
            }
            for (Iterator it = children.values().iterator(); it.hasNext(); ) {
                INode child = (INode) it.next();
                child.collectSubtreeBlocks(v);
            }
        }

        /**
         */
        int numItemsInTree() {
            int total = 0;
            for (Iterator it = children.values().iterator(); it.hasNext(); ) {
                INode child = (INode) it.next();
                total += child.numItemsInTree();
            }
            return total + 1;
        }

        /**
         */
        String computeName() {
            if (parent != null) {
                return parent.computeName() + "/" + name;
            } else {
                return name;
            }
        }

        /**
         */
        long computeFileLength() {
            long total = 0;
            if (blocks != null) {
                for (int i = 0; i < blocks.length; i++) {
                    total += blocks[i].getNumBytes();
                }
            }
            return total;
        }

        /**
         */
        long computeContentsLength() {
            long total = computeFileLength();
            for (Iterator it = children.values().iterator(); it.hasNext(); ) {
                INode child = (INode) it.next();
                total += child.computeContentsLength();
            }
            return total;
        }

        /**
         */
        void listContents(Vector v) {
            if (parent != null && blocks != null) {
                v.add(this);
            }

            for (Iterator it = children.values().iterator(); it.hasNext(); ) {
                INode child = (INode) it.next();
                v.add(child);
            }
        }

        /**
         */
        void saveImage(String parentPrefix, DataOutputStream out) throws IOException {
            String fullName = "";
            if (parent != null) {
                fullName = parentPrefix + "/" + name;
                new UTF8(fullName).write(out);
                out.writeShort(blockReplication);
                if (blocks == null) {
                    out.writeInt(0);
                } else {
                    out.writeInt(blocks.length);
                    for (int i = 0; i < blocks.length; i++) {
                        blocks[i].write(out);
                    }
                }
            }
            for (Iterator it = children.values().iterator(); it.hasNext(); ) {
                INode child = (INode) it.next();
                child.saveImage(fullName, out);
            }
        }
    }

    
    INode rootDir = new INode("");
    TreeMap activeBlocks = new TreeMap();
    TreeMap activeLocks = new TreeMap();
    DataOutputStream editlog = null;
    boolean ready = false;

    /** Access an existing dfs name directory. */
    public FSDirectory(File dir, Configuration conf) throws IOException {
        File fullimage = new File(dir, "image");
        if (! fullimage.exists()) {
          throw new IOException("NameNode not formatted: " + dir);
        }
        File edits = new File(dir, "edits");
        if (loadFSImage(fullimage, edits, conf)) {
            saveFSImage(fullimage, edits);
        }

        synchronized (this) {
            this.ready = true;
            this.notifyAll();
            this.editlog = new DataOutputStream(new FileOutputStream(edits));
            editlog.writeInt( DFS_CURRENT_VERSION );
        }
    }

    /** Create a new dfs name directory.  Caution: this destroys all files
     * in this filesystem. */
    public static void format(File dir, Configuration conf)
      throws IOException {
        File image = new File(dir, "image");
        File edits = new File(dir, "edits");

        if (!((!image.exists() || FileUtil.fullyDelete(image)) &&
              (!edits.exists() || edits.delete()) &&
              image.mkdirs())) {
          
          throw new IOException("Unable to format: "+dir);
        }
    }

    /**
     * Shutdown the filestore
     */
    public void close() throws IOException {
        editlog.close();
    }

    /**
     * Block until the object is ready to be used.
     */
    void waitForReady() {
        if (! ready) {
            synchronized (this) {
                while (!ready) {
                    try {
                        this.wait(5000);
                    } catch (InterruptedException ie) {
                    }
                }
            }
        }
    }

    /**
     * Load in the filesystem image.  It's a big list of
     * filenames and blocks.  Return whether we should
     * "re-save" and consolidate the edit-logs
     */
    boolean loadFSImage( File fsdir, 
                         File edits, 
                         Configuration conf
                       ) throws IOException {
        //
        // Atomic move sequence, to recover from interrupted save
        //
        File curFile = new File(fsdir, FS_IMAGE);
        File newFile = new File(fsdir, NEW_FS_IMAGE);
        File oldFile = new File(fsdir, OLD_FS_IMAGE);

        // Maybe we were interrupted between 2 and 4
        if (oldFile.exists() && curFile.exists()) {
            oldFile.delete();
            if (edits.exists()) {
                edits.delete();
            }
        } else if (oldFile.exists() && newFile.exists()) {
            // Or maybe between 1 and 2
            newFile.renameTo(curFile);
            oldFile.delete();
        } else if (curFile.exists() && newFile.exists()) {
            // Or else before stage 1, in which case we lose the edits
            newFile.delete();
        }

        //
        // Load in bits
        //
        if (curFile.exists()) {
            DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(curFile)));
            try {
                // read image version
                int imgVersion = in.readInt();
                // read number of files
                int numFiles = 0;
                // version 0 does not store version #
                // starts directly with the number of files
                if( imgVersion >= 0 ) {  
                  numFiles = imgVersion;
                  imgVersion = 0;
                } else 
                  numFiles = in.readInt();
                  
                if( imgVersion < DFS_CURRENT_VERSION ) // future version
                  throw new IOException(
                              "Unsupported version of the file system image: "
                              + imgVersion
                              + ". Current version = " 
                              + DFS_CURRENT_VERSION + "." );
                
                // read file info
                short replication = (short)conf.getInt("dfs.replication", 3);
                for (int i = 0; i < numFiles; i++) {
                    UTF8 name = new UTF8();
                    name.readFields(in);
                    // version 0 does not support per file replication
                    if( !(imgVersion >= 0) ) {
                      replication = in.readShort(); // other versions do
                      replication = adjustReplication( replication, conf );
                    }
                    int numBlocks = in.readInt();
                    Block blocks[] = null;
                    if (numBlocks > 0) {
                        blocks = new Block[numBlocks];
                        for (int j = 0; j < numBlocks; j++) {
                            blocks[j] = new Block();
                            blocks[j].readFields(in);
                        }
                    }
                    unprotectedAddFile(name, 
                            new INode( name.toString(), blocks, replication ));
                }
            } finally {
                in.close();
            }
        }

        if (edits.exists() && loadFSEdits(edits, conf) > 0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Load an edit log, and apply the changes to the in-memory structure
     *
     * This is where we apply edits that we've been writing to disk all
     * along.
     */
    int loadFSEdits(File edits, Configuration conf) throws IOException {
        int numEdits = 0;
        int logVersion = 0;

        if (edits.exists()) {
            DataInputStream in = new DataInputStream(
                                    new BufferedInputStream(
                                        new FileInputStream(edits)));
            // Read log file version. Could be missing. 
            in.mark( 4 );
            if( in.available() > 0 ) {
              logVersion = in.readByte();
              in.reset();
              if( logVersion >= 0 )
                logVersion = 0;
              else
                logVersion = in.readInt();
              if( logVersion < DFS_CURRENT_VERSION ) // future version
                  throw new IOException(
                            "Unexpected version of the file system log file: "
                            + logVersion
                            + ". Current version = " 
                            + DFS_CURRENT_VERSION + "." );
            }
            
            short replication = (short)conf.getInt("dfs.replication", 3);
            try {
                while (in.available() > 0) {
                    byte opcode = in.readByte();
                    numEdits++;
                    switch (opcode) {
                    case OP_ADD: {
                        UTF8 name = new UTF8();
                        ArrayWritable aw = null;
                        Writable writables[];
                        // version 0 does not support per file replication
                        if( logVersion >= 0 )
                          name.readFields(in);  // read name only
                        else {  // other versions do
                          // get name and replication
                          aw = new ArrayWritable(UTF8.class);
                          aw.readFields(in);
                          writables = aw.get(); 
                          if( writables.length != 2 )
                            throw new IOException("Incorrect data fortmat. " 
                                           + "Name & replication pair expected");
                          name = (UTF8) writables[0];
                          replication = Short.parseShort(
                                              ((UTF8)writables[1]).toString());
                          replication = adjustReplication( replication, conf );
                        }
                        // get blocks
                        aw = new ArrayWritable(Block.class);
                        aw.readFields(in);
                        writables = aw.get();
                        Block blocks[] = new Block[writables.length];
                        System.arraycopy(writables, 0, blocks, 0, blocks.length);
                        // add to the file tree
                        unprotectedAddFile(name, 
                            new INode( name.toString(), blocks, replication ));
                        break;
                    }
                    case OP_SET_REPLICATION: {
                        UTF8 src = new UTF8();
                        UTF8 repl = new UTF8();
                        src.readFields(in);
                        repl.readFields(in);
                        replication=adjustReplication(
                                fromLogReplication(repl),
                                conf);
                        unprotectedSetReplication(src.toString(), 
                                                  replication,
                                                  null);
                        break;
                    } 
                    case OP_RENAME: {
                        UTF8 src = new UTF8();
                        UTF8 dst = new UTF8();
                        src.readFields(in);
                        dst.readFields(in);
                        unprotectedRenameTo(src, dst);
                        break;
                    }
                    case OP_DELETE: {
                        UTF8 src = new UTF8();
                        src.readFields(in);
                        unprotectedDelete(src);
                        break;
                    }
                    case OP_MKDIR: {
                        UTF8 src = new UTF8();
                        src.readFields(in);
                        unprotectedMkdir(src.toString());
                        break;
                    }
                    default: {
                        throw new IOException("Never seen opcode " + opcode);
                    }
                    }
                }
            } finally {
                in.close();
            }
        }
        
        if( logVersion != DFS_CURRENT_VERSION ) // other version
          numEdits++; // save this image asap
        return numEdits;
    }

    private static short adjustReplication( short replication, Configuration conf) {
        short minReplication = (short)conf.getInt("dfs.replication.min", 1);
        if( replication<minReplication ) {
            replication = minReplication;
        }
        short maxReplication = (short)conf.getInt("dfs.replication.max", 512);
        if( replication>maxReplication ) {
            replication = maxReplication;
        }
        return replication;
    }
    /**
     * Save the contents of the FS image
     */
    void saveFSImage(File fullimage, File edits) throws IOException {
        File curFile = new File(fullimage, FS_IMAGE);
        File newFile = new File(fullimage, NEW_FS_IMAGE);
        File oldFile = new File(fullimage, OLD_FS_IMAGE);

        //
        // Write out data
        //
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(newFile)));
        try {
            out.writeInt(DFS_CURRENT_VERSION);
            out.writeInt(rootDir.numItemsInTree() - 1);
            rootDir.saveImage("", out);
        } finally {
            out.close();
        }

        //
        // Atomic move sequence
        //
        // 1.  Move cur to old
        curFile.renameTo(oldFile);
        
        // 2.  Move new to cur
        newFile.renameTo(curFile);

        // 3.  Remove pending-edits file (it's been integrated with newFile)
        edits.delete();
        
        // 4.  Delete old
        oldFile.delete();
    }

    /**
     * Write an operation to the edit log
     */
    void logEdit(byte op, Writable w1, Writable w2) {
        synchronized (editlog) {
            try {
                editlog.write(op);
                if (w1 != null) {
                    w1.write(editlog);
                }
                if (w2 != null) {
                    w2.write(editlog);
                }
            } catch (IOException ie) {
            }
        }
    }

    /**
     * Add the given filename to the fs.
     */
    public boolean addFile(UTF8 path, Block[] blocks, short replication) {
        waitForReady();

        // Always do an implicit mkdirs for parent directory tree
        String pathString = path.toString();
        mkdirs(new Path(pathString).getParent().toString());
        INode newNode = new INode( new File(pathString).getName(), blocks, replication);
        if( ! unprotectedAddFile(path, newNode) )
          return false;
        // add create file record to log
        UTF8 nameReplicationPair[] = new UTF8[] { 
                              path, 
                              toLogReplication( replication )};
        logEdit(OP_ADD,
                new ArrayWritable( UTF8.class, nameReplicationPair ), 
                new ArrayWritable( Block.class, newNode.blocks ));
        return true;
    }
    
    private static UTF8 toLogReplication( short replication ) {
      return new UTF8( Short.toString(replication));
    }
    
    private static short fromLogReplication( UTF8 replication ) {
      return Short.parseShort(replication.toString());
    }    
    
    /**
     */
    boolean unprotectedAddFile(UTF8 path, INode newNode) {
      synchronized (rootDir) {
        int nrBlocks = (newNode.blocks == null) ? 0 : newNode.blocks.length;
        // Add file->block mapping
        for (int i = 0; i < nrBlocks; i++)
            activeBlocks.put(newNode.blocks[i], newNode);
        return (rootDir.addNode(path.toString(), newNode) != null);
      }
    }

    /**
     * Change the filename
     */
    public boolean renameTo(UTF8 src, UTF8 dst) {
        waitForReady();
        if (unprotectedRenameTo(src, dst)) {
            logEdit(OP_RENAME, src, dst);
            return true;
        } else {
            return false;
        }
    }

    /**
     */
    boolean unprotectedRenameTo(UTF8 src, UTF8 dst) {
        synchronized(rootDir) {
          String srcStr = src.toString();
          String dstStr = dst.toString();
            INode renamedNode = rootDir.getNode(srcStr);
            if (renamedNode == null) {
                return false;
            }
            renamedNode.removeNode();
            if (isDir(dst)) {
              dstStr += "/" + new File(srcStr).getName();
            }
            // the renamed node can be reused now
            if( rootDir.addNode(dstStr, renamedNode ) == null ) {
              rootDir.addNode(srcStr, renamedNode); // put it back
              return false;
            }
            return true;
        }
    }

    /**
     * Set file replication
     * 
     * @param src file name
     * @param replication new replication
     * @param oldReplication old replication - output parameter
     * @return array of file blocks
     * @throws IOException
     */
    public Block[] setReplication(String src, 
                              short replication,
                              Vector oldReplication
                             ) throws IOException {
      waitForReady();
      Block[] fileBlocks = unprotectedSetReplication(src, replication, oldReplication );
      if( fileBlocks != null )  // 
        logEdit(OP_SET_REPLICATION, 
                new UTF8(src), 
                toLogReplication( replication ));
      return fileBlocks;
    }

    private Block[] unprotectedSetReplication( String src, 
                                          short replication,
                                          Vector oldReplication
                                        ) throws IOException {
      if( oldReplication == null )
        oldReplication = new Vector();
      oldReplication.setSize(1);
      oldReplication.set( 0, new Integer(-1) );
      Block[] fileBlocks = null;
      synchronized(rootDir) {
        INode fileNode = rootDir.getNode(src);
        if (fileNode == null)
          return null;
        if( fileNode.isDir() )
          return null;
        oldReplication.set( 0, new Integer( fileNode.blockReplication ));
        fileNode.blockReplication = replication;
        fileBlocks = fileNode.blocks;
      }
      return fileBlocks;
    }
                                 
    /**
     * Remove the file from management, return blocks
     */
    public Block[] delete(UTF8 src) {
        waitForReady();
        Block[] blocks = unprotectedDelete(src); 
        if( blocks != null )
          logEdit(OP_DELETE, src, null);
        return blocks;
    }

    /**
     */
    Block[] unprotectedDelete(UTF8 src) {
        synchronized (rootDir) {
            INode targetNode = rootDir.getNode(src.toString());
            if (targetNode == null) {
                return null;
            } else {
                //
                // Remove the node from the namespace and GC all
                // the blocks underneath the node.
                //
                if (! targetNode.removeNode()) {
                    return null;
                } else {
                    Vector v = new Vector();
                    targetNode.collectSubtreeBlocks(v);
                    for (Iterator it = v.iterator(); it.hasNext(); ) {
                        Block b = (Block) it.next();
                        activeBlocks.remove(b);
                    }
                    return (Block[]) v.toArray(new Block[v.size()]);
                }
            }
        }
    }

    /**
     */
    public int obtainLock(UTF8 src, UTF8 holder, boolean exclusive) {
        TreeSet holders = (TreeSet) activeLocks.get(src);
        if (holders == null) {
            holders = new TreeSet();
            activeLocks.put(src, holders);
        }
        if (exclusive && holders.size() > 0) {
            return STILL_WAITING;
        } else {
            holders.add(holder);
            return COMPLETE_SUCCESS;
        }
    }

    /**
     */
    public int releaseLock(UTF8 src, UTF8 holder) {
        TreeSet holders = (TreeSet) activeLocks.get(src);
        if (holders != null && holders.contains(holder)) {
            holders.remove(holder);
            if (holders.size() == 0) {
                activeLocks.remove(src);
            }
            return COMPLETE_SUCCESS;
        } else {
            return OPERATION_FAILED;
        }
    }

    /**
     * Get a listing of files given path 'src'
     *
     * This function is admittedly very inefficient right now.  We'll
     * make it better later.
     */
    public DFSFileInfo[] getListing(UTF8 src) {
        String srcs = normalizePath(src);

        synchronized (rootDir) {
            INode targetNode = rootDir.getNode(srcs);
            if (targetNode == null) {
                return null;
            } else {
                Vector contents = new Vector();
                targetNode.listContents(contents);

                DFSFileInfo listing[] = new DFSFileInfo[contents.size()];
                int i = 0;
                for (Iterator it = contents.iterator(); it.hasNext(); i++) {
                    listing[i] = new DFSFileInfo( (INode) it.next() );
                }
                return listing;
            }
        }
    }

    /**
     * Get the blocks associated with the file
     */
    public Block[] getFile(UTF8 src) {
        waitForReady();
        synchronized (rootDir) {
            INode targetNode = rootDir.getNode(src.toString());
            if (targetNode == null) {
                return null;
            } else {
                return targetNode.blocks;
            }
        }
    }

    /** 
     * Check whether the filepath could be created
     */
    public boolean isValidToCreate(UTF8 src) {
        String srcs = normalizePath(src);
        synchronized (rootDir) {
            if (srcs.startsWith("/") && 
                ! srcs.endsWith("/") && 
                rootDir.getNode(srcs) == null) {
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * Check whether the path specifies a directory
     */
    public boolean isDir(UTF8 src) {
        synchronized (rootDir) {
            INode node = rootDir.getNode(normalizePath(src));
            return node != null && node.isDir();
        }
    }

    /**
     * Create the given directory and all its parent dirs.
     */
    public boolean mkdirs(UTF8 src) {
        return mkdirs(src.toString());
    }

    /**
     * Create directory entries for every item
     */
    boolean mkdirs(String src) {
        src = normalizePath(new UTF8(src));

        // Use this to collect all the dirs we need to construct
        Vector v = new Vector();

        // The dir itself
        v.add(src);

        // All its parents
        Path parent = new Path(src).getParent();
        while (parent != null) {
            v.add(parent.toString());
            parent = parent.getParent();
        }

        // Now go backwards through list of dirs, creating along
        // the way
        boolean lastSuccess = false;
        int numElts = v.size();
        for (int i = numElts - 1; i >= 0; i--) {
            String cur = (String) v.elementAt(i);
            INode inserted = unprotectedMkdir(cur);
            if (inserted != null) {
                logEdit(OP_MKDIR, new UTF8(inserted.computeName()), null);
                lastSuccess = true;
            } else {
                lastSuccess = false;
            }
        }
        return lastSuccess;
    }

    /**
     */
    INode unprotectedMkdir(String src) {
        synchronized (rootDir) {
            return rootDir.addNode(src, new INode(new File(src).getName()));
        }
    }

    /**
     */
    String normalizePath(UTF8 src) {
        String srcs = src.toString();
        if (srcs.length() > 1 && srcs.endsWith("/")) {
            srcs = srcs.substring(0, srcs.length() - 1);
        }
        return srcs;
    }

    /**
     * Returns whether the given block is one pointed-to by a file.
     */
    public boolean isValidBlock(Block b) {
        synchronized (rootDir) {
            if (activeBlocks.containsKey(b)) {
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * Returns whether the given block is one pointed-to by a file.
     */
    public INode getFileByBlock(Block b) {
      synchronized (rootDir) {
        return (INode)activeBlocks.get(b);
      }
    }
}
