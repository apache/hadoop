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

import org.apache.hadoop.io.*;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.Metrics;

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

    /******************************************************
     * We keep an in-memory representation of the file/block
     * hierarchy.
     * 
     * TODO: Factor out INode to a standalone class.
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
         * Get local file name
         * @return local file name
         */
        String getLocalName() {
          return name;
        }

        /**
         * Get file blocks 
         * @return file blocks
         */
        Block[] getBlocks() {
          return this.blocks;
        }
        
        /**
         * Get parent directory 
         * @return parent INode
         */
        INode getParent() {
          return this.parent;
        }

        /**
         * Get children 
         * @return TreeMap of children
         */
        TreeMap getChildren() {
          return this.children;
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
         * @throws FileNotFoundException 
         * @author shv
         */
        INode addNode(String path, INode newNode) throws FileNotFoundException {
          File target = new File( path );
          // find parent
          Path parent = new Path(path).getParent();
          if (parent == null) { // add root
              return null;
          }
          INode parentNode = getNode(parent.toString());
          if (parentNode == null) {
              throw new FileNotFoundException(
                      "Parent path does not exist: "+path);
          }
          if (!parentNode.isDir()) {
        	  throw new FileNotFoundException(
        			  "Parent path is not a directory: "+path);
          }
           // check whether the parent already has a node with that name
          String name = newNode.name = target.getName();
          if( parentNode.getChild( name ) != null ) {
            return null;
          }
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
            Metrics.report(metricsRecord, "files-deleted", ++numFilesDeleted);
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
         * Get the block size of the first block
         * @return the number of bytes
         */
        public long getBlockSize() {
          if (blocks == null || blocks.length == 0) {
            return 0;
          } else {
            return blocks[0].getNumBytes();
          }
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
    }

    
    INode rootDir = new INode("");
    Map activeBlocks = new HashMap();
    TreeMap activeLocks = new TreeMap();
    FSImage fsImage;  
    boolean ready = false;
    int namespaceID = 0;    // TODO: move to FSImage class, it belongs there
    // Metrics members
    private MetricsRecord metricsRecord = null;
    private int numFilesDeleted = 0;
    
    /** Access an existing dfs name directory. */
    public FSDirectory(File[] dirs) throws IOException {
      this.fsImage = new FSImage( dirs );
    }
    
    void loadFSImage( Configuration conf ) throws IOException {
      fsImage.loadFSImage( conf );
      synchronized (this) {
        this.ready = true;
        this.notifyAll();
        fsImage.getEditLog().create();
      }
      metricsRecord = Metrics.createRecord("dfs", "namenode");
    }

    /**
     * Shutdown the filestore
     */
    public void close() throws IOException {
        fsImage.getEditLog().close();
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
     * Add the given filename to the fs.
     */
    public boolean addFile(UTF8 path, Block[] blocks, short replication) {
        waitForReady();

        // Always do an implicit mkdirs for parent directory tree
        String pathString = path.toString();
        if( ! mkdirs(new Path(pathString).getParent().toString()) ) {
           return false;
        }
        INode newNode = new INode( new File(pathString).getName(), blocks, replication);
        if( ! unprotectedAddFile(path, newNode) ) {
           NameNode.stateChangeLog.info("DIR* FSDirectory.addFile: "
                    +"failed to add "+path+" with "
                    +blocks.length+" blocks to the file system" );
           return false;
        }
        // add create file record to log
        fsImage.getEditLog().logCreateFile( newNode );
        NameNode.stateChangeLog.debug("DIR* FSDirectory.addFile: "
                +path+" with "+blocks.length+" blocks is added to the file system" );
        return true;
    }
    
    /**
     */
    boolean unprotectedAddFile(UTF8 path, INode newNode) {
      synchronized (rootDir) {
         try {
            if( rootDir.addNode(path.toString(), newNode ) != null ) {
                int nrBlocks = (newNode.blocks == null) ? 0 : newNode.blocks.length;
                // Add file->block mapping
                for (int i = 0; i < nrBlocks; i++)
                    activeBlocks.put(newNode.blocks[i], newNode);
                return true;
            } else {
                return false;
            }
        } catch (FileNotFoundException e ) {
            return false;
        }
      }
    }
    
    boolean unprotectedAddFile(UTF8 path, Block[] blocks, short replication ) {
      return unprotectedAddFile( path,  
                    new INode( path.toString(), blocks, replication ));
    }

    /**
     * Change the filename
     */
    public boolean renameTo(UTF8 src, UTF8 dst) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.renameTo: "
          +src+" to "+dst );
      waitForReady();
      if( ! unprotectedRenameTo(src, dst) )
        return false;
      fsImage.getEditLog().logRename(src, dst);
      return true;
    }

    /**
     */
    boolean unprotectedRenameTo(UTF8 src, UTF8 dst) {
        synchronized(rootDir) {
          String srcStr = src.toString();
          String dstStr = dst.toString();
            INode renamedNode = rootDir.getNode(srcStr);
            if (renamedNode == null) {
                NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                        +"failed to rename "+src+" to "+dst+ " because source does not exist" );
                return false;
            }
            if (isDir(dst)) {
              dstStr += "/" + new File(srcStr).getName();
            }
            if( rootDir.getNode(dstStr.toString()) != null ) {
                NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                        +"failed to rename "+src+" to "+dstStr+ " because destination exists" );
                return false;
            }
            renamedNode.removeNode();
            
            // the renamed node can be reused now
            try {
                if( rootDir.addNode(dstStr, renamedNode ) != null ) {
                    NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedRenameTo: "
                        +src+" is renamed to "+dst );
                    return true;
                }
            } catch (FileNotFoundException e ) {
                NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                        +"failed to rename "+src+" to "+dst );
                try {
                    rootDir.addNode(srcStr, renamedNode); // put it back
                }catch(FileNotFoundException e2) {                
                }
            }

            return false;
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
    Block[] setReplication( String src, 
                            short replication,
                            Vector oldReplication
                           ) throws IOException {
      waitForReady();
      Block[] fileBlocks = unprotectedSetReplication(src, replication, oldReplication );
      if( fileBlocks != null )  // log replication change
        fsImage.getEditLog().logSetReplication( src, replication );
      return fileBlocks;
    }

    Block[] unprotectedSetReplication(  String src, 
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
     * Get the blocksize of a file
     * @param filename the filename
     * @return the number of bytes in the first block
     * @throws IOException if it is a directory or does not exist.
     */
    public long getBlockSize(String filename) throws IOException {
      synchronized (rootDir) {
        INode fileNode = rootDir.getNode(filename);
        if (fileNode == null) {
          throw new IOException("Unknown file: " + filename);
        }
        if (fileNode.isDir()) {
          throw new IOException("Getting block size of a directory: " + 
                                filename);
        }
        return fileNode.getBlockSize();
      }
    }
    
    /**
     * Remove the file from management, return blocks
     */
    public Block[] delete(UTF8 src) {
        NameNode.stateChangeLog.debug("DIR* FSDirectory.delete: "
                +src );
        waitForReady();
        Block[] blocks = unprotectedDelete(src); 
        if( blocks != null )
          fsImage.getEditLog().logDelete( src );
        return blocks;
    }

    /**
     */
    Block[] unprotectedDelete(UTF8 src) {
        synchronized (rootDir) {
            INode targetNode = rootDir.getNode(src.toString());
            if (targetNode == null) {
                NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedDelete: "
                        +"failed to remove "+src+" because it does not exist" );
                return null;
            } else {
                //
                // Remove the node from the namespace and GC all
                // the blocks underneath the node.
                //
                if (! targetNode.removeNode()) {
                    NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedDelete: "
                            +"failed to remove "+src+" because it does not have a parent" );
                    return null;
                } else {
                    NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
                            +src+" is removed" );
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
        int numElts = v.size();
        for (int i = numElts - 1; i >= 0; i--) {
            String cur = (String) v.elementAt(i);
            try {
               INode inserted = unprotectedMkdir(cur);
               if (inserted != null) {
                   NameNode.stateChangeLog.debug("DIR* FSDirectory.mkdirs: "
                        +"created directory "+cur );
                   fsImage.getEditLog().logMkDir( inserted );
               } else { // otherwise cur exists, verify that it is a directory
                 if (!isDir(new UTF8(cur))) {
                   NameNode.stateChangeLog.debug("DIR* FSDirectory.mkdirs: "
                        +"path " + cur + " is not a directory ");
                   return false;
                 } 
               }
            } catch (FileNotFoundException e ) {
                NameNode.stateChangeLog.debug("DIR* FSDirectory.mkdirs: "
                        +"failed to create directory "+src);
                return false;
            }
        }
        return true;
    }

    /**
     */
    INode unprotectedMkdir(String src) throws FileNotFoundException {
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
