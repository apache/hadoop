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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.FSDirectory.INode;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.UTF8;

/**
 * FSImage handles checkpointing and logging of the namespace edits.
 * 
 * @author Konstantin Shvachko
 */
class FSImage {

  //
  // The filenames used for storing the images
  //
  private enum NameNodeFile {
    IMAGE ("fsimage"),
    CKPT ("fsimage.ckpt"),
    TIME ("fstime");

    private String fileName;
    private NameNodeFile(String name) {
      this.fileName = name;
    }
    String getName() {
      return fileName;
    }
  }

  private File[] imageDirs;  /// directories that contains the image file 
  private FSEditLog editLog;

  /**
   * 
   */
  FSImage( File[] fsDirs ) throws IOException {
    this.imageDirs = new File[fsDirs.length];
    for (int idx = 0; idx < imageDirs.length; idx++) {
      imageDirs[idx] = new File(fsDirs[idx], "image");
      if (! imageDirs[idx].exists()) {
        throw new IOException("NameNode not formatted: " + imageDirs[idx]);
      }
    }
    this.editLog = new FSEditLog( fsDirs , this);
  }

  /**
   * Represents an Image (image and edit file).
   */
  FSImage(File imageDir, String edits) throws IOException {
    this.imageDirs = new File[1];
    imageDirs[0] = imageDir;
    if (!imageDirs[0].exists()) {
      throw new IOException("File " + imageDirs[0] + " not found.");
    }
    this.editLog = new FSEditLog(imageDir, this, edits);
  }

  /*
   * Create an fsimage and edits log from scratch.
   */
  void create() throws IOException {
    saveFSImage(NameNodeFile.IMAGE.getName());
    editLog.create();
  }

  /**
   * If there is an IO Error on any log operations, remove that
   * directory from the list of directories. If no more directories
   * remain, then raise an exception that will possibly cause the
   * server to exit
   */
  void processIOError(int index) throws IOException {
    if (imageDirs.length == 1) {
      throw new IOException("Checkpoint directories inaccessible.");
    }
    assert(index < imageDirs.length);
    int newsize = imageDirs.length - 1;
    int oldsize = imageDirs.length;

    //
    // save existing values and allocate space for new ones
    //
    File[] imageDirs1 = imageDirs;
    imageDirs = new File[newsize];

    //
    // copy in saved values, skipping the one on which we had
    // an error
    //
    for (int idx = 0; idx < index; idx++) {
      imageDirs[idx] = imageDirs1[idx];
    }
    for (int idx = index; idx < oldsize - 1; idx++) {
      imageDirs[idx] = imageDirs1[idx+1];
    }
  }
        
  FSEditLog getEditLog() {
    return editLog;
  }

  /**
   * Load in the filesystem image.  It's a big list of
   * filenames and blocks.  Return whether we should
   * "re-save" and consolidate the edit-logs
   */
  void loadFSImage( Configuration conf ) throws IOException {
    for (int idx = 0; idx < imageDirs.length; idx++) {
      //
      // Atomic move sequence, to recover from interrupted save
      //
      File curFile = new File(imageDirs[idx], 
                              NameNodeFile.IMAGE.getName());
      File ckptFile = new File(imageDirs[idx], 
                              NameNodeFile.CKPT.getName());

      //
      // If we were in the midst of a checkpoint
      //
      if (ckptFile.exists()) {
        if (editLog.existsNew(idx)) {
          //
          // checkpointing migth have uploaded a new
          // merged image, but we discard it here because we are
          // not sure whether the entire merged image was uploaded
          // before the namenode crashed.
          //
          if (!ckptFile.delete()) {
            throw new IOException("Unable to delete " + ckptFile);
          }
        } else {
          //
          // checkpointing was in progress when the namenode
          // shutdown. The fsimage.ckpt was created and the edits.new
          // file was moved to edits. We complete that checkpoint by
          // moving fsimage.new to fsimage. There is no need to 
          // update the fstime file here. renameTo fails on Windows
          // if the destination file already exists.
          //
          if (!ckptFile.renameTo(curFile)) {
            curFile.delete();
            if (!ckptFile.renameTo(curFile)) {
              throw new IOException("Unable to rename " + ckptFile +
                                    " to " + curFile);
            }
          }
        }
      }
    }

    // Now check all curFiles and see which is the newest
    File curFile = null;
    long maxTimeStamp = Long.MIN_VALUE;
    int saveidx = 0;
    boolean needToSave = false;
    for (int idx = 0; idx < imageDirs.length; idx++) {
      File file = new File(imageDirs[idx], 
                           NameNodeFile.IMAGE.getName());
      if (file.exists()) {
        long timeStamp = 0;
        File timeFile = new File(imageDirs[idx], 
                                 NameNodeFile.TIME.getName());
        if (timeFile.exists() && timeFile.canRead()) {
          DataInputStream in = new DataInputStream(
              new FileInputStream(timeFile));
          try {
            timeStamp = in.readLong();
          } finally {
            in.close();
          }
        } else {
          needToSave |= true;
        }
        if (maxTimeStamp < timeStamp) {
          maxTimeStamp = timeStamp;
          curFile = file;
          saveidx = idx;
        }
      } else {
        needToSave |= true;
      }
    }

    //
    // Load in bits
    //
    needToSave |= loadFSImage(conf, curFile);

    //
    // read in the editlog from the same directory from
    // which we read in the image
    //
    needToSave |= (editLog.loadFSEdits(conf, saveidx) > 0);
    if (needToSave) {
      saveFSImage();
    }
  }

  boolean loadFSImage(Configuration conf, File curFile)
                      throws IOException {
    FSNamesystem fsNamesys = FSNamesystem.getFSNamesystem();
    FSDirectory fsDir = fsNamesys.dir;
    //
    // Load in bits
    //
    boolean needToSave = true;
    int imgVersion = FSConstants.DFS_CURRENT_VERSION;
    if (curFile != null) {
      DataInputStream in = new DataInputStream(
                              new BufferedInputStream(
                                  new FileInputStream(curFile)));
      try {
        // read image version: first appeared in version -1
        imgVersion = in.readInt();
        // read namespaceID: first appeared in version -2
        if( imgVersion <= -2 )
          fsDir.namespaceID = in.readInt();
        // read number of files
        int numFiles = 0;
        // version 0 does not store version #
        // starts directly with the number of files
        if( imgVersion >= 0 ) {
          numFiles = imgVersion;
          imgVersion = 0;
        } else {
          numFiles = in.readInt();
        }

        needToSave = ( imgVersion != FSConstants.DFS_CURRENT_VERSION );
        if( imgVersion < FSConstants.DFS_CURRENT_VERSION ) // future version
          throw new IncorrectVersionException(imgVersion, "file system image");

        // read file info
        short replication = (short)conf.getInt("dfs.replication", 3);
        for (int i = 0; i < numFiles; i++) {
          UTF8 name = new UTF8();
          name.readFields(in);
          // version 0 does not support per file replication
          if( !(imgVersion >= 0) ) {
            replication = in.readShort(); // other versions do
            replication = FSEditLog.adjustReplication( replication, conf );
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
          fsDir.unprotectedAddFile(name, blocks, replication );
        }

        // load datanode info
        this.loadDatanodes( imgVersion, in );
      } finally {
        in.close();
      }
    }
    if( fsDir.namespaceID == 0 )
      fsDir.namespaceID = newNamespaceID();

    return needToSave;
  }

  /**
   * Save the contents of the FS image
   */
  void saveFSImage(String filename) throws IOException {
    FSNamesystem fsNamesys = FSNamesystem.getFSNamesystem();
    FSDirectory fsDir = fsNamesys.dir;
    for (int idx = 0; idx < imageDirs.length; idx++) {
      File newFile = new File(imageDirs[idx], filename);
      
      //
      // Write out data
      //
      DataOutputStream out = new DataOutputStream(
            new BufferedOutputStream(
            new FileOutputStream(newFile)));
      try {
        out.writeInt(FSConstants.DFS_CURRENT_VERSION);
        out.writeInt(fsDir.namespaceID);
        out.writeInt(fsDir.rootDir.numItemsInTree() - 1);
        saveImage( "", fsDir.rootDir, out );
        saveDatanodes( out );
      } finally {
        out.close();
      }
    }
  }

  /**
   * Save the contents of the FS image
   */
  void saveFSImage() throws IOException {
    editLog.createNewIfMissing();
    saveFSImage(NameNodeFile.CKPT.getName());
    rollFSImage(false);
  }

  void updateTimeFile(File timeFile, long timestamp) throws IOException {
    if (timeFile.exists()) { timeFile.delete(); }
    DataOutputStream out = new DataOutputStream(
          new FileOutputStream(timeFile));
    try {
      out.writeLong(timestamp);
    } finally {
      out.close();
    }
  }

  /**
   * Generate new namespaceID.
   * 
   * namespaceID is a persistent attribute of the namespace.
   * It is generated when the namenode is formatted and remains the same
   * during the life cycle of the namenode.
   * When a datanodes register they receive it as the registrationID,
   * which is checked every time the datanode is communicating with the 
   * namenode. Datanodes that do not 'know' the namespaceID are rejected.
   * 
   * @return new namespaceID
   */
  private int newNamespaceID() {
    Random r = new Random();
    r.setSeed( System.currentTimeMillis() );
    int newID = 0;
    while( newID == 0)
      newID = r.nextInt();
    return newID;
  }
  
  /** Create new dfs name directory.  Caution: this destroys all files
   * in this filesystem. */
  static void format(File dir) throws IOException {
    File image = new File(dir, "image");
    File edits = new File(dir, "edits");
    
    if (!((!image.exists() || FileUtil.fullyDelete(image)) &&
        (!edits.exists() || edits.delete()) &&
        image.mkdirs())) {
      throw new IOException("Unable to format: "+dir);
    }
  }

  /**
   * Save file tree image starting from the given root.
   */
  void saveImage( String parentPrefix, 
                  FSDirectory.INode root, 
                  DataOutputStream out ) throws IOException {
    String fullName = "";
    if( root.getParent() != null) {
      fullName = parentPrefix + "/" + root.getLocalName();
      new UTF8(fullName).write(out);
      out.writeShort( root.getReplication() );
      if( root.isDir() ) {
        out.writeInt(0);
      } else {
        int nrBlocks = root.getBlocks().length;
        out.writeInt( nrBlocks );
        for (int i = 0; i < nrBlocks; i++)
          root.getBlocks()[i].write(out);
      }
    }
    for(Iterator<INode> it = root.getChildIterator(); it != null &&
                                                      it.hasNext(); ) {
      saveImage( fullName, it.next(), out );
    }
  }

  /**
   * Save list of datanodes contained in {@link FSNamesystem#datanodeMap}.
   * Only the {@link DatanodeInfo} part is stored.
   * The {@link DatanodeDescriptor#blocks} is transient.
   * 
   * @param out output stream
   * @throws IOException
   */
  void saveDatanodes( DataOutputStream out ) throws IOException {
    Map datanodeMap = FSNamesystem.getFSNamesystem().datanodeMap;
    int size = datanodeMap.size();
    out.writeInt( size );
    for( Iterator it = datanodeMap.values().iterator(); it.hasNext(); ) {
      DatanodeImage nodeImage = new DatanodeImage((DatanodeDescriptor) it.next());
      nodeImage.write( out );
    }
  }

  void loadDatanodes( int version, DataInputStream in ) throws IOException {
    if( version > -3 ) // pre datanode image version
      return;
    FSNamesystem fsNamesys = FSNamesystem.getFSNamesystem();
    int size = in.readInt();
    for( int i = 0; i < size; i++ ) {
      DatanodeImage nodeImage = new DatanodeImage();
      nodeImage.readFields(in);
      fsNamesys.unprotectedAddDatanode(nodeImage.getDatanodeDescriptor());
    }
  }
  /**
   * Moves fsimage.ckpt to fsImage and edits.new to edits
   * Reopens the new edits file.
   */
  void rollFSImage() throws IOException {
    rollFSImage(true);
  }

  /**
   * Moves fsimage.ckpt to fsImage and edits.new to edits
   */
  void rollFSImage(boolean reopenEdits) throws IOException {
    //
    // First, verify that edits.new and fsimage.ckpt exists in all
    // checkpoint directories.
    //
    if (!editLog.existsNew()) {
      throw new IOException("New Edits file does not exist");
    }
    for (int idx = 0; idx < imageDirs.length; idx++) {
      File ckpt = new File(imageDirs[idx], 
                           NameNodeFile.CKPT.getName());
      File curFile = new File(imageDirs[idx], 
                              NameNodeFile.IMAGE.getName());

      if (!curFile.exists()) {
        throw new IOException("Image file " + curFile +
                              " does not exist");
      }
      if (!ckpt.exists()) {
        throw new IOException("Checkpoint file " + ckpt +
                              " does not exist");
      }
    }
    editLog.purgeEditLog(reopenEdits); // renamed edits.new to edits

    //
    // Renames new image
    //
    for (int idx = 0; idx < imageDirs.length; idx++) {
      File ckpt = new File(imageDirs[idx], 
                           NameNodeFile.CKPT.getName());
      File curFile = new File(imageDirs[idx], 
                              NameNodeFile.IMAGE.getName());
      // renameTo fails on Windows if the destination file 
      // already exists.
      if (!ckpt.renameTo(curFile)) {
        curFile.delete();
        if (!ckpt.renameTo(curFile)) {
          editLog.processIOError(idx);
          idx--;
        }
      }
    }

    //
    // Updates the fstime file
    //
    long now = System.currentTimeMillis();
    for (int idx = 0; idx < imageDirs.length; idx++) {
	  File timeFile = new File(imageDirs[idx], 
                               NameNodeFile.TIME.getName());
      try {
        updateTimeFile(timeFile, now);
      } catch (IOException e) {
        editLog.processIOError(idx);
        idx--;
      }
    }
  }

  /**
   * Return the name of the image file.
   */
  File getFsImageName() {
      return new File(imageDirs[0], NameNodeFile.IMAGE.getName());
  }

  /**
   * Return the name of the image file that is uploaded by periodic
   * checkpointing.
   */
  File[] getFsImageNameCheckpoint() {
      File[] list = new File[imageDirs.length];
      for (int i = 0; i < imageDirs.length; i++) {
        list[i] = new File(imageDirs[i], 
                           NameNodeFile.CKPT.getName());
      }
      return list;
  }

  static class DatanodeImage implements WritableComparable {

    /**************************************************
     * DatanodeImage is used to store persistent information
     * about datanodes into the fsImage.
     **************************************************/
    DatanodeDescriptor              node;

    DatanodeImage() {
      node = new DatanodeDescriptor();
    }

    DatanodeImage(DatanodeDescriptor from) {
      node = from;
    }

    /** 
     * Returns the underlying Datanode Descriptor
     */
    DatanodeDescriptor getDatanodeDescriptor() { 
      return node; 
    }

    public int compareTo(Object o) {
      return node.compareTo(o);
    }

    /////////////////////////////////////////////////
    // Writable
    /////////////////////////////////////////////////
    /**
     * Public method that serializes the information about a
     * Datanode to be stored in the fsImage.
     */
    public void write(DataOutput out) throws IOException {
      DatanodeID id = new DatanodeID(node.getName(), node.getStorageID(),
                                     node.getInfoPort());
      id.write(out);
      out.writeLong(node.getCapacity());
      out.writeLong(node.getRemaining());
      out.writeLong(node.getLastUpdate());
      out.writeInt(node.getXceiverCount());
    }

    /**
     * Public method that reads a serialized Datanode
     * from the fsImage.
     */
    public void readFields(DataInput in) throws IOException {
      DatanodeID id = new DatanodeID();
      id.readFields(in);
      long capacity = in.readLong();
      long remaining = in.readLong();
      long lastUpdate = in.readLong();
      int xceiverCount = in.readInt();

      // update the DatanodeDescriptor with the data we read in
      node.updateRegInfo(id);
      node.setStorageID(id.getStorageID());
      node.setCapacity(capacity);
      node.setRemaining(remaining);
      node.setLastUpdate(lastUpdate);
      node.setXceiverCount(xceiverCount);
    }
  }
}
