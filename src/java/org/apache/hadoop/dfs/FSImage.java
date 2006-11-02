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
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.FSDirectory.INode;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.UTF8;

/**
 * FSImage handles checkpointing and logging of the namespace edits.
 * 
 * @author Konstantin Shvachko
 */
class FSImage {
  private static final String FS_IMAGE = "fsimage";
  private static final String NEW_FS_IMAGE = "fsimage.new";
  private static final String OLD_FS_IMAGE = "fsimage.old";
  private static final String FS_TIME = "fstime";

  private File[] imageDirs;  /// directories that contains the image file 
  private FSEditLog editLog;
  // private int namespaceID = 0;    /// a persistent attribute of the namespace

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
    File[] edits = new File[fsDirs.length];
    for (int idx = 0; idx < edits.length; idx++) {
      edits[idx] = new File(fsDirs[idx], "edits");
    }
    this.editLog = new FSEditLog( edits );
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
    FSNamesystem fsNamesys = FSNamesystem.getFSNamesystem();
    FSDirectory fsDir = fsNamesys.dir;
    for (int idx = 0; idx < imageDirs.length; idx++) {
      //
      // Atomic move sequence, to recover from interrupted save
      //
      File curFile = new File(imageDirs[idx], FS_IMAGE);
      File newFile = new File(imageDirs[idx], NEW_FS_IMAGE);
      File oldFile = new File(imageDirs[idx], OLD_FS_IMAGE);

      // Maybe we were interrupted between 2 and 4
      if (oldFile.exists() && curFile.exists()) {
        oldFile.delete();
        if (editLog.exists()) {
          editLog.deleteAll();
        }
      } else if (oldFile.exists() && newFile.exists()) {
        // Or maybe between 1 and 2
        newFile.renameTo(curFile);
        oldFile.delete();
      } else if (curFile.exists() && newFile.exists()) {
        // Or else before stage 1, in which case we lose the edits
        newFile.delete();
      }
    }
    
    // Now check all curFiles and see which is the newest
    File curFile = null;
    long maxTimeStamp = Long.MIN_VALUE;
    for (int idx = 0; idx < imageDirs.length; idx++) {
      File file = new File(imageDirs[idx], FS_IMAGE);
      if (file.exists()) {
        long timeStamp = 0;
        File timeFile = new File(imageDirs[idx], FS_TIME);
        if (timeFile.exists() && timeFile.canRead()) {
          DataInputStream in = new DataInputStream(
              new FileInputStream(timeFile));
          try {
            timeStamp = in.readLong();
          } finally {
            in.close();
          }
        }
        if (maxTimeStamp < timeStamp) {
          maxTimeStamp = timeStamp;
          curFile = file;
        }
      }
    }

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
        } else 
          numFiles = in.readInt();
        
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
    
    needToSave |= ( editLog.exists() && editLog.loadFSEdits(conf) > 0 );
    if( needToSave )
      saveFSImage();
  }

  /**
   * Save the contents of the FS image
   */
  void saveFSImage() throws IOException {
    FSNamesystem fsNamesys = FSNamesystem.getFSNamesystem();
    FSDirectory fsDir = fsNamesys.dir;
    for (int idx = 0; idx < imageDirs.length; idx++) {
      File newFile = new File(imageDirs[idx], NEW_FS_IMAGE);
      
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
    
    //
    // Atomic move sequence
    //
    for (int idx = 0; idx < imageDirs.length; idx++) {
      File curFile = new File(imageDirs[idx], FS_IMAGE);
      File newFile = new File(imageDirs[idx], NEW_FS_IMAGE);
      File oldFile = new File(imageDirs[idx], OLD_FS_IMAGE);
      File timeFile = new File(imageDirs[idx], FS_TIME);
      // 1.  Move cur to old and delete timeStamp
      curFile.renameTo(oldFile);
      if (timeFile.exists()) { timeFile.delete(); }
      // 2.  Move new to cur and write timestamp
      newFile.renameTo(curFile);
      DataOutputStream out = new DataOutputStream(
            new FileOutputStream(timeFile));
      try {
        out.writeLong(System.currentTimeMillis());
      } finally {
        out.close();
      }
      // 3.  Remove pending-edits file (it's been integrated with newFile)
      editLog.delete(idx);
      // 4.  Delete old
      oldFile.delete();
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
    for(Iterator it = root.getChildren().values().iterator(); it.hasNext(); ) {
      INode child = (INode) it.next();
      saveImage( fullName, child, out );
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
    for( Iterator it = datanodeMap.values().iterator(); it.hasNext(); )
      ((DatanodeDescriptor)it.next()).write( out );
  }

  void loadDatanodes( int version, DataInputStream in ) throws IOException {
    if( version > -3 ) // pre datanode image version
      return;
    FSNamesystem fsNamesys = FSNamesystem.getFSNamesystem();
    int size = in.readInt();
    for( int i = 0; i < size; i++ ) {
      DatanodeDescriptor node = new DatanodeDescriptor();
      node.readFields(in);
      fsNamesys.unprotectedAddDatanode( node );
    }
  }
}
