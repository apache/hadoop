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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;

/**
 * FSEditLog maintains a log of the namespace modifications.
 * 
 * @author Konstantin Shvachko
 */
class FSEditLog {
  private static final byte OP_ADD = 0;
  private static final byte OP_RENAME = 1;
  private static final byte OP_DELETE = 2;
  private static final byte OP_MKDIR = 3;
  private static final byte OP_SET_REPLICATION = 4;
  private static final byte OP_DATANODE_ADD = 5;
  private static final byte OP_DATANODE_REMOVE = 6;

  private static final String FS_EDIT = "edits";
  private static final String FS_EDIT_NEW = "edits.new";
  
  private File[] editFiles = null;
  private File[] editFilesNew = null;

  DataOutputStream[] editStreams = null;
  FileDescriptor[] editDescriptors = null;
  private FSImage fsimage = null;

  FSEditLog(File[] fsDirs, FSImage image)  throws IOException {
    fsimage = image;
    editFiles = new File[fsDirs.length];
    editFilesNew = new File[fsDirs.length];
    for (int idx = 0; idx < fsDirs.length; idx++) {
       editFiles[idx] = new File(fsDirs[idx], FS_EDIT);
       editFilesNew[idx] = new File(fsDirs[idx], FS_EDIT_NEW);
     }
   }

  FSEditLog(File imageDir, FSImage image, String edits)  throws IOException {
    fsimage = image;
    editFiles = new File[1];
    editFiles[0] = new File(imageDir, edits);
   }

  /**
   * Initialize the output stream for logging.
   * 
   * @throws IOException
   */
  void create() throws IOException {
    editStreams = new DataOutputStream[editFiles.length];
    editDescriptors = new FileDescriptor[editFiles.length];
    for (int idx = 0; idx < editStreams.length; idx++) {
      FileOutputStream stream = new FileOutputStream(editFiles[idx]);
      editStreams[idx] = new DataOutputStream(stream);
      editDescriptors[idx] = stream.getFD();
      editStreams[idx].writeInt( FSConstants.DFS_CURRENT_VERSION );
    }
  }

  /**
   * Create edits.new if non existant.
   */
  void createNewIfMissing() throws IOException {
    for (int idx = 0; idx < editFilesNew.length; idx++) {
      if (!editFilesNew[idx].exists()) {
        FileOutputStream stream = new FileOutputStream(editFilesNew[idx]);
        DataOutputStream editStr = new DataOutputStream(stream);
        editStr.writeInt( FSConstants.DFS_CURRENT_VERSION );
        editStr.flush();
        editStr.close();
      } 
    }
  }
  
  /**
   * Shutdown the filestore
   */
  void close() throws IOException {
    if (editStreams == null) {
      return;
    }
    for (int idx = 0; idx < editStreams.length; idx++) {
      try {
        editStreams[idx].flush();
        editDescriptors[idx].sync();
        editStreams[idx].close();
      } catch (IOException e) {
        processIOError(idx);
        idx--;
      }
    }
  }

  /**
   * If there is an IO Error on any log operations, remove that
   * directory from the list of directories. If no more directories
   * remain, then raise an exception that will possibly cause the
   * server to exit
   */
   void processIOError(int index) throws IOException {
     if (editStreams == null || editStreams.length == 1) {
       throw new IOException("Checkpoint directories inaccessible.");
     }
     assert(index < editFiles.length);
     assert(editFiles.length == editFilesNew.length);
     assert(editFiles.length == editStreams.length);
     int newsize = editStreams.length - 1;
     int oldsize = editStreams.length;

     //
     // save existing values and allocate space for new ones
     //
     File[] editFiles1 = editFiles;
     File[] editFilesNew1 = editFilesNew;
     DataOutputStream[] editStreams1 = editStreams;
     FileDescriptor[] editDescriptors1 = editDescriptors;
     editFiles = new File[newsize];
     editFilesNew = new File[newsize];
     editStreams = new DataOutputStream[newsize];
     editDescriptors = new FileDescriptor[newsize];

     //
     // copy values from old into new, skip the one with error.
     //
     for (int idx = 0; idx < index; idx++) {
       editFiles[idx] = editFiles1[idx];
       editFilesNew[idx] = editFilesNew1[idx];
       editStreams[idx] = editStreams1[idx];
       editDescriptors[idx] = editDescriptors1[idx];
     }
     for (int idx = index; idx < oldsize - 1; idx++) {
       editFiles[idx] = editFiles1[idx+1];
       editFilesNew[idx] = editFilesNew1[idx+1];
       editStreams[idx] = editStreams1[idx+1];
       editDescriptors[idx] = editDescriptors1[idx+1];
     }
     //
     // Invoke the ioerror routine of the fsimage
     //
     fsimage.processIOError(index);
   }

  /**
   * Delete specified editLog
   */
  void delete(int idx) throws IOException {
    if (editStreams != null) {
      try {
        editStreams[idx].close();
      } catch (IOException e) {
        processIOError(idx);
      }
    }
    if (!editFiles[idx].delete() || !editFilesNew[idx].delete()) {
      if (editStreams != null) {
        processIOError(idx);
      }
    }
  }

  /**
   * check if ANY edits log exists
   */
  boolean exists() throws IOException {
    for (int idx = 0; idx < editFiles.length; idx++) {
      if (editFiles[idx].exists()) { 
        return true;
      }
    }
    return false;
  }

  /**
   * check if ANY edits.new log exists
   */
  boolean existsNew() throws IOException {
    for (int idx = 0; idx < editFilesNew.length; idx++) {
      if (editFilesNew[idx].exists()) { 
        return true;
      }
    }
    return false;
  }

  /**
   * check if a particular edits.new log exists
   */
  boolean existsNew(int idx) throws IOException {
    if (editFilesNew[idx].exists()) { 
      return true;
    }
    return false;
  }

  
  /**
   * Load an edit log, and apply the changes to the in-memory structure
   * This is where we apply edits that we've been writing to disk all
   * along.
   */
  int loadFSEdits(Configuration conf, int index) throws IOException {
    int numEdits = 0;
    numEdits = loadFSEdits(conf, editFiles[index]);
    if (editFilesNew[index].exists()) { 
      numEdits += loadFSEdits(conf, editFilesNew[index]);
    }
    return numEdits;
  }

  int loadFSEdits( Configuration conf, File edits)
                                                 throws IOException {
    FSNamesystem fsNamesys = FSNamesystem.getFSNamesystem();
    FSDirectory fsDir = fsNamesys.dir;
    int numEdits = 0;
    int logVersion = 0;
    
    if (edits != null) {
      DataInputStream in = new DataInputStream(
          new BufferedInputStream(
              new FileInputStream(edits)));
      // Read log file version. Could be missing. 
      in.mark( 4 );
      // If edits log is greater than 2G, available method will return negative
      // numbers, so we avoid having to call available
      boolean available = true;
      try {
        logVersion = in.readByte();
      } catch (EOFException e) {
        available = false;
      }
      if (available) {
        in.reset();
        if( logVersion >= 0 )
          logVersion = 0;
        else
          logVersion = in.readInt();
        if( logVersion < FSConstants.DFS_CURRENT_VERSION ) // future version
          throw new IOException(
              "Unexpected version of the file system log file: "
              + logVersion
              + ". Current version = " 
              + FSConstants.DFS_CURRENT_VERSION + "." );
      }
      
      short replication = (short)conf.getInt("dfs.replication", 3);
      try {
        while (true) {
          byte opcode = -1;
          try {
            opcode = in.readByte();
          } catch (EOFException e) {
            break; // no more transactions
          }
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
            fsDir.unprotectedAddFile(name, blocks, replication );
            break;
          }
          case OP_SET_REPLICATION: {
            UTF8 src = new UTF8();
            UTF8 repl = new UTF8();
            src.readFields(in);
            repl.readFields(in);
            replication = adjustReplication(
                            fromLogReplication(repl),
                            conf);
            fsDir.unprotectedSetReplication(src.toString(), 
                replication,
                null);
            break;
          } 
          case OP_RENAME: {
            UTF8 src = new UTF8();
            UTF8 dst = new UTF8();
            src.readFields(in);
            dst.readFields(in);
            fsDir.unprotectedRenameTo(src, dst);
            break;
          }
          case OP_DELETE: {
            UTF8 src = new UTF8();
            src.readFields(in);
            fsDir.unprotectedDelete(src);
            break;
          }
          case OP_MKDIR: {
            UTF8 src = new UTF8();
            src.readFields(in);
            fsDir.unprotectedMkdir(src.toString());
            break;
          }
          case OP_DATANODE_ADD: {
            if( logVersion > -3 )
              throw new IOException("Unexpected opcode " + opcode 
                  + " for version " + logVersion );
            FSImage.DatanodeImage nodeimage = new FSImage.DatanodeImage();
            nodeimage.readFields(in);
            DatanodeDescriptor node = nodeimage.getDatanodeDescriptor();
            fsNamesys.unprotectedAddDatanode( node );
            break;
          }
          case OP_DATANODE_REMOVE: {
            if( logVersion > -3 )
              throw new IOException("Unexpected opcode " + opcode 
                  + " for version " + logVersion );
            DatanodeID nodeID = new DatanodeID();
            nodeID.readFields(in);
            DatanodeDescriptor node = fsNamesys.getDatanode( nodeID );
            if( node != null ) {
              fsNamesys.unprotectedRemoveDatanode( node );
              // physically remove node from datanodeMap
              fsNamesys.wipeDatanode( nodeID );
            }
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
    
    if( logVersion != FSConstants.DFS_CURRENT_VERSION ) // other version
      numEdits++; // save this image asap
    return numEdits;
  }
  
  static short adjustReplication( short replication, Configuration conf) {
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
   * Write an operation to the edit log
   */
  void logEdit(byte op, Writable w1, Writable w2) {
    for (int idx = 0; idx < editStreams.length; idx++) {
      synchronized (editStreams[idx]) {
        try {
          editStreams[idx].write(op);
          if (w1 != null) {
            w1.write(editStreams[idx]);
          }
          if (w2 != null) {
            w2.write(editStreams[idx]);
          }
          editStreams[idx].flush();
          editDescriptors[idx].sync();
        } catch (IOException ie) {
          try {
            processIOError(idx);         
          } catch (IOException e) {
            FSNamesystem.LOG.error("Unable to append to edit log. " +
                                   "Fatal Error.");
            System.exit(-1);
          }
        }
      }
    }
  }

  /** 
   * Add create file record to edit log
   */
  void logCreateFile( FSDirectory.INode newNode ) {
    UTF8 nameReplicationPair[] = new UTF8[] { 
                        new UTF8( newNode.computeName() ), 
                        FSEditLog.toLogReplication( newNode.getReplication() )};
    logEdit(OP_ADD,
            new ArrayWritable( UTF8.class, nameReplicationPair ), 
            new ArrayWritable( Block.class, newNode.getBlocks() ));
  }
  
  /** 
   * Add create directory record to edit log
   */
  void logMkDir( FSDirectory.INode newNode ) {
    logEdit(OP_MKDIR, new UTF8( newNode.computeName() ), null );
  }
  
  /** 
   * Add rename record to edit log
   * TODO: use String parameters until just before writing to disk
   */
  void logRename( UTF8 src, UTF8 dst ) {
    logEdit(OP_RENAME, src, dst);
  }
  
  /** 
   * Add set replication record to edit log
   */
  void logSetReplication( String src, short replication ) {
    logEdit(OP_SET_REPLICATION, 
            new UTF8(src), 
            FSEditLog.toLogReplication( replication ));
  }
  
  /** 
   * Add delete file record to edit log
   */
  void logDelete( UTF8 src ) {
    logEdit(OP_DELETE, src, null);
  }
  
  /** 
   * Creates a record in edit log corresponding to a new data node
   * registration event.
   */
  void logAddDatanode( DatanodeDescriptor node ) {
    logEdit( OP_DATANODE_ADD, new FSImage.DatanodeImage(node), null );
  }
  
  /** 
   * Creates a record in edit log corresponding to a data node
   * removal event.
   */
  void logRemoveDatanode( DatanodeID nodeID ) {
    logEdit( OP_DATANODE_REMOVE, new DatanodeID( nodeID ), null );
  }
  
  static UTF8 toLogReplication( short replication ) {
    return new UTF8( Short.toString(replication));
  }
  
  static short fromLogReplication( UTF8 replication ) {
    return Short.parseShort(replication.toString());
  }

  /**
   * Return the size of the current EditLog
   */
  long getEditLogSize() throws IOException {
    assert(editFiles.length == editStreams.length);
    long size = 0;
    for (int idx = 0; idx < editFiles.length; idx++) {
      synchronized (editStreams[idx]) {
        assert(size == 0 || size == editFiles[idx].length());
        size = editFiles[idx].length();
      }
    }
    return size;
  }
 
  /**
   * Closes the current edit log and opens edits.new. 
   */
  void rollEditLog() throws IOException {
    //
    // If edits.new already exists, then return error.
    //
    if (existsNew()) {
      throw new IOException("Attempt to roll edit log but edits.new exists");
    }

    close();                     // close existing edit log

    //
    // Open edits.new
    //
    for (int idx = 0; idx < editFiles.length; idx++ ) {
      try {
        FileOutputStream stream = new FileOutputStream(editFilesNew[idx]);
        editStreams[idx] = new DataOutputStream(stream);
        editDescriptors[idx] = stream.getFD();
        editStreams[idx].writeInt( FSConstants.DFS_CURRENT_VERSION );
      } catch (IOException e) {
        processIOError(idx);
        idx--;
      }
    }
  }

  /**
   * Closes the current edit log and opens edits.new. 
   * If edits.new already exists, then ignore it.
   */
  void rollEditLogIfNeeded() throws IOException {

    //
    // Open edits.new
    //
    for (int idx = 0; idx < editFiles.length; idx++ ) {
      if (existsNew(idx)) {
        continue;
      }
      try {
        FileOutputStream stream = new FileOutputStream(editFilesNew[idx]);
        editStreams[idx] = new DataOutputStream(stream);
        editDescriptors[idx] = stream.getFD();
        editStreams[idx].writeInt( FSConstants.DFS_CURRENT_VERSION );
      } catch (IOException e) {
        processIOError(idx);
        idx--;
      }
    }
  }
  /**
   * Removes the old edit log and renamed edits.new as edits.
   * Reopens the edits file.
   */
  void purgeEditLog() throws IOException {
    purgeEditLog(true);
  }

  /**
   * Removes the old edit log and renamed edits.new as edits.
   */
  void purgeEditLog(boolean reopenEdits) throws IOException {
    //
    // If edits.new does not exists, then return error.
    //
    if (!existsNew()) {
      throw new IOException("Attempt to purge edit log " +
                            "but edits.new does not exist.");
    }
    close();

    //
    // Delete edits and rename edits.new to edits.
    //
    for (int idx = 0; idx < editFiles.length; idx++ ) {
      if (!editFilesNew[idx].renameTo(editFiles[idx])) {
        //
        // renameTo() fails on Windows if the destination
        // file exists.
        //
        editFiles[idx].delete();
        if (!editFilesNew[idx].renameTo(editFiles[idx])) {
          processIOError(idx); 
          idx--; 
        }
      }
    }
    //
    // Reopen all the edits logs.
    //
    boolean append = true;
    for (int idx = 0; reopenEdits && idx < editStreams.length; idx++) {
      try {
        FileOutputStream stream = new FileOutputStream(editFiles[idx],
                                                       append);
        editStreams[idx] = new DataOutputStream(stream);
        editDescriptors[idx] = stream.getFD();
      } catch (IOException e) {
        processIOError(idx); 
        idx--; 
      }
    }
  }

  /**
   * Return the name of the edit file
   */
  File getFsEditName() throws IOException {
      return editFiles[0];
  }
}
