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
import java.util.ArrayList;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;

/**
 * FSEditLog maintains a log of the namespace modifications.
 * 
 */
class FSEditLog {
  private static final byte OP_ADD = 0;
  private static final byte OP_RENAME = 1;
  private static final byte OP_DELETE = 2;
  private static final byte OP_MKDIR = 3;
  private static final byte OP_SET_REPLICATION = 4;
  private static final byte OP_DATANODE_ADD = 5;
  private static final byte OP_DATANODE_REMOVE = 6;

  private ArrayList<EditLogOutputStream> editStreams = null;
  private FSImage fsimage = null;

  private long lastModificationTime;
  private long lastSyncTime;
  
  static class EditLogOutputStream extends DataOutputStream {
    private FileDescriptor fd;

    EditLogOutputStream(File name) throws IOException {
      super(new FileOutputStream(name, true)); // open for append
      this.fd = ((FileOutputStream)out).getFD();
    }

    void flushAndSync() throws IOException {
      this.flush();
      this.fd.sync();
    }

    void create() throws IOException {
      writeInt(FSConstants.LAYOUT_VERSION);
      flushAndSync();
    }
  }

  FSEditLog(FSImage image) {
    fsimage = image;
    lastModificationTime = 0;
    lastSyncTime = 0;
  }

  private File getEditFile(int idx) {
    return fsimage.getEditFile(idx);
  }

  private File getEditNewFile(int idx) {
    return fsimage.getEditNewFile(idx);
  }
  
  private int getNumStorageDirs() {
    return fsimage.getNumStorageDirs();
  }
  
  int getNumEditStreams() {
    return editStreams == null ? 0 : editStreams.size();
  }

  /**
   * Create empty edit log files.
   * Initialize the output stream for logging.
   * 
   * @throws IOException
   */
  void open() throws IOException {
    int size = getNumStorageDirs();
    if (editStreams == null)
      editStreams = new ArrayList<EditLogOutputStream>(size);
    for (int idx = 0; idx < size; idx++) {
      File eFile = getEditFile(idx);
      try {
        EditLogOutputStream eStream = new EditLogOutputStream(eFile);
        editStreams.add(eStream);
      } catch (IOException e) {
        FSNamesystem.LOG.warn("Unable to open edit log file " + eFile);
        processIOError(idx); 
        idx--; 
      }
    }
  }

  void createEditLogFile(File name) throws IOException {
    EditLogOutputStream eStream = new EditLogOutputStream(name);
    eStream.create();
    eStream.flushAndSync();
    eStream.close();
  }

  /**
   * Create edits.new if non existant.
   */
  void createNewIfMissing() throws IOException {
    for (int idx = 0; idx < getNumStorageDirs(); idx++) {
      File newFile = getEditNewFile(idx);
      if (!newFile.exists())
        createEditLogFile(newFile);
    }
  }
  
  /**
   * Shutdown the filestore
   */
  void close() throws IOException {
    if (editStreams == null) {
      return;
    }
    for (int idx = 0; idx < editStreams.size(); idx++) {
      EditLogOutputStream eStream = editStreams.get(idx);
      try {
        eStream.flushAndSync();
        eStream.close();
      } catch (IOException e) {
        processIOError(idx);
        idx--;
      }
    }
    editStreams.clear();
  }

  /**
   * If there is an IO Error on any log operations, remove that
   * directory from the list of directories. If no more directories
   * remain, then raise an exception that will possibly cause the
   * server to exit
   */
  void processIOError(int index) throws IOException {
    if (editStreams == null || editStreams.size() == 1) {
      throw new IOException("Checkpoint directories inaccessible.");
    }
    assert(index < getNumStorageDirs());
    assert(getNumStorageDirs() == editStreams.size());

    editStreams.remove(index);
    //
    // Invoke the ioerror routine of the fsimage
    //
    fsimage.processIOError(index);
  }

  /**
   * check if ANY edits.new log exists
   */
  boolean existsNew() throws IOException {
    for (int idx = 0; idx < getNumStorageDirs(); idx++) {
      if (getEditNewFile(idx).exists()) { 
        return true;
      }
    }
    return false;
  }

  /**
   * Load an edit log, and apply the changes to the in-memory structure
   * This is where we apply edits that we've been writing to disk all
   * along.
   */
  int loadFSEdits(File edits) throws IOException {
    FSNamesystem fsNamesys = FSNamesystem.getFSNamesystem();
    FSDirectory fsDir = fsNamesys.dir;
    int numEdits = 0;
    int logVersion = 0;
    
    if (edits != null) {
      DataInputStream in = new DataInputStream(
                                               new BufferedInputStream(
                                                                       new FileInputStream(edits)));
      // Read log file version. Could be missing. 
      in.mark(4);
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
        if (logVersion >= 0)
          logVersion = 0;
        else
          logVersion = in.readInt();
        if (logVersion < FSConstants.LAYOUT_VERSION) // future version
          throw new IOException(
                                "Unexpected version of the file system log file: "
                                + logVersion
                                + ". Current version = " 
                                + FSConstants.LAYOUT_VERSION + ".");
      }
      
      short replication = fsNamesys.getDefaultReplication();
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
            if (logVersion >= 0)
              name.readFields(in);  // read name only
            else {  // other versions do
              // get name and replication
              aw = new ArrayWritable(UTF8.class);
              aw.readFields(in);
              writables = aw.get(); 
              if (writables.length != 2)
                throw new IOException("Incorrect data fortmat. " 
                                      + "Name & replication pair expected");
              name = (UTF8) writables[0];
              replication = Short.parseShort(
                                             ((UTF8)writables[1]).toString());
              replication = adjustReplication(replication);
            }
            // get blocks
            aw = new ArrayWritable(Block.class);
            aw.readFields(in);
            writables = aw.get();
            Block blocks[] = new Block[writables.length];
            System.arraycopy(writables, 0, blocks, 0, blocks.length);
            // add to the file tree
            fsDir.unprotectedAddFile(name, blocks, replication);
            break;
          }
          case OP_SET_REPLICATION: {
            UTF8 src = new UTF8();
            UTF8 repl = new UTF8();
            src.readFields(in);
            repl.readFields(in);
            replication = adjustReplication(fromLogReplication(repl));
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
            if (logVersion > -3)
              throw new IOException("Unexpected opcode " + opcode 
                                    + " for version " + logVersion);
            FSImage.DatanodeImage nodeimage = new FSImage.DatanodeImage();
            nodeimage.readFields(in);
            DatanodeDescriptor node = nodeimage.getDatanodeDescriptor();
            fsNamesys.unprotectedAddDatanode(node);
            break;
          }
          case OP_DATANODE_REMOVE: {
            if (logVersion > -3)
              throw new IOException("Unexpected opcode " + opcode 
                                    + " for version " + logVersion);
            DatanodeID nodeID = new DatanodeID();
            nodeID.readFields(in);
            DatanodeDescriptor node = fsNamesys.getDatanode(nodeID);
            if (node != null) {
              fsNamesys.unprotectedRemoveDatanode(node);
              // physically remove node from datanodeMap
              fsNamesys.wipeDatanode(nodeID);
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
    
    if (logVersion != FSConstants.LAYOUT_VERSION) // other version
      numEdits++; // save this image asap
    return numEdits;
  }
  
  static short adjustReplication(short replication) {
    FSNamesystem fsNamesys = FSNamesystem.getFSNamesystem();
    short minReplication = fsNamesys.getMinReplication();
    if (replication<minReplication) {
      replication = minReplication;
    }
    short maxReplication = fsNamesys.getMaxReplication();
    if (replication>maxReplication) {
      replication = maxReplication;
    }
    return replication;
  }

  /**
   * Write an operation to the edit log. Do not sync to persistent
   * store yet.
   */
  synchronized void logEdit(byte op, Writable w1, Writable w2) {
    assert this.getNumEditStreams() > 0 : "no editlog streams";
    for (int idx = 0; idx < editStreams.size(); idx++) {
      EditLogOutputStream eStream;
      synchronized (eStream = editStreams.get(idx)) {
        try {
          eStream.write(op);
          if (w1 != null) {
            w1.write(eStream);
          }
          if (w2 != null) {
            w2.write(eStream);
          }
        } catch (IOException ie) {
          try {
            processIOError(idx);         
          } catch (IOException e) {
            FSNamesystem.LOG.error("Unable to append to edit log. " +
                                   "Fatal Error.");
            throw new RuntimeException("Unable to append to edit log. ");
          }
        }
      }
    }
    //
    // record the time when new data was written to the edits log
    //
    lastModificationTime = System.currentTimeMillis();
  }

  //
  // flush all data of the Edits log into persistent store
  //
  synchronized void logSync() {
    assert this.getNumEditStreams() > 0 : "no editlog streams";

    //
    // If data was generated before the beginning of the last sync time
    // then there is nothing to flush
    //
    if (lastModificationTime < lastSyncTime) {
      return;
    }
    lastSyncTime = System.currentTimeMillis();

    for (int idx = 0; idx < editStreams.size(); idx++) {
      EditLogOutputStream eStream;
      synchronized (eStream = editStreams.get(idx)) {
        try {
          eStream.flushAndSync();
        } catch (IOException ie) {
          try {
            processIOError(idx);         
          } catch (IOException e) {
            FSNamesystem.LOG.error("Unable to sync edit log. " +
                                   "Fatal Error.");
            throw new RuntimeException("Unable to sync edit log. " +
                                       "Fatal Error.");
          }
        }
      }
    }
  }

  /** 
   * Add create file record to edit log
   */
  void logCreateFile(FSDirectory.INode newNode) {
    UTF8 nameReplicationPair[] = new UTF8[] { 
      new UTF8(newNode.computeName()), 
      FSEditLog.toLogReplication(newNode.getReplication())};
    logEdit(OP_ADD,
            new ArrayWritable(UTF8.class, nameReplicationPair), 
            new ArrayWritable(Block.class, newNode.getBlocks()));
  }
  
  /** 
   * Add create directory record to edit log
   */
  void logMkDir(FSDirectory.INode newNode) {
    logEdit(OP_MKDIR, new UTF8(newNode.computeName()), null);
  }
  
  /** 
   * Add rename record to edit log
   * TODO: use String parameters until just before writing to disk
   */
  void logRename(UTF8 src, UTF8 dst) {
    logEdit(OP_RENAME, src, dst);
  }
  
  /** 
   * Add set replication record to edit log
   */
  void logSetReplication(String src, short replication) {
    logEdit(OP_SET_REPLICATION, 
            new UTF8(src), 
            FSEditLog.toLogReplication(replication));
  }
  
  /** 
   * Add delete file record to edit log
   */
  void logDelete(UTF8 src) {
    logEdit(OP_DELETE, src, null);
  }
  
  /** 
   * Creates a record in edit log corresponding to a new data node
   * registration event.
   */
  void logAddDatanode(DatanodeDescriptor node) {
    logEdit(OP_DATANODE_ADD, new FSImage.DatanodeImage(node), null);
  }
  
  /** 
   * Creates a record in edit log corresponding to a data node
   * removal event.
   */
  void logRemoveDatanode(DatanodeID nodeID) {
    logEdit(OP_DATANODE_REMOVE, new DatanodeID(nodeID), null);
  }
  
  static UTF8 toLogReplication(short replication) {
    return new UTF8(Short.toString(replication));
  }
  
  static short fromLogReplication(UTF8 replication) {
    return Short.parseShort(replication.toString());
  }

  /**
   * Return the size of the current EditLog
   */
  long getEditLogSize() throws IOException {
    assert(getNumStorageDirs() == editStreams.size());
    long size = 0;
    for (int idx = 0; idx < getNumStorageDirs(); idx++) {
      synchronized (editStreams.get(idx)) {
        assert(size == 0 || size == getEditFile(idx).length());
        size = getEditFile(idx).length();
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
    for (int idx = 0; idx < getNumStorageDirs(); idx++) {
      try {
        EditLogOutputStream eStream = new EditLogOutputStream(getEditNewFile(idx));
        eStream.create();
        editStreams.add(eStream);
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
    for (int idx = 0; idx < getNumStorageDirs(); idx++) {
      if (!getEditNewFile(idx).renameTo(getEditFile(idx))) {
        //
        // renameTo() fails on Windows if the destination
        // file exists.
        //
        getEditFile(idx).delete();
        if (!getEditNewFile(idx).renameTo(getEditFile(idx))) {
          processIOError(idx); 
          idx--; 
        }
      }
    }
    //
    // Reopen all the edits logs.
    //
    open();
  }

  /**
   * Return the name of the edit file
   */
  File getFsEditName() throws IOException {
    return getEditFile(0);
  }
}
