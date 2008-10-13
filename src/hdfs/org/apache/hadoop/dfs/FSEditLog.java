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
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.lang.Math;
import java.nio.channels.FileChannel;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.dfs.DFSFileInfo;

/**
 * FSEditLog maintains a log of the namespace modifications.
 * 
 */
class FSEditLog {
  private static final byte OP_ADD = 0;
  private static final byte OP_RENAME = 1;  // rename
  private static final byte OP_DELETE = 2;  // delete
  private static final byte OP_MKDIR = 3;   // create directory
  private static final byte OP_SET_REPLICATION = 4; // set replication
  //the following two are used only for backward compatibility :
  @Deprecated private static final byte OP_DATANODE_ADD = 5;
  @Deprecated private static final byte OP_DATANODE_REMOVE = 6;
  private static final byte OP_SET_PERMISSIONS = 7;
  private static final byte OP_SET_OWNER = 8;
  private static final byte OP_CLOSE = 9;    // close after write
  private static final byte OP_SET_GENSTAMP = 10;    // store genstamp
  private static final byte OP_SET_QUOTA = 11; // set a directory's quota
  private static final byte OP_CLEAR_QUOTA = 12; // clear a directory's quota
  private static int sizeFlushBuffer = 512*1024;

  private ArrayList<EditLogOutputStream> editStreams = null;
  private FSImage fsimage = null;

  // a monotonically increasing counter that represents transactionIds.
  private long txid = 0;

  // stores the last synced transactionId.
  private long synctxid = 0;

  // the time of printing the statistics to the log file.
  private long lastPrintTime;

  // is a sync currently running?
  private boolean isSyncRunning;

  // these are statistics counters.
  private long numTransactions;        // number of transactions
  private long totalTimeTransactions;  // total time for all transactions
  private NameNodeMetrics metrics;

  private static class TransactionId {
    public long txid;

    TransactionId(long value) {
      this.txid = value;
    }
  }

  // stores the most current transactionId of this thread.
  private static final ThreadLocal<TransactionId> myTransactionId = new ThreadLocal<TransactionId>() {
    protected synchronized TransactionId initialValue() {
      return new TransactionId(Long.MAX_VALUE);
    }
  };

  static class EditLogOutputStream {
    private FileChannel fc;
    private FileOutputStream fp;
    private DataOutputStream od;
    private DataOutputStream od1;
    private DataOutputStream od2;
    private ByteArrayOutputStream buf1;
    private ByteArrayOutputStream buf2;
    private int bufSize;

    // these are statistics counters
    private long numSync;        // number of syncs to disk
    private long totalTimeSync;  // total time to sync

    EditLogOutputStream(File name) throws IOException {
      bufSize = sizeFlushBuffer;
      buf1 = new ByteArrayOutputStream(bufSize);
      buf2 = new ByteArrayOutputStream(bufSize);
      od1 = new DataOutputStream(buf1);
      od2 = new DataOutputStream(buf2);
      od = od1;                              // start with first buffer
      fp = new FileOutputStream(name, true); // open for append
      fc = fp.getChannel();
      numSync = totalTimeSync = 0;
    }

    // returns the current output stream
    DataOutputStream getOutputStream() {
      return od;
    }

    void flushAndSync() throws IOException {
      this.flush();
      fc.force(true);
    }

    void create() throws IOException {
      fc.truncate(0);
      od.writeInt(FSConstants.LAYOUT_VERSION);
      flushAndSync();
    }

    // flush current buffer
    private void flush() throws IOException {
      ByteArrayOutputStream buf = getBuffer();
      if (buf.size() == 0) {
        return;                // no data to flush
      }
      buf.writeTo(fp);         // write data to file
      buf.reset();             // erase all data in buf
    }

    void close() throws IOException {
      // close should have been called after all pending transactions 
      // have been flushed & synced.
      if (getBufSize() != 0) {
        throw new IOException("FSEditStream has " + getBufSize() +
                              " bytes still to be flushed and cannot " +
                              "closed.");
      } 
      od.close();
      fp.close();
      buf1 = buf2 = null;
      od = od1 = od2 = null;
    }

    // returns the amount of data in the buffer
    int getBufSize() {
      return getBuffer().size();
    }

    // get the current buffer
    private ByteArrayOutputStream getBuffer() {
      if (od == od1) {
        return buf1;
      } else {
        return buf2;
      }
    }

    //
    // Flush current buffer to output stream, swap buffers
    // This is protected by the flushLock.
    //
    void swap() {
      if (od == od1) {
        od = od2;
      } else {
        od = od1;
      }
    }

    //
    // Flush old buffer to persistent store
    //
    void flushAndSyncOld() throws IOException {
      numSync++;
      ByteArrayOutputStream oldbuf;
      if (od == od1) {
        oldbuf = buf2;
      } else {
        oldbuf = buf1;
      }
      long start = FSNamesystem.now();
      oldbuf.writeTo(fp);         // write data to file
      oldbuf.reset();             // erase all data in buf
      fc.force(true);             // sync to persistent store
      long end = FSNamesystem.now();
      totalTimeSync += (end - start);
    }

    long getTotalSyncTime() {
      return totalTimeSync;
    }

    long getNumSync() {
      return numSync;
    }
  }

  FSEditLog(FSImage image) {
    fsimage = image;
    isSyncRunning = false;
    metrics = NameNode.getNameNodeMetrics();
    lastPrintTime = FSNamesystem.now();
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
  
  synchronized int getNumEditStreams() {
    return editStreams == null ? 0 : editStreams.size();
  }

  boolean isOpen() {
    return getNumEditStreams() > 0;
  }

  /**
   * Create empty edit log files.
   * Initialize the output stream for logging.
   * 
   * @throws IOException
   */
  synchronized void open() throws IOException {
    numTransactions = totalTimeTransactions = 0;
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
        fsimage.processIOError(idx);
        idx--; 
      }
    }
  }

  synchronized void createEditLogFile(File name) throws IOException {
    EditLogOutputStream eStream = new EditLogOutputStream(name);
    eStream.create();
    eStream.close();
  }

  /**
   * Create edits.new if non existant.
   */
  synchronized void createNewIfMissing() throws IOException {
    for (int idx = 0; idx < getNumStorageDirs(); idx++) {
      File newFile = getEditNewFile(idx);
      if (!newFile.exists())
        createEditLogFile(newFile);
    }
  }
  
  /**
   * Shutdown the filestore
   */
  synchronized void close() throws IOException {
    while (isSyncRunning) {
      try {
        wait(1000);
      } catch (InterruptedException ie) { 
      }
    }
    if (editStreams == null) {
      return;
    }
    printStatistics(true);
    numTransactions = totalTimeTransactions = 0;

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
   * directory from the list of directories.
   * If no more directories remain, then exit.
   */
  synchronized void processIOError(int index) {
    if (editStreams == null || editStreams.size() <= 1) {
      FSNamesystem.LOG.fatal(
      "Fatal Error : All storage directories are inaccessible."); 
      Runtime.getRuntime().exit(-1);
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
   * The specified streams have IO errors. Remove them from logging
   * new transactions.
   */
  private void processIOError(ArrayList<EditLogOutputStream> errorStreams) {
    if (errorStreams == null) {
      return;                       // nothing to do
    }
    for (int idx = 0; idx < errorStreams.size(); idx++) {
      EditLogOutputStream eStream = errorStreams.get(idx);
      int j = 0;
      for (j = 0; j < editStreams.size(); j++) {
        if (editStreams.get(j) == eStream) {
          break;
        }
      }
      if (j == editStreams.size()) {
          FSNamesystem.LOG.error("Unable to find sync log on which " +
                                 " IO error occured. " +
                                 "Fatal Error.");
          Runtime.getRuntime().exit(-1);
      }
      processIOError(j);
    }
    int failedStreamIdx = 0;
    while(failedStreamIdx >= 0) {
      failedStreamIdx = fsimage.incrementCheckpointTime();
      if(failedStreamIdx >= 0)
        processIOError(failedStreamIdx);
    }
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
    INode old = null;
    String clientName = null;
    String clientMachine = null;
    String path = null;
    int numOpAdd = 0, numOpClose = 0, numOpDelete = 0,
        numOpRename = 0, numOpSetRepl = 0, numOpMkDir = 0,
        numOpSetPerm = 0, numOpSetOwner = 0, numOpSetGenStamp = 0,
        numOpOther = 0;
    long startTime = FSNamesystem.now();

    if (edits != null) {
      DataInputStream in = new DataInputStream(new BufferedInputStream(
                                new FileInputStream(edits)));
      try {
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
          logVersion = in.readInt();
          if (logVersion < FSConstants.LAYOUT_VERSION) // future version
            throw new IOException(
                            "Unexpected version of the file system log file: "
                            + logVersion + ". Current version = " 
                            + FSConstants.LAYOUT_VERSION + ".");
        }
        assert logVersion <= Storage.LAST_UPGRADABLE_LAYOUT_VERSION :
                              "Unsupported version " + logVersion;
  
        while (true) {
          long timestamp = 0;
          long mtime = 0;
          long blockSize = 0;
          byte opcode = -1;
          try {
            opcode = in.readByte();
          } catch (EOFException e) {
            break; // no more transactions
          }
          numEdits++;
          switch (opcode) {
          case OP_ADD:
          case OP_CLOSE: {
            // versions > 0 support per file replication
            // get name and replication
            int length = in.readInt();
            if (-7 == logVersion && length != 3||
                logVersion < -7 && length != 4) {
                throw new IOException("Incorrect data format."  +
                                      " logVersion is " + logVersion +
                                      " but writables.length is " +
                                      length + ". ");
            }
            path = FSImage.readString(in);
            short replication = adjustReplication(readShort(in));
            mtime = readLong(in);
            if (logVersion < -7) {
              blockSize = readLong(in);
            }
            // get blocks
            Block blocks[] = null;
            if (logVersion <= -14) {
              blocks = readBlocks(in);
            } else {
              BlockTwo oldblk = new BlockTwo();
              int num = in.readInt();
              blocks = new Block[num];
              for (int i = 0; i < num; i++) {
                oldblk.readFields(in);
                blocks[i] = new Block(oldblk.blkid, oldblk.len, 
                                      Block.GRANDFATHER_GENERATION_STAMP);
              }
            }

            // Older versions of HDFS does not store the block size in inode.
            // If the file has more than one block, use the size of the
            // first block as the blocksize. Otherwise use the default
            // block size.
            if (-8 <= logVersion && blockSize == 0) {
              if (blocks.length > 1) {
                blockSize = blocks[0].getNumBytes();
              } else {
                long first = ((blocks.length == 1)? blocks[0].getNumBytes(): 0);
                blockSize = Math.max(fsNamesys.getDefaultBlockSize(), first);
              }
            }
             
            PermissionStatus permissions = fsNamesys.getUpgradePermission();
            if (logVersion <= -11) {
              permissions = PermissionStatus.read(in);
            }

            // clientname, clientMachine and block locations of last block.
            if (opcode == OP_ADD && logVersion <= -12) {
              clientName = FSImage.readString(in);
              clientMachine = FSImage.readString(in);
              if (-13 <= logVersion) {
                readDatanodeDescriptorArray(in);
              }
            } else {
              clientName = "";
              clientMachine = "";
            }
  
            // The open lease transaction re-creates a file if necessary.
            // Delete the file if it already exists.
            if (FSNamesystem.LOG.isDebugEnabled()) {
              FSNamesystem.LOG.debug(opcode + ": " + path + 
                                     " numblocks : " + blocks.length +
                                     " clientHolder " +  clientName +
                                     " clientMachine " + clientMachine);
            }

            old = fsDir.unprotectedDelete(path, mtime);

            // add to the file tree
            INodeFile node = (INodeFile)fsDir.unprotectedAddFile(
                                                      path, permissions,
                                                      blocks, replication, 
                                                      mtime, blockSize);
            if (opcode == OP_ADD) {
              numOpAdd++;
              //
              // Replace current node with a INodeUnderConstruction.
              // Recreate in-memory lease record.
              //
              INodeFileUnderConstruction cons = new INodeFileUnderConstruction(
                                        node.getLocalNameBytes(),
                                        node.getReplication(), 
                                        node.getModificationTime(),
                                        node.getPreferredBlockSize(),
                                        node.getBlocks(),
                                        node.getPermissionStatus(),
                                        clientName, 
                                        clientMachine, 
                                        null);
              fsDir.replaceNode(path, node, cons);
              fsNamesys.leaseManager.addLease(cons.clientName, path);
            } else if (opcode == OP_CLOSE) {
              //
              // Remove lease if it exists.
              //
              if (old.isUnderConstruction()) {
                INodeFileUnderConstruction cons = (INodeFileUnderConstruction)
                                                     old;
                fsNamesys.leaseManager.removeLease(cons.clientName, path);
              }
            }
            break;
          } 
          case OP_SET_REPLICATION: {
            numOpSetRepl++;
            path = FSImage.readString(in);
            short replication = adjustReplication(readShort(in));
            fsDir.unprotectedSetReplication(path, replication, null);
            break;
          } 
          case OP_RENAME: {
            numOpRename++;
            int length = in.readInt();
            if (length != 3) {
              throw new IOException("Incorrect data format. " 
                                    + "Mkdir operation.");
            }
            String s = FSImage.readString(in);
            String d = FSImage.readString(in);
            timestamp = readLong(in);
            DFSFileInfo dinfo = fsDir.getFileInfo(d);
            fsDir.unprotectedRenameTo(s, d, timestamp);
            fsNamesys.changeLease(s, d, dinfo);
            break;
          }
          case OP_DELETE: {
            numOpDelete++;
            int length = in.readInt();
            if (length != 2) {
              throw new IOException("Incorrect data format. " 
                                    + "delete operation.");
            }
            path = FSImage.readString(in);
            timestamp = readLong(in);
            old = fsDir.unprotectedDelete(path, timestamp);
            if (old != null && old.isUnderConstruction()) {
              INodeFileUnderConstruction cons = (INodeFileUnderConstruction)old;
              fsNamesys.leaseManager.removeLease(cons.clientName, path);
            }
            break;
          }
          case OP_MKDIR: {
            numOpMkDir++;
            PermissionStatus permissions = fsNamesys.getUpgradePermission();
            int length = in.readInt();
            if (length != 2) {
              throw new IOException("Incorrect data format. " 
                                    + "Mkdir operation.");
            }
            path = FSImage.readString(in);
            timestamp = readLong(in);

            if (logVersion <= -11) {
              permissions = PermissionStatus.read(in);
            }
            fsDir.unprotectedMkdir(path, permissions, timestamp);
            break;
          }
          case OP_SET_GENSTAMP: {
            numOpSetGenStamp++;
            long lw = in.readLong();
            fsDir.namesystem.setGenerationStamp(lw);
            break;
          } 
          case OP_DATANODE_ADD: {
            numOpOther++;
            FSImage.DatanodeImage nodeimage = new FSImage.DatanodeImage();
            nodeimage.readFields(in);
            //Datnodes are not persistent any more.
            break;
          }
          case OP_DATANODE_REMOVE: {
            numOpOther++;
            DatanodeID nodeID = new DatanodeID();
            nodeID.readFields(in);
            //Datanodes are not persistent any more.
            break;
          }
          case OP_SET_PERMISSIONS: {
            numOpSetPerm++;
            if (logVersion > -11)
              throw new IOException("Unexpected opcode " + opcode
                                    + " for version " + logVersion);
            fsDir.unprotectedSetPermission(
                FSImage.readString(in), FsPermission.read(in));
            break;
          }
          case OP_SET_OWNER: {
            numOpSetOwner++;
            if (logVersion > -11)
              throw new IOException("Unexpected opcode " + opcode
                                    + " for version " + logVersion);
            fsDir.unprotectedSetOwner(FSImage.readString(in),
                FSImage.readString_EmptyAsNull(in),
                FSImage.readString_EmptyAsNull(in));
            break;
          }
          case OP_SET_QUOTA: {
            if (logVersion > -16) {
              throw new IOException("Unexpected opcode " + opcode
                  + " for version " + logVersion);
            }
            fsDir.unprotectedSetQuota(FSImage.readString(in), 
                readLongWritable(in) );
            break;
          }
          case OP_CLEAR_QUOTA: {
            if (logVersion > -16) {
              throw new IOException("Unexpected opcode " + opcode
                  + " for version " + logVersion);
            }
            fsDir.unprotectedClearQuota(FSImage.readString(in));
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
      FSImage.LOG.info("Edits file " + edits.getName() 
          + " of size " + edits.length() + " edits # " + numEdits 
          + " loaded in " + (FSNamesystem.now()-startTime)/1000 + " seconds.");
    }

    if (FSImage.LOG.isDebugEnabled()) {
      FSImage.LOG.debug("numOpAdd = " + numOpAdd + " numOpClose = " + numOpClose 
          + " numOpDelete = " + numOpDelete + " numOpRename = " + numOpRename 
          + " numOpSetRepl = " + numOpSetRepl + " numOpMkDir = " + numOpMkDir
          + " numOpSetPerm = " + numOpSetPerm 
          + " numOpSetOwner = " + numOpSetOwner
          + " numOpSetGenStamp = " + numOpSetGenStamp 
          + " numOpOther = " + numOpOther);
    }

    if (logVersion != FSConstants.LAYOUT_VERSION) // other version
      numEdits++; // save this image asap
    return numEdits;
  }

  // a place holder for reading a long
  private static final LongWritable longWritable = new LongWritable();

  /** Read an integer from an input stream */
  private static long readLongWritable(DataInputStream in) throws IOException {
    synchronized (longWritable) {
      longWritable.readFields(in);
      return longWritable.get();
    }
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
  synchronized void logEdit(byte op, Writable ... writables) {
    assert this.getNumEditStreams() > 0 : "no editlog streams";
    long start = FSNamesystem.now();
    for (int idx = 0; idx < editStreams.size(); idx++) {
      EditLogOutputStream eStream = editStreams.get(idx);
      try {
        DataOutputStream od = eStream.getOutputStream();
        od.write(op);
        for(Writable w : writables) {
          w.write(od);
        }
      } catch (IOException ie) {
        processIOError(idx);         
      }
    }
    // get a new transactionId
    txid++;

    //
    // record the transactionId when new data was written to the edits log
    //
    TransactionId id = myTransactionId.get();
    id.txid = txid;

    // update statistics
    long end = FSNamesystem.now();
    numTransactions++;
    totalTimeTransactions += (end-start);
    metrics.transactions.inc((end-start));
  }

  //
  // Sync all modifications done by this thread.
  //
  void logSync() {
    ArrayList<EditLogOutputStream> errorStreams = null;
    long syncStart = 0;

    // Fetch the transactionId of this thread. 
    TransactionId id = myTransactionId.get();
    long mytxid = id.txid;

    synchronized (this) {
      assert this.getNumEditStreams() > 0 : "no editlog streams";
      printStatistics(false);

      // if somebody is already syncing, then wait
      while (mytxid > synctxid && isSyncRunning) {
        try {
          wait(1000);
        } catch (InterruptedException ie) { 
        }
      }

      //
      // If this transaction was already flushed, then nothing to do
      //
      if (mytxid <= synctxid) {
        return;
      }
   
      // now, this thread will do the sync
      syncStart = txid;
      isSyncRunning = true;   

      // swap buffers
      for (int idx = 0; idx < editStreams.size(); idx++) {
        EditLogOutputStream eStream = editStreams.get(idx);
        eStream.swap();
      }
    }

    // do the sync
    long start = FSNamesystem.now();
    for (int idx = 0; idx < editStreams.size(); idx++) {
      EditLogOutputStream eStream = editStreams.get(idx);
      try {
        eStream.flushAndSyncOld();
      } catch (IOException ie) {
        //
        // remember the streams that encountered an error.
        //
        if (errorStreams == null) {
          errorStreams = new ArrayList<EditLogOutputStream>(1);
        }
        errorStreams.add(eStream);
        FSNamesystem.LOG.error("Unable to sync edit log. " +
                               "Fatal Error.");
      }
    }
    long elapsed = FSNamesystem.now() - start;

    synchronized (this) {
       processIOError(errorStreams);
       synctxid = syncStart;
       isSyncRunning = false;
       this.notifyAll();
    }

    metrics.syncs.inc(elapsed);
  }

  //
  // print statistics every 1 minute.
  //
  private void printStatistics(boolean force) {
    long now = FSNamesystem.now();
    if (lastPrintTime + 60000 > now && !force) {
      return;
    }
    if (editStreams == null) {
      return;
    }
    lastPrintTime = now;
    StringBuffer buf = new StringBuffer();

    buf.append("Number of transactions: " + numTransactions +
               " Total time for transactions(ms): " + 
               totalTimeTransactions);
    buf.append(" Number of syncs: " + editStreams.get(0).getNumSync());
    buf.append(" SyncTimes(ms): ");
    for (int idx = 0; idx < editStreams.size(); idx++) {
      EditLogOutputStream eStream = editStreams.get(idx);
      buf.append(eStream.getTotalSyncTime());
      buf.append(" ");
    }
    FSNamesystem.LOG.info(buf);
  }

  /** 
   * Add open lease record to edit log. 
   * Records the block locations of the last block.
   */
  void logOpenFile(String path, INodeFileUnderConstruction newNode) 
                   throws IOException {

    UTF8 nameReplicationPair[] = new UTF8[] { 
      new UTF8(path), 
      FSEditLog.toLogReplication(newNode.getReplication()),
      FSEditLog.toLogLong(newNode.getModificationTime()),
      FSEditLog.toLogLong(newNode.getPreferredBlockSize())};
    logEdit(OP_ADD,
            new ArrayWritable(UTF8.class, nameReplicationPair), 
            new ArrayWritable(Block.class, newNode.getBlocks()),
            newNode.getPermissionStatus(),
            new UTF8(newNode.getClientName()),
            new UTF8(newNode.getClientMachine()));
  }

  /** 
   * Add close lease record to edit log.
   */
  void logCloseFile(String path, INodeFile newNode) {
    UTF8 nameReplicationPair[] = new UTF8[] {
      new UTF8(path),
      FSEditLog.toLogReplication(newNode.getReplication()),
      FSEditLog.toLogLong(newNode.getModificationTime()),
      FSEditLog.toLogLong(newNode.getPreferredBlockSize())};
    logEdit(OP_CLOSE,
            new ArrayWritable(UTF8.class, nameReplicationPair),
            new ArrayWritable(Block.class, newNode.getBlocks()),
            newNode.getPermissionStatus());
  }
  
  /** 
   * Add create directory record to edit log
   */
  void logMkDir(String path, INode newNode) {
    UTF8 info[] = new UTF8[] {
      new UTF8(path),
      FSEditLog.toLogLong(newNode.getModificationTime())
    };
    logEdit(OP_MKDIR, new ArrayWritable(UTF8.class, info),
        newNode.getPermissionStatus());
  }
  
  /** 
   * Add rename record to edit log
   * TODO: use String parameters until just before writing to disk
   */
  void logRename(String src, String dst, long timestamp) {
    UTF8 info[] = new UTF8[] { 
      new UTF8(src),
      new UTF8(dst),
      FSEditLog.toLogLong(timestamp)};
    logEdit(OP_RENAME, new ArrayWritable(UTF8.class, info));
  }
  
  /** 
   * Add set replication record to edit log
   */
  void logSetReplication(String src, short replication) {
    logEdit(OP_SET_REPLICATION, 
            new UTF8(src), 
            FSEditLog.toLogReplication(replication));
  }
  
  /** Add set quota record to edit log
   * 
   * @param src the string representation of the path to a directory
   * @param quota the directory size limit
   */
  void logSetQuota(String src, long quota) {
    logEdit(OP_SET_QUOTA, new UTF8(src), new LongWritable(quota));
  }

  /** Add clear quota record to edit log
   * 
   * @param src the string representation of the path to a directory
   */
  void logClearQuota(String src) {
    logEdit(OP_CLEAR_QUOTA, new UTF8(src));
  }
  
  /**  Add set permissions record to edit log */
  void logSetPermissions(String src, FsPermission permissions) {
    logEdit(OP_SET_PERMISSIONS, new UTF8(src), permissions);
  }

  /**  Add set owner record to edit log */
  void logSetOwner(String src, String username, String groupname) {
    UTF8 u = new UTF8(username == null? "": username);
    UTF8 g = new UTF8(groupname == null? "": groupname);
    logEdit(OP_SET_OWNER, new UTF8(src), u, g);
  }

  /** 
   * Add delete file record to edit log
   */
  void logDelete(String src, long timestamp) {
    UTF8 info[] = new UTF8[] { 
      new UTF8(src),
      FSEditLog.toLogLong(timestamp)};
    logEdit(OP_DELETE, new ArrayWritable(UTF8.class, info));
  }

  /** 
   * Add generation stamp record to edit log
   */
  void logGenerationStamp(long genstamp) {
    logEdit(OP_SET_GENSTAMP, new LongWritable(genstamp));
  }
  
  static private UTF8 toLogReplication(short replication) {
    return new UTF8(Short.toString(replication));
  }
  
  static private UTF8 toLogLong(long timestamp) {
    return new UTF8(Long.toString(timestamp));
  }

  /**
   * Return the size of the current EditLog
   */
  synchronized long getEditLogSize() throws IOException {
    assert(getNumStorageDirs() == editStreams.size());
    long size = 0;
    for (int idx = 0; idx < getNumStorageDirs(); idx++) {
      EditLogOutputStream eStream = editStreams.get(idx);
      assert(size == 0 || 
             size == getEditFile(idx).length() + eStream.getBufSize());
      size = getEditFile(idx).length() + eStream.getBufSize();
    }
    return size;
  }
 
  /**
   * Closes the current edit log and opens edits.new. 
   * Returns the lastModified time of the edits log.
   */
  synchronized void rollEditLog() throws IOException {
    //
    // If edits.new already exists in some directory, verify it
    // exists in all directories.
    //
    if (existsNew()) {
      for (int idx = 0; idx < getNumStorageDirs(); idx++) {
        if (!getEditNewFile(idx).exists()) { 
          throw new IOException("Inconsistent existance of edits.new " +
                                getEditNewFile(idx));
        }
      }
      return; // nothing to do, edits.new exists!
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
  synchronized void purgeEditLog() throws IOException {
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
          fsimage.processIOError(idx); 
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
  synchronized File getFsEditName() throws IOException {
    return getEditFile(0);
  }

  /**
   * Returns the timestamp of the edit log
   */
  synchronized long getFsEditTime() {
    return getEditFile(0).lastModified();
  }

  // sets the initial capacity of the flush buffer.
  static void setBufferCapacity(int size) {
    sizeFlushBuffer = size;
  }

  /**
   * A class to read in blocks stored in the old format. The only two
   * fields in the block were blockid and length.
   */
  static class BlockTwo implements Writable {
    long blkid;
    long len;

    static {                                      // register a ctor
      WritableFactories.setFactory
        (BlockTwo.class,
         new WritableFactory() {
           public Writable newInstance() { return new BlockTwo(); }
         });
    }


    BlockTwo() {
      blkid = 0;
      len = 0;
    }
    /////////////////////////////////////
    // Writable
    /////////////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeLong(blkid);
      out.writeLong(len);
    }

    public void readFields(DataInput in) throws IOException {
      this.blkid = in.readLong();
      this.len = in.readLong();
    }
  }

  /** This method is defined for compatibility reason. */
  static private DatanodeDescriptor[] readDatanodeDescriptorArray(DataInput in
      ) throws IOException {
    DatanodeDescriptor[] locations = new DatanodeDescriptor[in.readInt()];
    for (int i = 0; i < locations.length; i++) {
      locations[i] = new DatanodeDescriptor();
      locations[i].readFieldsFromFSEditLog(in);
    }
    return locations;
  }

  static private short readShort(DataInputStream in) throws IOException {
    return Short.parseShort(FSImage.readString(in));
  }

  static private long readLong(DataInputStream in) throws IOException {
    return Long.parseLong(FSImage.readString(in));
  }

  static private Block[] readBlocks(DataInputStream in) throws IOException {
    int numBlocks = in.readInt();
    Block[] blocks = new Block[numBlocks];
    for (int i = 0; i < numBlocks; i++) {
      blocks[i] = new Block();
      blocks[i].readFields(in);
    }
    return blocks;
  }
}
