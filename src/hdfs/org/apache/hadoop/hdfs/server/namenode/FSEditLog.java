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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeInstrumentation;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.security.token.delegation.DelegationKey;

/**
 * FSEditLog maintains a log of the namespace modifications.
 * 
 */
public class FSEditLog {
  public static final Log LOG = LogFactory.getLog(FSEditLog.class);

  static final byte OP_INVALID = -1;
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
  /* The following two are not used any more. Should be removed once
   * LAST_UPGRADABLE_LAYOUT_VERSION is -17 or newer. */
  private static final byte OP_SET_NS_QUOTA = 11; // set namespace quota
  private static final byte OP_CLEAR_NS_QUOTA = 12; // clear namespace quota
  private static final byte OP_TIMES = 13; // sets mod & access time on a file
  private static final byte OP_SET_QUOTA = 14; // sets name and disk quotas.
  private static final byte OP_CONCAT_DELETE = 16; // concat files.
  private static final byte OP_GET_DELEGATION_TOKEN = 18; //new delegation token
  private static final byte OP_RENEW_DELEGATION_TOKEN = 19; //renew delegation token
  private static final byte OP_CANCEL_DELEGATION_TOKEN = 20; //cancel delegation token
  private static final byte OP_UPDATE_MASTER_KEY = 21; //update master key

  private static int sizeFlushBuffer = 512*1024;
  /** Preallocation length in bytes for writing edit log. */
  static final int MIN_PREALLOCATION_LENGTH = 1024 * 1024;
  /** The limit of the length in bytes for each edit log transaction. */
  private static int TRANSACTION_LENGTH_LIMIT = Integer.MAX_VALUE;

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
  private long numTransactionsBatchedInSync;
  private long totalTimeTransactions;  // total time for all transactions
  private NameNodeInstrumentation metrics;

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

  /**
   * An implementation of the abstract class {@link EditLogOutputStream},
   * which stores edits in a local file.
   */
  static class EditLogFileOutputStream extends EditLogOutputStream {
    /** Preallocation buffer, padded with OP_INVALID */
    private static final ByteBuffer PREALLOCATION_BUFFER
        = ByteBuffer.allocateDirect(MIN_PREALLOCATION_LENGTH);
    static {
      PREALLOCATION_BUFFER.position(0).limit(MIN_PREALLOCATION_LENGTH);
      for(int i = 0; i < PREALLOCATION_BUFFER.capacity(); i++) {
        PREALLOCATION_BUFFER.put(OP_INVALID);
      }
    }

    private File file;
    private FileOutputStream fp;    // file stream for storing edit logs 
    private FileChannel fc;         // channel of the file stream for sync
    private DataOutputBuffer bufCurrent;  // current buffer for writing
    private DataOutputBuffer bufReady;    // buffer ready for flushing

    EditLogFileOutputStream(File name) throws IOException {
      super();
      file = name;
      bufCurrent = new DataOutputBuffer(sizeFlushBuffer);
      bufReady = new DataOutputBuffer(sizeFlushBuffer);
      RandomAccessFile rp = new RandomAccessFile(name, "rw");
      fp = new FileOutputStream(rp.getFD()); // open for append
      fc = rp.getChannel();
      fc.position(fc.size());
    }

    @Override
    String getName() {
      return file.getPath();
    }

    /** {@inheritDoc} */
    @Override
    public void write(int b) throws IOException {
      bufCurrent.write(b);
    }

    /** {@inheritDoc} */
    @Override
    void write(byte op, Writable ... writables) throws IOException {
      write(op);
      for(Writable w : writables) {
        w.write(bufCurrent);
      }
    }

    /**
     * Create empty edits logs file.
     */
    @Override
    void create() throws IOException {
      fc.truncate(0);
      fc.position(0);
      bufCurrent.writeInt(FSConstants.LAYOUT_VERSION);
      setReadyToFlush();
      flush();
    }

    @Override
    public void close() throws IOException {
      LOG.info("closing edit log: position=" + fc.position() + ", editlog=" + getName());

      // close should have been called after all pending transactions 
      // have been flushed & synced.
      int bufSize = bufCurrent.size();
      if (bufSize != 0) {
        throw new IOException("FSEditStream has " + bufSize +
           " bytes still to be flushed and cannot be closed.");
      } 
      bufCurrent.close();
      bufReady.close();

      // remove any preallocated padding bytes from the transaction log.
      fc.truncate(fc.position());
      fp.close();
      
      bufCurrent = bufReady = null;

      LOG.info("close success: truncate to " + file.length() + ", editlog=" + getName());
    }

    /**
     * All data that has been written to the stream so far will be flushed.
     * New data can be still written to the stream while flushing is performed.
     */
    @Override
    void setReadyToFlush() throws IOException {
      assert bufReady.size() == 0 : "previous data is not flushed yet";
      DataOutputBuffer tmp = bufReady;
      bufReady = bufCurrent;
      bufCurrent = tmp;
    }

    /**
     * Flush ready buffer to persistent store.
     * currentBuffer is not flushed as it accumulates new log records
     * while readyBuffer will be flushed and synced.
     */
    @Override
    protected void flushAndSync() throws IOException {
      preallocate();            // preallocate file if necessary
      bufReady.writeTo(fp);     // write data to file
      bufReady.reset();         // erase all data in the buffer
      fc.force(false);          // metadata updates not needed because of preallocation
    }

    /**
     * Return the size of the current edit log including buffered data.
     */
    @Override
    long length() throws IOException {
      // file size + size of both buffers
      return fc.size() + bufReady.size() + bufCurrent.size();
    }

    // allocate a big chunk of data
    private void preallocate() throws IOException {
      long size = fc.size();
      int bufSize = bufReady.getLength();
      long need = bufSize - (size - fc.position());
      if (need <= 0) {
        return;
      }
      long oldSize = size;
      long total = 0;
      long fillCapacity = PREALLOCATION_BUFFER.capacity();
      while (need > 0) {
        PREALLOCATION_BUFFER.position(0);
        do {
          size += fc.write(PREALLOCATION_BUFFER, size);
        } while (PREALLOCATION_BUFFER.remaining() > 0);
        need -= fillCapacity;
        total += fillCapacity;
      }
      if(FSNamesystem.LOG.isDebugEnabled()) {
        FSNamesystem.LOG.debug("Preallocated " + total + " bytes at the end of " +
            "the edit log (offset " + oldSize + ")");
      }
    }
    
    /**
     * Returns the file associated with this stream
     */
    File getFile() {
      return file;
    }
  }

  static class EditLogFileInputStream extends EditLogInputStream {
    private File file;
    private FileInputStream fStream;

    EditLogFileInputStream(File name) throws IOException {
      file = name;
      fStream = new FileInputStream(name);
    }

    @Override
    String getName() {
      return file.getPath();
    }

    @Override
    public int available() throws IOException {
      return fStream.available();
    }

    @Override
    public int read() throws IOException {
      return fStream.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return fStream.read(b, off, len);
    }

    @Override
    public void close() throws IOException {
      fStream.close();
    }

    @Override
    long length() throws IOException {
      // file size + size of both buffers
      return file.length();
    }
  }

  FSEditLog(FSImage image) {
    fsimage = image;
    isSyncRunning = false;
    metrics = NameNode.getNameNodeMetrics();
    lastPrintTime = FSNamesystem.now();
  }
  
  private File getEditFile(StorageDirectory sd) {
    return fsimage.getEditFile(sd);
  }
  
  private File getEditNewFile(StorageDirectory sd) {
    return fsimage.getEditNewFile(sd);
  }
  
  private int getNumStorageDirs() {
   int numStorageDirs = 0;
   Iterator<StorageDirectory> it = fsimage.dirIterator(NameNodeDirType.EDITS);
   while (it.hasNext()) {
     numStorageDirs++;
     it.next();
   }
   return numStorageDirs;
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
  public synchronized void open() throws IOException {
    numTransactions = totalTimeTransactions = numTransactionsBatchedInSync = 0;
    if (editStreams == null) {
      editStreams = new ArrayList<EditLogOutputStream>();
    }
    Iterator<StorageDirectory> it = fsimage.dirIterator(NameNodeDirType.EDITS); 
    while (it.hasNext()) {
      StorageDirectory sd = it.next();
      File eFile = getEditFile(sd);
      try {
        EditLogOutputStream eStream = new EditLogFileOutputStream(eFile);
        editStreams.add(eStream);
      } catch (IOException ioe) {
        fsimage.updateRemovedDirs(sd, ioe);
        it.remove();
      }
    }
    exitIfNoStreams();
  }

  public synchronized void createEditLogFile(File name) throws IOException {
    EditLogOutputStream eStream = new EditLogFileOutputStream(name);
    eStream.create();
    eStream.close();
  }

  /**
   * Shutdown the file store.
   */
  public synchronized void close() throws IOException {
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
    numTransactions = totalTimeTransactions = numTransactionsBatchedInSync = 0;

    for (int idx = 0; idx < editStreams.size(); idx++) {
      EditLogOutputStream eStream = editStreams.get(idx);
      try {
        eStream.setReadyToFlush();
        eStream.flush();
        eStream.close();
      } catch (IOException ioe) {
        removeEditsAndStorageDir(idx);
        idx--;
      }
    }
    editStreams.clear();
  }

  void fatalExit(String msg) {
    LOG.fatal(msg, new Exception(msg));
    Runtime.getRuntime().exit(-1);
  }

  /**
   * Exit the NN process if the edit streams have not yet been
   * initialized, eg we failed while opening.
   */
  private void exitIfStreamsNotSet() {
    if (editStreams == null) {
      fatalExit("Edit streams not yet initialized");
    }
  }

  /**
   * Exit the NN process if there are no edit streams to log to.
   */
  void exitIfNoStreams() {
    if (editStreams == null || editStreams.isEmpty()) {
      fatalExit("No edit streams are accessible");
    }
  }

  /**
   * @return the storage directory for the given edit stream. 
   */
  private File getStorageDirForStream(int idx) {
    File editsFile =
      ((EditLogFileOutputStream)editStreams.get(idx)).getFile();
    // Namedir is the parent of current which is the parent of edits
    return editsFile.getParentFile().getParentFile();
  }

  /**
   * Remove the given edits stream and its containing storage dir.
   */
  synchronized void removeEditsAndStorageDir(int idx) {
    exitIfStreamsNotSet();

    assert idx < getNumStorageDirs();
    assert getNumStorageDirs() == editStreams.size();
    
    File dir = getStorageDirForStream(idx);
    editStreams.remove(idx);
    exitIfNoStreams();
    fsimage.removeStorageDir(dir);
  }

  /**
   * Remove all edits streams for the given storage directory.
   */
  synchronized void removeEditsForStorageDir(StorageDirectory sd) {
    exitIfStreamsNotSet();

    if (!sd.getStorageDirType().isOfType(NameNodeDirType.EDITS)) {
      return;
    }
    for (int idx = 0; idx < editStreams.size(); idx++) {
      File parentDir = getStorageDirForStream(idx);
      if (parentDir.getAbsolutePath().equals(
            sd.getRoot().getAbsolutePath())) {
        editStreams.remove(idx);
        idx--;
      }
    }
    exitIfNoStreams();
  }
  
  /**
   * Remove each of the given edits streams and their corresponding
   * storage directories.
   */
  private void removeEditsStreamsAndStorageDirs(
      ArrayList<EditLogOutputStream> errorStreams) {
    if (errorStreams == null) {
      return;
    }
    for (EditLogOutputStream errorStream : errorStreams) {
      int idx = editStreams.indexOf(errorStream);
      if (-1 == idx) {
        fatalExit("Unable to find edits stream with IO error");
      }
      removeEditsAndStorageDir(idx);
    }
    fsimage.incrementCheckpointTime();
  }

  /**
   * check if ANY edits.new log exists
   */
  boolean existsNew() throws IOException {
    Iterator<StorageDirectory> it = fsimage.dirIterator(NameNodeDirType.EDITS);
    while (it.hasNext()) {
      if (getEditNewFile(it.next()).exists()) { 
        return true;
      }
    }
    return false;
  }

  /**
   * The end of a edit log should contain padding of either 0x00 or OP_INVALID.
   * If it contains other bytes, the edit log may be corrupted.
   * It is important to perform the check; otherwise, a stray OP_INVALID byte 
   * could be misinterpreted as an end-of-log, and lead to silent data loss.
   */
  private static void checkEndOfLog(final EditLogInputStream edits,
      final DataInputStream in,
      final PositionTrackingInputStream pin,
      final int tolerationLength) throws IOException {
    if (tolerationLength < 0) {
      //the checking is disabled.
      return;
    }
    LOG.info("Start checking end of edit log (" + edits.getName() + ") ...");

    in.mark(0); //clear the mark
    final long readLength = pin.getPos();

    long firstPadPos = -1; //first padded byte position 
    byte pad = 0;          //padded value; must be either 0 or OP_INVALID
    
    final byte[] bytes = new byte[4096];
    for(int n; (n = in.read(bytes)) != -1; ) {
      for(int i = 0; i < n; i++) {
        final byte b = bytes[i];

        if (firstPadPos != -1 && b != pad) {
          //the byte is different from the first padded byte, reset firstPos
          firstPadPos = -1;

          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("reset: bytes[%d]=0x%X, pad=0x%02X", i, b, pad));
          }
        }

        if (firstPadPos == -1) {
          if (b == 0 || b == OP_INVALID) {
            //found the first padded byte
            firstPadPos = pin.getPos() - n + i;
            pad = b;
            
            if (LOG.isDebugEnabled()) {
              LOG.debug(String.format("found: bytes[%d]=0x%02X=pad, firstPadPos=%d",
                  i, b, firstPadPos));
            }
          }
        } 
      }
    }
    
    final long corruptionLength;
    final long padLength;
    if (firstPadPos == -1) { //padding not found
      corruptionLength = edits.length() - readLength;
      padLength = 0;
    } else {
      corruptionLength = firstPadPos - readLength;
      padLength = edits.length() - firstPadPos;
    }

    LOG.info("Checked the bytes after the end of edit log (" + edits.getName() + "):");
    LOG.info("  Padding position  = " + firstPadPos + " (-1 means padding not found)");
    LOG.info("  Edit log length   = " + edits.length());
    LOG.info("  Read length       = " + readLength);
    LOG.info("  Corruption length = " + corruptionLength);
    LOG.info("  Toleration length = " + tolerationLength
        + " (= " + DFSConfigKeys.DFS_NAMENODE_EDITS_TOLERATION_LENGTH_KEY + ")");
    LOG.info(String.format(
        "Summary: |---------- Read=%d ----------|-- Corrupt=%d --|-- Pad=%d --|",
        readLength, corruptionLength, padLength));

    if (pin.getPos() != edits.length()) {
      throw new IOException("Edit log length mismatched: edits.length() = "
          + edits.length() + " != input steam position = " + pin.getPos());
    }
    if (corruptionLength > 0) {
      final String err = "Edit log corruption detected: corruption length = " + corruptionLength;
      if (corruptionLength <= tolerationLength) {
        LOG.warn(err + " <= toleration length = " + tolerationLength
            + "; the corruption is tolerable.");
      } else {
        throw new IOException(err + " > toleration length = " + tolerationLength
            + "; the corruption is intolerable.");
      }
    }
    return;
  }
  
  /**
   * Load an edit log, and apply the changes to the in-memory structure
   * This is where we apply edits that we've been writing to disk all
   * along.
   */
  static int loadFSEdits(EditLogInputStream edits, int tolerationLength,
      MetaRecoveryContext recovery) throws IOException {
    FSNamesystem fsNamesys = FSNamesystem.getFSNamesystem();
    FSDirectory fsDir = fsNamesys.dir;
    int numEdits = 0;
    int logVersion = 0;
    String clientName = null;
    String clientMachine = null;
    String path = null;
    int numOpAdd = 0, numOpClose = 0, numOpDelete = 0,
        numOpRename = 0, numOpSetRepl = 0, numOpMkDir = 0,
        numOpSetPerm = 0, numOpSetOwner = 0, numOpSetGenStamp = 0,
        numOpTimes = 0, numOpGetDelegationToken = 0,
        numOpRenewDelegationToken = 0, numOpCancelDelegationToken = 0,
        numOpUpdateMasterKey = 0, numOpOther = 0,
        numOpConcatDelete = 0;
    long highestGenStamp = -1;
    long startTime = FSNamesystem.now();

    LOG.info("Start loading edits file " + edits.getName());
    //
    // Keep track of the file offsets of the last several opcodes.
    // This is handy when manually recovering corrupted edits files.
    PositionTrackingInputStream tracker = 
      new PositionTrackingInputStream(new BufferedInputStream(edits));
    long recentOpcodeOffsets[] = new long[4];
    Arrays.fill(recentOpcodeOffsets, -1);

    final boolean isTolerationEnabled = tolerationLength >= 0;
    DataInputStream in = new DataInputStream(tracker);
    Byte opcode = null;
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
        if (isTolerationEnabled) {
          //mark position could be reset in case of exceptions
          in.mark(TRANSACTION_LENGTH_LIMIT); 
        }

        long timestamp = 0;
        long mtime = 0;
        long atime = 0;
        long blockSize = 0;
        opcode = null;
        try {
          opcode = in.readByte();
          if (opcode == OP_INVALID) {
            LOG.info("Invalid opcode, reached end of edit log " +
                       "Number of transactions found: " + numEdits + ".  " +
                       "Bytes read: " + tracker.getPos());
            break; // no more transactions
          }
        } catch (EOFException e) {
          LOG.info("EOF of " + edits.getName() + ", reached end of edit log "
              + "Number of transactions found: " + numEdits + ".  "
              + "Bytes read: " + tracker.getPos());
          break; // no more transactions
        }
        recentOpcodeOffsets[numEdits % recentOpcodeOffsets.length] =
          tracker.getPos();
        numEdits++;
        switch (opcode) {
        case OP_ADD:
        case OP_CLOSE: {
          // versions > 0 support per file replication
          // get name and replication
          int length = in.readInt();
          if (-7 == logVersion && length != 3||
              -17 < logVersion && logVersion < -7 && length != 4 ||
              logVersion <= -17 && length != 5) {
              throw new IOException("Incorrect data format."  +
                                    " logVersion is " + logVersion +
                                    " but writables.length is " +
                                    length + ". ");
          }
          path = FSImage.readString(in);
          short replication = adjustReplication(readShort(in));
          mtime = readLong(in);
          if (logVersion <= -17) {
            atime = readLong(in);
          }
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
          if (LOG.isDebugEnabled()) {
            LOG.debug(opcode + ": " + path + 
                                   " numblocks : " + blocks.length +
                                   " clientHolder " +  clientName +
                                   " clientMachine " + clientMachine);
          }

          fsDir.unprotectedDelete(path, mtime);

          // add to the file tree
          INodeFile node = (INodeFile)fsDir.unprotectedAddFile(
                                                    path, permissions,
                                                    blocks, replication, 
                                                    mtime, atime, blockSize);
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
        case OP_CONCAT_DELETE: {
          if (logVersion > -22) {
            throw new IOException("Unexpected opcode " + opcode
                + " for version " + logVersion);
          }
          numOpConcatDelete++;
          int length = in.readInt();
          if (length < 3) { // trg, srcs.., timestam
            throw new IOException("Incorrect data format. " 
                                  + "ConcatDelete operation.");
          }
          String trg = FSImage.readString(in);
          int srcSize = length - 1 - 1; //trg and timestamp
          String [] srcs = new String [srcSize];
          for(int i=0; i<srcSize;i++) {
            srcs[i]= FSImage.readString(in);
          }
          timestamp = readLong(in);
          fsDir.unprotectedConcat(trg, srcs, timestamp);
          break;
        }
        case OP_RENAME: {
          numOpRename++;
          int length = in.readInt();
          if (length != 3) {
            throw new IOException("Incorrect data format. " 
                                  + "Rename operation.");
          }
          String s = FSImage.readString(in);
          String d = FSImage.readString(in);
          timestamp = readLong(in);
          HdfsFileStatus dinfo = fsDir.getFileInfo(d);
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
          fsDir.unprotectedDelete(path, timestamp);
          break;
        }
        case OP_MKDIR: {
          numOpMkDir++;
          PermissionStatus permissions = fsNamesys.getUpgradePermission();
          int length = in.readInt();
          if (-17 < logVersion && length != 2 ||
              logVersion <= -17 && length != 3) {
            throw new IOException("Incorrect data format. " 
                                  + "Mkdir operation.");
          }
          path = FSImage.readString(in);
          timestamp = readLong(in);

          // The disk format stores atimes for directories as well.
          // However, currently this is not being updated/used because of
          // performance reasons.
          if (logVersion <= -17) {
            atime = readLong(in);
          }

          if (logVersion <= -11) {
            permissions = PermissionStatus.read(in);
          }
          fsDir.unprotectedMkdir(path, permissions, timestamp);
          break;
        }
        case OP_SET_GENSTAMP: {
          numOpSetGenStamp++;
          long lw = in.readLong();
          if ((highestGenStamp != -1) && (highestGenStamp + 1 != lw)) {
            throw new IOException("OP_SET_GENSTAMP tried to set a genstamp of " + lw + 
              " but the previous highest genstamp was " + highestGenStamp);
          }
          highestGenStamp = lw;
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
        case OP_SET_NS_QUOTA: {
          if (logVersion > -16) {
            throw new IOException("Unexpected opcode " + opcode
                + " for version " + logVersion);
          }
          fsDir.unprotectedSetQuota(FSImage.readString(in), 
                                    readLongWritable(in), 
                                    FSConstants.QUOTA_DONT_SET);
          break;
        }
        case OP_CLEAR_NS_QUOTA: {
          if (logVersion > -16) {
            throw new IOException("Unexpected opcode " + opcode
                + " for version " + logVersion);
          }
          fsDir.unprotectedSetQuota(FSImage.readString(in),
                                    FSConstants.QUOTA_RESET,
                                    FSConstants.QUOTA_DONT_SET);
          break;
        }

        case OP_SET_QUOTA:
          fsDir.unprotectedSetQuota(FSImage.readString(in),
                                    readLongWritable(in),
                                    readLongWritable(in));
                                      
          break;

        case OP_TIMES: {
          numOpTimes++;
          int length = in.readInt();
          if (length != 3) {
            throw new IOException("Incorrect data format. " 
                                  + "times operation.");
          }
          path = FSImage.readString(in);
          mtime = readLong(in);
          atime = readLong(in);
          fsDir.unprotectedSetTimes(path, mtime, atime, true);
          break;
        }
        case OP_GET_DELEGATION_TOKEN: {
          if (logVersion > -19) {
            throw new IOException("Unexpected opcode " + opcode
                + " for version " + logVersion);
          }
          numOpGetDelegationToken++;
          DelegationTokenIdentifier delegationTokenId = 
              new DelegationTokenIdentifier();
          delegationTokenId.readFields(in);
          long expiryTime = readLong(in);
          fsNamesys.getDelegationTokenSecretManager()
              .addPersistedDelegationToken(delegationTokenId, expiryTime);
          break;
        }
        case OP_RENEW_DELEGATION_TOKEN: {
          if (logVersion > -19) {
            throw new IOException("Unexpected opcode " + opcode
                + " for version " + logVersion);
          }
          numOpRenewDelegationToken++;
          DelegationTokenIdentifier delegationTokenId = 
              new DelegationTokenIdentifier();
          delegationTokenId.readFields(in);
          long expiryTime = readLong(in);
          fsNamesys.getDelegationTokenSecretManager()
              .updatePersistedTokenRenewal(delegationTokenId, expiryTime);
          break;
        }
        case OP_CANCEL_DELEGATION_TOKEN: {
          if (logVersion > -19) {
            throw new IOException("Unexpected opcode " + opcode
                + " for version " + logVersion);
          }
          numOpCancelDelegationToken++;
          DelegationTokenIdentifier delegationTokenId = 
              new DelegationTokenIdentifier();
          delegationTokenId.readFields(in);
          fsNamesys.getDelegationTokenSecretManager()
              .updatePersistedTokenCancellation(delegationTokenId);
          break;
        }
        case OP_UPDATE_MASTER_KEY: {
          if (logVersion > -19) {
            throw new IOException("Unexpected opcode " + opcode
                + " for version " + logVersion);
          }
          numOpUpdateMasterKey++;
          DelegationKey delegationKey = new DelegationKey();
          delegationKey.readFields(in);
          fsNamesys.getDelegationTokenSecretManager().updatePersistedMasterKey(
              delegationKey);
          break;
        }
        default: {
          throw new IOException("Never seen opcode " + opcode);
        }
        }
      }
    } catch (Throwable t) {
      String msg = "Failed to parse edit log (" + edits.getName()
          + ") at position " + tracker.getPos()
          + ", edit log length is " + edits.length()
          + ", opcode=" + opcode
          + ", isTolerationEnabled=" + isTolerationEnabled;

      // Catch Throwable because in the case of a truly corrupt edits log, any
      // sort of error might be thrown (NumberFormat, NullPointer, EOF, etc.)
      if (Storage.is203LayoutVersion(logVersion) &&
          logVersion != FSConstants.LAYOUT_VERSION) {
        // Failed to load 0.20.203 version edits during upgrade. This version has
        // conflicting opcodes with the later releases. The editlog must be 
        // emptied by restarting the namenode, before proceeding with the upgrade.
        msg += ": During upgrade, failed to load the editlog version " + 
          logVersion + " from release 0.20.203. Please go back to the old " + 
          " release and restart the namenode. This empties the editlog " +
          " and saves the namespace. Resume the upgrade after this step.";
        throw new IOException(msg, t);
      }
      if (recentOpcodeOffsets[0] != -1) {
        Arrays.sort(recentOpcodeOffsets);
        StringBuilder sb = new StringBuilder(", Recent opcode offsets=[")
            .append(recentOpcodeOffsets[0]);
        for (int i = 1; i < recentOpcodeOffsets.length; i++) {
          if (recentOpcodeOffsets[i] != -1) {
            sb.append(' ').append(recentOpcodeOffsets[i]);
          }
        }
        msg += sb.append("]");
      }

      LOG.warn(msg, t);
      if (isTolerationEnabled) {
        in.reset(); //reset to the beginning position of this transaction
      } else {
        //edit log toleration feature is disabled
        MetaRecoveryContext.editLogLoaderPrompt(msg, recovery);
      }
    } finally {
      try {
        checkEndOfLog(edits, in, tracker, tolerationLength);
      } finally {
        in.close();
      }
    }
    LOG.info("Edits file " + edits.getName() 
        + " of size " + edits.length() + " edits # " + numEdits 
        + " loaded in " + (FSNamesystem.now()-startTime)/1000 + " seconds.");

    if (LOG.isDebugEnabled()) {
      LOG.debug("numOpAdd = " + numOpAdd + " numOpClose = " + numOpClose 
          + " numOpDelete = " + numOpDelete + " numOpRename = " + numOpRename 
          + " numOpSetRepl = " + numOpSetRepl + " numOpMkDir = " + numOpMkDir
          + " numOpSetPerm = " + numOpSetPerm 
          + " numOpSetOwner = " + numOpSetOwner
          + " numOpSetGenStamp = " + numOpSetGenStamp 
          + " numOpTimes = " + numOpTimes
          + " numOpGetDelegationToken = " + numOpGetDelegationToken
          + " numOpRenewDelegationToken = " + numOpRenewDelegationToken
          + " numOpCancelDelegationToken = " + numOpCancelDelegationToken
          + " numOpUpdateMasterKey = " + numOpUpdateMasterKey
          + " numOpConcatDelete  = " + numOpConcatDelete
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
    if (getNumEditStreams() < 1) {
      throw new AssertionError("No edit streams to log to");
    }
    long start = FSNamesystem.now();
    for (int idx = 0; idx < editStreams.size(); idx++) {
      EditLogOutputStream eStream = editStreams.get(idx);
      try {
        eStream.write(op, writables);
      } catch (IOException ioe) {
        removeEditsAndStorageDir(idx);
        idx--; 
      }
    }
    exitIfNoStreams();
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
    if (metrics != null) // Metrics is non-null only when used inside name node
      metrics.addTransaction(end-start);
  }

  //
  // Sync all modifications done by this thread.
  //
  public void logSync() throws IOException {
    ArrayList<EditLogOutputStream> errorStreams = null;
    long syncStart = 0;

    // Fetch the transactionId of this thread. 
    long mytxid = myTransactionId.get().txid;

    ArrayList<EditLogOutputStream> streams = new ArrayList<EditLogOutputStream>();
    boolean sync = false;
    try {
      synchronized (this) {
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
          numTransactionsBatchedInSync++;
          if (metrics != null) // Metrics is non-null only when used inside name node
            metrics.incrTransactionsBatchedInSync();
          return;
        }

        // now, this thread will do the sync
        syncStart = txid;
        isSyncRunning = true;
        sync = true;

        // swap buffers
        exitIfNoStreams();
        for(EditLogOutputStream eStream : editStreams) {
          try {
            eStream.setReadyToFlush();
            streams.add(eStream);
          } catch (IOException ie) {
            LOG.error("Unable to get ready to flush.", ie);
            //
            // remember the streams that encountered an error.
            //
            if (errorStreams == null) {
              errorStreams = new ArrayList<EditLogOutputStream>(1);
            }
            errorStreams.add(eStream);
          }
        }
      }

      // do the sync
      long start = FSNamesystem.now();
      for (EditLogOutputStream eStream : streams) {
        try {
          eStream.flush();
        } catch (IOException ie) {
          LOG.error("Unable to sync edit log.", ie);
          //
          // remember the streams that encountered an error.
          //
          if (errorStreams == null) {
            errorStreams = new ArrayList<EditLogOutputStream>(1);
          }
          errorStreams.add(eStream);
        }
      }
      long elapsed = FSNamesystem.now() - start;
      removeEditsStreamsAndStorageDirs(errorStreams);
      exitIfNoStreams();

      if (metrics != null) // Metrics is non-null only when used inside name node
        metrics.addSync(elapsed);

    } finally {
      synchronized (this) {
        if(sync) {
          synctxid = syncStart;
          isSyncRunning = false;
        }
        this.notifyAll();
      }
    }
  }

  //
  // print statistics every 1 minute.
  //
  private void printStatistics(boolean force) {
    long now = FSNamesystem.now();
    if (lastPrintTime + 60000 > now && !force) {
      return;
    }
    if (editStreams == null || editStreams.size()==0) {
      return;
    }
    lastPrintTime = now;
    StringBuilder buf = new StringBuilder();
    buf.append("Number of transactions: ");
    buf.append(numTransactions);
    buf.append(" Total time for transactions(ms): ");
    buf.append(totalTimeTransactions);
    buf.append(" Number of transactions batched in Syncs: ");
    buf.append(numTransactionsBatchedInSync);
    buf.append(" Number of syncs: ");
    buf.append(editStreams.get(0).getNumSync());
    buf.append(" SyncTimes(ms): ");

    int numEditStreams = editStreams.size();
    for (int idx = 0; idx < numEditStreams; idx++) {
      EditLogOutputStream eStream = editStreams.get(idx);
      buf.append(eStream.getTotalSyncTime());
      buf.append(" ");
    }
    LOG.info(buf);
  }

  /** 
   * Add open lease record to edit log. 
   * Records the block locations of the last block.
   */
  public void logOpenFile(String path, INodeFileUnderConstruction newNode) 
                   throws IOException {

    UTF8 nameReplicationPair[] = new UTF8[] { 
      new UTF8(path), 
      FSEditLog.toLogReplication(newNode.getReplication()),
      FSEditLog.toLogLong(newNode.getModificationTime()),
      FSEditLog.toLogLong(newNode.getAccessTime()),
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
  public void logCloseFile(String path, INodeFile newNode) {
    UTF8 nameReplicationPair[] = new UTF8[] {
      new UTF8(path),
      FSEditLog.toLogReplication(newNode.getReplication()),
      FSEditLog.toLogLong(newNode.getModificationTime()),
      FSEditLog.toLogLong(newNode.getAccessTime()),
      FSEditLog.toLogLong(newNode.getPreferredBlockSize())};
    logEdit(OP_CLOSE,
            new ArrayWritable(UTF8.class, nameReplicationPair),
            new ArrayWritable(Block.class, newNode.getBlocks()),
            newNode.getPermissionStatus());
  }
  
  /** 
   * Add create directory record to edit log
   */
  public void logMkDir(String path, INode newNode) {
    UTF8 info[] = new UTF8[] {
      new UTF8(path),
      FSEditLog.toLogLong(newNode.getModificationTime()),
      FSEditLog.toLogLong(newNode.getAccessTime())
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
  
  /** Add set namespace quota record to edit log
   * 
   * @param src the string representation of the path to a directory
   * @param quota the directory size limit
   */
  void logSetQuota(String src, long nsQuota, long dsQuota) {
    logEdit(OP_SET_QUOTA, new UTF8(src), 
            new LongWritable(nsQuota), new LongWritable(dsQuota));
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
   * concat(trg,src..) log
   */
  void logConcat(String trg, String [] srcs, long timestamp) {
    int size = 1 + srcs.length + 1; // trg, srcs, timestamp
    UTF8 info[] = new UTF8[size];
    int idx = 0;
    info[idx++] = new UTF8(trg);
    for(int i=0; i<srcs.length; i++) {
      info[idx++] = new UTF8(srcs[i]);
    }
    info[idx] = FSEditLog.toLogLong(timestamp);
    logEdit(OP_CONCAT_DELETE, new ArrayWritable(UTF8.class, info));
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

  /** 
   * Add access time record to edit log
   */
  void logTimes(String src, long mtime, long atime) {
    UTF8 info[] = new UTF8[] { 
      new UTF8(src),
      FSEditLog.toLogLong(mtime),
      FSEditLog.toLogLong(atime)};
    logEdit(OP_TIMES, new ArrayWritable(UTF8.class, info));
  }

  /**
   * log delegation token to edit log
   * @param id DelegationTokenIdentifier
   * @param expiryTime of the token
   * @return
   */
  void logGetDelegationToken(DelegationTokenIdentifier id,
      long expiryTime) {
    logEdit(OP_GET_DELEGATION_TOKEN, id, FSEditLog.toLogLong(expiryTime));
  }

  void logRenewDelegationToken(DelegationTokenIdentifier id,
      long expiryTime) {
    logEdit(OP_RENEW_DELEGATION_TOKEN, id, FSEditLog.toLogLong(expiryTime));
  }

  void logCancelDelegationToken(DelegationTokenIdentifier id) {
    logEdit(OP_CANCEL_DELEGATION_TOKEN, id);
  }

  void logUpdateMasterKey(DelegationKey key) {
    logEdit(OP_UPDATE_MASTER_KEY, key);
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
    for (int idx = 0; idx < editStreams.size(); idx++) {
      long curSize = editStreams.get(idx).length();
      assert (size == 0 || size == curSize) : "All streams must be the same";
      size = curSize;
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
      Iterator<StorageDirectory> it =
        fsimage.dirIterator(NameNodeDirType.EDITS);
      StringBuilder b = new StringBuilder();
      while (it.hasNext()) {
        File editsNew = getEditNewFile(it.next());
        b.append("\n  ").append(editsNew);
        if (!editsNew.exists()) {
          throw new IOException(
              "Inconsistent existence of edits.new " + editsNew);
        }
      }
      LOG.warn("Cannot roll edit log," +
          " edits.new files already exists in all healthy directories:" + b);
      return;
    }
    close(); // close existing edit log

    // After edit streams are closed, healthy edits files should be identical,
    // and same to fsimage files
    fsimage.restoreStorageDirs();
    
    //
    // Open edits.new
    //
    Iterator<StorageDirectory> it = fsimage.dirIterator(NameNodeDirType.EDITS);
    LinkedList<StorageDirectory> toRemove = new LinkedList<StorageDirectory>();
    while (it.hasNext()) {
      StorageDirectory sd = it.next();
      try {
        EditLogFileOutputStream eStream = 
             new EditLogFileOutputStream(getEditNewFile(sd));
        eStream.create();
        editStreams.add(eStream);
      } catch (IOException ioe) {
        LOG.error("error retrying to reopen storage directory '" +
            sd.getRoot().getAbsolutePath() + "'", ioe);
        toRemove.add(sd);
        it.remove();
      }
    }

    // updateRemovedDirs will abort the NameNode if it removes the last
    // valid edit log directory.
    for (StorageDirectory sd : toRemove) {
      removeEditsForStorageDir(sd);
      fsimage.updateRemovedDirs(sd);
    }
    exitIfNoStreams();
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
    Iterator<StorageDirectory> it = fsimage.dirIterator(NameNodeDirType.EDITS);
    while (it.hasNext()) {
      StorageDirectory sd = it.next();
      if (!getEditNewFile(sd).renameTo(getEditFile(sd))) {
        //
        // renameTo() fails on Windows if the destination
        // file exists.
        //
        getEditFile(sd).delete();
        if (!getEditNewFile(sd).renameTo(getEditFile(sd))) {
          sd.unlock();
          removeEditsForStorageDir(sd);
          fsimage.updateRemovedDirs(sd);
          it.remove();
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
    StorageDirectory sd = null;
    for (Iterator<StorageDirectory> it = 
           fsimage.dirIterator(NameNodeDirType.EDITS); it.hasNext();)
      sd = it.next();
    return getEditFile(sd);
  }

  /**
   * Returns the timestamp of the edit log
   */
  synchronized long getFsEditTime() {
    Iterator<StorageDirectory> it = fsimage.dirIterator(NameNodeDirType.EDITS);
    if(it.hasNext())
      return getEditFile(it.next()).lastModified();
    return 0;
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

  /**
   * Stream wrapper that keeps track of the current file position.
   */
  private static class PositionTrackingInputStream extends FilterInputStream {
    private long curPos = 0;
    private long markPos = -1;

    public PositionTrackingInputStream(InputStream is) {
      super(is);
    }

    public int read() throws IOException {
      int ret = super.read();
      if (ret != -1) curPos++;
      return ret;
    }

    public int read(byte[] data) throws IOException {
      int ret = super.read(data);
      if (ret > 0) curPos += ret;
      return ret;
    }

    public int read(byte[] data, int offset, int length) throws IOException {
      int ret = super.read(data, offset, length);
      if (ret > 0) curPos += ret;
      return ret;
    }

    public void mark(int limit) {
      super.mark(limit);
      markPos = curPos;
    }

    public void reset() throws IOException {
      if (markPos == -1) {
        throw new IOException("Not marked!");
      }
      super.reset();
      curPos = markPos;
      markPos = -1;
    }

    public long getPos() {
      return curPos;
    }
  }
}
