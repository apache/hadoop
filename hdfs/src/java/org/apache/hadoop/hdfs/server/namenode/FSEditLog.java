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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DeprecatedUTF8;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.security.token.delegation.DelegationKey;

/**
 * FSEditLog maintains a log of the namespace modifications.
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSEditLog {
  public  static final byte OP_INVALID = -1;
  private static final byte OP_ADD = 0;
  private static final byte OP_RENAME_OLD = 1;  // rename
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
  private static final byte OP_RENAME = 15;  // new rename
  private static final byte OP_CONCAT_DELETE = 16; // concat files.
  private static final byte OP_SYMLINK = 17; // a symbolic link
  private static final byte OP_GET_DELEGATION_TOKEN = 18; //new delegation token
  private static final byte OP_RENEW_DELEGATION_TOKEN = 19; //renew delegation token
  private static final byte OP_CANCEL_DELEGATION_TOKEN = 20; //cancel delegation token
  private static final byte OP_UPDATE_MASTER_KEY = 21; //update master key

  /* 
   * The following operations are used to control remote edit log streams,
   * and not logged into file streams.
   */
  static final byte OP_JSPOOL_START = // start journal spool
                                    NamenodeProtocol.JA_JSPOOL_START;
  static final byte OP_CHECKPOINT_TIME = // incr checkpoint time
                                    NamenodeProtocol.JA_CHECKPOINT_TIME;

  static final String NO_JOURNAL_STREAMS_WARNING = "!!! WARNING !!!" +
  		" File system changes are not persistent. No journal streams.";

  private volatile int sizeOutputFlushBuffer = 512*1024;

  private ArrayList<EditLogOutputStream> editStreams = null;
  private FSImage fsimage = null;

  // a monotonically increasing counter that represents transactionIds.
  private long txid = 0;

  // stores the last synced transactionId.
  private long synctxid = 0;

  // the time of printing the statistics to the log file.
  private long lastPrintTime;

  // is a sync currently running?
  private volatile boolean isSyncRunning;


  // these are statistics counters.
  private long numTransactions;        // number of transactions
  private long numTransactionsBatchedInSync;
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
  
  private int getNumEditsDirs() {
   return fsimage.getNumStorageDirs(NameNodeDirType.EDITS);
  }

  synchronized int getNumEditStreams() {
    return editStreams == null ? 0 : editStreams.size();
  }

  /**
   * Return the currently active edit streams.
   * This should be used only by unit tests.
   */
  ArrayList<EditLogOutputStream> getEditStreams() {
    return editStreams;
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
    numTransactions = totalTimeTransactions = numTransactionsBatchedInSync = 0;
    if (editStreams == null)
      editStreams = new ArrayList<EditLogOutputStream>();
    
    ArrayList<StorageDirectory> al = null;
    for (Iterator<StorageDirectory> it = 
           fsimage.dirIterator(NameNodeDirType.EDITS); it.hasNext();) {
      StorageDirectory sd = it.next();
      File eFile = getEditFile(sd);
      try {
        addNewEditLogStream(eFile);
      } catch (IOException e) {
        FSNamesystem.LOG.warn("Unable to open edit log file " + eFile);
        // Remove the directory from list of storage directories
        if(al == null) al = new ArrayList<StorageDirectory>(1);
        al.add(sd);
        
      }
    }
    
    if(al != null) fsimage.processIOError(al, false);
  }
  
  
  synchronized void addNewEditLogStream(File eFile) throws IOException {
    EditLogOutputStream eStream = new EditLogFileOutputStream(eFile,
        sizeOutputFlushBuffer);
    editStreams.add(eStream);
  }

  synchronized void createEditLogFile(File name) throws IOException {
    waitForSyncToFinish();

    EditLogOutputStream eStream = new EditLogFileOutputStream(name,
        sizeOutputFlushBuffer);
    eStream.create();
    eStream.close();
  }

  /**
   * Shutdown the file store.
   */
  synchronized void close() {
    waitForSyncToFinish();
    if (editStreams == null || editStreams.isEmpty()) {
      return;
    }
    printStatistics(true);
    numTransactions = totalTimeTransactions = numTransactionsBatchedInSync = 0;

    ArrayList<EditLogOutputStream> errorStreams = null;
    Iterator<EditLogOutputStream> it = getOutputStreamIterator(null);
    while(it.hasNext()) {
      EditLogOutputStream eStream = it.next();
      try {
        closeStream(eStream);
      } catch (IOException e) {
        FSNamesystem.LOG.warn("FSEditLog:close - failed to close stream " 
            + eStream.getName());
        if(errorStreams == null)
          errorStreams = new ArrayList<EditLogOutputStream>(1);
        errorStreams.add(eStream);
      }
    }
    processIOError(errorStreams, true);
    editStreams.clear();
  }

  /**
   * Close and remove edit log stream.
   * @param index of the stream
   */
  synchronized private void removeStream(int index) {
    EditLogOutputStream eStream = editStreams.get(index);
    try {
      eStream.close();
    } catch (Exception e) {}
    editStreams.remove(index);
  }

  /**
   * The specified streams have IO errors. Close and remove them.
   * If propagate is true - close related StorageDirectories.
   * (is called with propagate value true from everywhere
   *  except fsimage.processIOError)
   */
  synchronized void processIOError(
      ArrayList<EditLogOutputStream> errorStreams,
      boolean propagate) {
    
    if (errorStreams == null || errorStreams.size() == 0) {
      return;                       // nothing to do
    }

    String lsd = fsimage.listStorageDirectories();
    FSNamesystem.LOG.info("current list of storage dirs:" + lsd);

    ArrayList<StorageDirectory> al = null;
    for (EditLogOutputStream eStream : errorStreams) {
      FSNamesystem.LOG.error("Unable to log edits to " + eStream.getName()
          + "; removing it");     

      StorageDirectory storageDir;
      if(propagate && eStream.getType() == JournalType.FILE && //find SD
          (storageDir = getStorage(eStream)) != null) {
        FSNamesystem.LOG.info("about to remove corresponding storage:" 
            + storageDir.getRoot().getAbsolutePath());
        // remove corresponding storage dir
        if(al == null) al = new ArrayList<StorageDirectory>(1);
        al.add(storageDir);
      }
      Iterator<EditLogOutputStream> ies = editStreams.iterator();
      while (ies.hasNext()) {
        EditLogOutputStream es = ies.next();
        if (es == eStream) {  
          try { eStream.close(); } catch (IOException e) {
            // nothing to do.
            FSNamesystem.LOG.warn("Failed to close eStream " + eStream.getName()
                + " before removing it (might be ok)");
          }
          ies.remove();
          break;
        }
      } 
    }
    
    if (editStreams == null || editStreams.size() <= 0) {
      String msg = "Fatal Error: All storage directories are inaccessible.";
      FSNamesystem.LOG.fatal(msg, new IOException(msg)); 
      Runtime.getRuntime().exit(-1);
    }

    // removed failed SDs
    if(propagate && al != null) fsimage.processIOError(al, false);
    
    //for the rest of the streams
    if(propagate) incrementCheckpointTime();
    
    lsd = fsimage.listStorageDirectories();
    FSNamesystem.LOG.info("at the end current list of storage dirs:" + lsd);
  }


  /**
   * get an editStream corresponding to a sd
   * @param es - stream to remove
   * @return the matching stream
   */
  StorageDirectory getStorage(EditLogOutputStream es) {
    String parentStorageDir = ((EditLogFileOutputStream)es).getFile()
    .getParentFile().getParentFile().getAbsolutePath();

    Iterator<StorageDirectory> it = fsimage.dirIterator(); 
    while (it.hasNext()) {
      StorageDirectory sd = it.next();
      FSNamesystem.LOG.info("comparing: " + parentStorageDir + " and " + sd.getRoot().getAbsolutePath()); 
      if (parentStorageDir.equals(sd.getRoot().getAbsolutePath()))
        return sd;
    }
    return null;
  }
  
  /**
   * get an editStream corresponding to a sd
   * @param sd
   * @return the matching stream
   */
  synchronized EditLogOutputStream getEditsStream(StorageDirectory sd) {
	for (EditLogOutputStream es : editStreams) {
	  File parentStorageDir = ((EditLogFileOutputStream)es).getFile()
	  .getParentFile().getParentFile();
	  if (parentStorageDir.getName().equals(sd.getRoot().getName()))
		return es;
	}
	return null;
  }

  /**
   * check if edits.new log exists in the specified stoorage directory
   */
  boolean existsNew(StorageDirectory sd) {
    return getEditNewFile(sd).exists(); 
  }

  /**
   * Load an edit log, and apply the changes to the in-memory structure
   * This is where we apply edits that we've been writing to disk all
   * along.
   */
  int loadFSEdits(EditLogInputStream edits) throws IOException {
    DataInputStream in = edits.getDataInputStream();
    long startTime = FSNamesystem.now();
    int numEdits = loadFSEdits(in, true);
    FSImage.LOG.info("Edits file " + edits.getName() 
        + " of size " + edits.length() + " edits # " + numEdits 
        + " loaded in " + (FSNamesystem.now()-startTime)/1000 + " seconds.");
    return numEdits;
  }

  int loadFSEdits(DataInputStream in, boolean closeOnExit) throws IOException {
    int numEdits = 0;
    int logVersion = 0;

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
      numEdits = loadEditRecords(logVersion, in, false);
    } finally {
      if(closeOnExit)
        in.close();
    }
    if (logVersion != FSConstants.LAYOUT_VERSION) // other version
      numEdits++; // save this image asap
    return numEdits;
  }

  @SuppressWarnings("deprecation")
  int loadEditRecords(int logVersion, DataInputStream in,
      boolean closeOnExit) throws IOException {
    FSNamesystem fsNamesys = fsimage.getFSNamesystem();
    FSDirectory fsDir = fsNamesys.dir;
    int numEdits = 0;
    String clientName = null;
    String clientMachine = null;
    String path = null;
    int numOpAdd = 0, numOpClose = 0, numOpDelete = 0,
        numOpRenameOld = 0, numOpSetRepl = 0, numOpMkDir = 0,
        numOpSetPerm = 0, numOpSetOwner = 0, numOpSetGenStamp = 0,
        numOpTimes = 0, numOpRename = 0, numOpConcatDelete = 0, 
        numOpSymlink = 0, numOpGetDelegationToken = 0,
        numOpRenewDelegationToken = 0, numOpCancelDelegationToken = 0, 
        numOpUpdateMasterKey = 0, numOpOther = 0;

    try {
      while (true) {
        long timestamp = 0;
        long mtime = 0;
        long atime = 0;
        long blockSize = 0;
        byte opcode = -1;
        try {
          in.mark(1);
          opcode = in.readByte();
          if (opcode == OP_INVALID) {
            in.reset(); // reset back to end of file if somebody reads it again
            break; // no more transactions
          }
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
          boolean isFileUnderConstruction = (opcode == OP_ADD);
          BlockInfo blocks[] = 
            readBlocks(in, logVersion, isFileUnderConstruction, replication);

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

          fsDir.unprotectedDelete(path, mtime);

          // add to the file tree
          INodeFile node = (INodeFile)fsDir.unprotectedAddFile(
                                                    path, permissions,
                                                    blocks, replication, 
                                                    mtime, atime, blockSize);
          if (isFileUnderConstruction) {
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
            fsNamesys.leaseManager.addLease(cons.getClientName(), path);
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
                                  + "Mkdir operation.");
          }
          String trg = FSImage.readString(in);
          int srcSize = length - 1 - 1; //trg and timestamp
          String [] srcs = new String [srcSize];
          for(int i=0; i<srcSize;i++) {
            srcs[i]= FSImage.readString(in);
          }
          timestamp = readLong(in);
          fsDir.unprotectedConcat(trg, srcs);
          break;
        }
        case OP_RENAME_OLD: {
          numOpRenameOld++;
          int length = in.readInt();
          if (length != 3) {
            throw new IOException("Incorrect data format. " 
                                  + "Mkdir operation.");
          }
          String s = FSImage.readString(in);
          String d = FSImage.readString(in);
          timestamp = readLong(in);
          HdfsFileStatus dinfo = fsDir.getFileInfo(d, false);
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
          fsNamesys.setGenerationStamp(lw);
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
        case OP_SYMLINK: {
          numOpSymlink++;
          int length = in.readInt();
          if (length != 4) {
            throw new IOException("Incorrect data format. " 
                                  + "symlink operation.");
          }
          path = FSImage.readString(in);
          String value = FSImage.readString(in);
          mtime = readLong(in);
          atime = readLong(in);
          PermissionStatus perm = PermissionStatus.read(in);
          fsDir.unprotectedSymlink(path, value, mtime, atime, perm);
          break;
        }
        case OP_RENAME: {
          if (logVersion > -21) {
            throw new IOException("Unexpected opcode " + opcode
                + " for version " + logVersion);
          }
          numOpRename++;
          int length = in.readInt();
          if (length != 3) {
            throw new IOException("Incorrect data format. " 
                                  + "Mkdir operation.");
          }
          String s = FSImage.readString(in);
          String d = FSImage.readString(in);
          timestamp = readLong(in);
          Rename[] options = readRenameOptions(in);
          HdfsFileStatus dinfo = fsDir.getFileInfo(d, false);
          fsDir.unprotectedRenameTo(s, d, timestamp, options);
          fsNamesys.changeLease(s, d, dinfo);
          break;
        }
        case OP_GET_DELEGATION_TOKEN: {
          if (logVersion > -24) {
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
          if (logVersion > -24) {
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
          if (logVersion > -24) {
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
          if (logVersion > -24) {
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
    } finally {
      if(closeOnExit)
        in.close();
    }
    if (FSImage.LOG.isDebugEnabled()) {
      FSImage.LOG.debug("numOpAdd = " + numOpAdd + " numOpClose = " + numOpClose 
          + " numOpDelete = " + numOpDelete 
          + " numOpRenameOld = " + numOpRenameOld 
          + " numOpSetRepl = " + numOpSetRepl + " numOpMkDir = " + numOpMkDir
          + " numOpSetPerm = " + numOpSetPerm 
          + " numOpSetOwner = " + numOpSetOwner
          + " numOpSetGenStamp = " + numOpSetGenStamp 
          + " numOpTimes = " + numOpTimes
          + " numOpConcatDelete  = " + numOpConcatDelete
          + " numOpRename = " + numOpRename
          + " numOpGetDelegationToken = " + numOpGetDelegationToken
          + " numOpRenewDelegationToken = " + numOpRenewDelegationToken
          + " numOpCancelDelegationToken = " + numOpCancelDelegationToken
          + " numOpUpdateMasterKey = " + numOpUpdateMasterKey
          + " numOpOther = " + numOpOther);
    }
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
  
  short adjustReplication(short replication) {
    FSNamesystem fsNamesys = fsimage.getFSNamesystem();
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
    if(getNumEditStreams() == 0)
      throw new java.lang.IllegalStateException(NO_JOURNAL_STREAMS_WARNING);
    ArrayList<EditLogOutputStream> errorStreams = null;
    long start = FSNamesystem.now();
    for(EditLogOutputStream eStream : editStreams) {
      FSImage.LOG.debug("loggin edits into " + eStream.getName()  + " stream");
      if(!eStream.isOperationSupported(op))
        continue;
      try {
        eStream.write(op, writables);
      } catch (IOException ie) {
        FSImage.LOG.warn("logEdit: removing "+ eStream.getName(), ie);
        if(errorStreams == null)
          errorStreams = new ArrayList<EditLogOutputStream>(1);
        errorStreams.add(eStream);
      }
    }
    processIOError(errorStreams, true);
    recordTransaction(start);
  }

  private void recordTransaction(long start) {
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
      metrics.transactions.inc((end-start));
  }

  /**
   * Blocks until all ongoing edits have been synced to disk.
   * This differs from logSync in that it waits for edits that have been
   * written by other threads, not just edits from the calling thread.
   *
   * NOTE: this should be done while holding the FSNamesystem lock, or
   * else more operations can start writing while this is in progress.
   */
  void logSyncAll() throws IOException {
    // Record the most recent transaction ID as our own id
    synchronized (this) {
      TransactionId id = myTransactionId.get();
      id.txid = txid;
    }
    // Then make sure we're synced up to this point
    logSync();
  }
  
  /**
   * Sync all modifications done by this thread.
   *
   * The internal concurrency design of this class is as follows:
   *   - Log items are written synchronized into an in-memory buffer,
   *     and each assigned a transaction ID.
   *   - When a thread (client) would like to sync all of its edits, logSync()
   *     uses a ThreadLocal transaction ID to determine what edit number must
   *     be synced to.
   *   - The isSyncRunning volatile boolean tracks whether a sync is currently
   *     under progress.
   *
   * The data is double-buffered within each edit log implementation so that
   * in-memory writing can occur in parallel with the on-disk writing.
   *
   * Each sync occurs in three steps:
   *   1. synchronized, it swaps the double buffer and sets the isSyncRunning
   *      flag.
   *   2. unsynchronized, it flushes the data to storage
   *   3. synchronized, it resets the flag and notifies anyone waiting on the
   *      sync.
   *
   * The lack of synchronization on step 2 allows other threads to continue
   * to write into the memory buffer while the sync is in progress.
   * Because this step is unsynchronized, actions that need to avoid
   * concurrency with sync() should be synchronized and also call
   * waitForSyncToFinish() before assuming they are running alone.
   */
  public void logSync() throws IOException {
    ArrayList<EditLogOutputStream> errorStreams = null;
    long syncStart = 0;

    // Fetch the transactionId of this thread. 
    long mytxid = myTransactionId.get().txid;
    EditLogOutputStream streams[] = null;
    boolean sync = false;
    try {
      synchronized (this) {
        assert editStreams.size() > 0 : "no editlog streams";
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
            metrics.transactionsBatchedInSync.inc();
          return;
        }
     
        // now, this thread will do the sync
        syncStart = txid;
        isSyncRunning = true;
        sync = true;
  
        // swap buffers
        for(EditLogOutputStream eStream : editStreams) {
          eStream.setReadyToFlush();
        }
        streams = 
          editStreams.toArray(new EditLogOutputStream[editStreams.size()]);
      }
  
      // do the sync
      long start = FSNamesystem.now();
      for (int idx = 0; idx < streams.length; idx++) {
        EditLogOutputStream eStream = streams[idx];
        try {
          eStream.flush();
        } catch (IOException ie) {
          FSNamesystem.LOG.error("Unable to sync edit log.", ie);
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
      processIOError(errorStreams, true);
  
      if (metrics != null) // Metrics non-null only when used inside name node
        metrics.syncs.inc(elapsed);
    } finally {
      synchronized (this) {
        synctxid = syncStart;
        if (sync) {
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
    buf.append("Number of transactions batched in Syncs: ");
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
    FSNamesystem.LOG.info(buf);
  }

  /** 
   * Add open lease record to edit log. 
   * Records the block locations of the last block.
   */
  public void logOpenFile(String path, INodeFileUnderConstruction newNode) {

    DeprecatedUTF8 nameReplicationPair[] = new DeprecatedUTF8[] { 
      new DeprecatedUTF8(path), 
      FSEditLog.toLogReplication(newNode.getReplication()),
      FSEditLog.toLogLong(newNode.getModificationTime()),
      FSEditLog.toLogLong(newNode.getAccessTime()),
      FSEditLog.toLogLong(newNode.getPreferredBlockSize())};
    logEdit(OP_ADD,
            new ArrayWritable(DeprecatedUTF8.class, nameReplicationPair), 
            new ArrayWritable(Block.class, newNode.getBlocks()),
            newNode.getPermissionStatus(),
            new DeprecatedUTF8(newNode.getClientName()),
            new DeprecatedUTF8(newNode.getClientMachine()));
  }

  /** 
   * Add close lease record to edit log.
   */
  public void logCloseFile(String path, INodeFile newNode) {
    DeprecatedUTF8 nameReplicationPair[] = new DeprecatedUTF8[] {
      new DeprecatedUTF8(path),
      FSEditLog.toLogReplication(newNode.getReplication()),
      FSEditLog.toLogLong(newNode.getModificationTime()),
      FSEditLog.toLogLong(newNode.getAccessTime()),
      FSEditLog.toLogLong(newNode.getPreferredBlockSize())};
    logEdit(OP_CLOSE,
            new ArrayWritable(DeprecatedUTF8.class, nameReplicationPair),
            new ArrayWritable(Block.class, newNode.getBlocks()),
            newNode.getPermissionStatus());
  }
  
  /** 
   * Add create directory record to edit log
   */
  public void logMkDir(String path, INode newNode) {
    DeprecatedUTF8 info[] = new DeprecatedUTF8[] {
      new DeprecatedUTF8(path),
      FSEditLog.toLogLong(newNode.getModificationTime()),
      FSEditLog.toLogLong(newNode.getAccessTime())
    };
    logEdit(OP_MKDIR, new ArrayWritable(DeprecatedUTF8.class, info),
        newNode.getPermissionStatus());
  }
  
  /** 
   * Add rename record to edit log
   * TODO: use String parameters until just before writing to disk
   */
  void logRename(String src, String dst, long timestamp) {
    DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
      new DeprecatedUTF8(src),
      new DeprecatedUTF8(dst),
      FSEditLog.toLogLong(timestamp)};
    logEdit(OP_RENAME_OLD, new ArrayWritable(DeprecatedUTF8.class, info));
  }
  
  /** 
   * Add rename record to edit log
   */
  void logRename(String src, String dst, long timestamp, Options.Rename... options) {
    DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
      new DeprecatedUTF8(src),
      new DeprecatedUTF8(dst),
      FSEditLog.toLogLong(timestamp)};
    logEdit(OP_RENAME, new ArrayWritable(DeprecatedUTF8.class, info),
        toBytesWritable(options));
  }
  
  /** 
   * Add set replication record to edit log
   */
  void logSetReplication(String src, short replication) {
    logEdit(OP_SET_REPLICATION, 
            new DeprecatedUTF8(src), 
            FSEditLog.toLogReplication(replication));
  }
  
  /** Add set namespace quota record to edit log
   * 
   * @param src the string representation of the path to a directory
   * @param quota the directory size limit
   */
  void logSetQuota(String src, long nsQuota, long dsQuota) {
    logEdit(OP_SET_QUOTA, new DeprecatedUTF8(src), 
            new LongWritable(nsQuota), new LongWritable(dsQuota));
  }

  /**  Add set permissions record to edit log */
  void logSetPermissions(String src, FsPermission permissions) {
    logEdit(OP_SET_PERMISSIONS, new DeprecatedUTF8(src), permissions);
  }

  /**  Add set owner record to edit log */
  void logSetOwner(String src, String username, String groupname) {
    DeprecatedUTF8 u = new DeprecatedUTF8(username == null? "": username);
    DeprecatedUTF8 g = new DeprecatedUTF8(groupname == null? "": groupname);
    logEdit(OP_SET_OWNER, new DeprecatedUTF8(src), u, g);
  }
  
  /**
   * concat(trg,src..) log
   */
  void logConcat(String trg, String [] srcs, long timestamp) {
    int size = 1 + srcs.length + 1; // trg, srcs, timestamp
    DeprecatedUTF8 info[] = new DeprecatedUTF8[size];
    int idx = 0;
    info[idx++] = new DeprecatedUTF8(trg);
    for(int i=0; i<srcs.length; i++) {
      info[idx++] = new DeprecatedUTF8(srcs[i]);
    }
    info[idx] = FSEditLog.toLogLong(timestamp);
    logEdit(OP_CONCAT_DELETE, new ArrayWritable(DeprecatedUTF8.class, info));
  }
  
  /** 
   * Add delete file record to edit log
   */
  void logDelete(String src, long timestamp) {
    DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
      new DeprecatedUTF8(src),
      FSEditLog.toLogLong(timestamp)};
    logEdit(OP_DELETE, new ArrayWritable(DeprecatedUTF8.class, info));
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
    DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
      new DeprecatedUTF8(src),
      FSEditLog.toLogLong(mtime),
      FSEditLog.toLogLong(atime)};
    logEdit(OP_TIMES, new ArrayWritable(DeprecatedUTF8.class, info));
  }

  /** 
   * Add a create symlink record.
   */
  void logSymlink(String path, String value, long mtime, 
                  long atime, INodeSymlink node) {
    DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
      new DeprecatedUTF8(path),
      new DeprecatedUTF8(value),
      FSEditLog.toLogLong(mtime),
      FSEditLog.toLogLong(atime)};
    logEdit(OP_SYMLINK, 
            new ArrayWritable(DeprecatedUTF8.class, info),
            node.getPermissionStatus());
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
  
  static private DeprecatedUTF8 toLogReplication(short replication) {
    return new DeprecatedUTF8(Short.toString(replication));
  }
  
  static private DeprecatedUTF8 toLogLong(long timestamp) {
    return new DeprecatedUTF8(Long.toString(timestamp));
  }

  /**
   * Return the size of the current EditLog
   */
  synchronized long getEditLogSize() throws IOException {
    assert getNumEditsDirs() <= getNumEditStreams() : 
        "Number of edits directories should not exceed the number of streams.";
    long size = 0;
    ArrayList<EditLogOutputStream> al = null;
    for (int idx = 0; idx < getNumEditStreams(); idx++) {
      EditLogOutputStream es = editStreams.get(idx);
      try {
        long curSize = es.length();
        assert (size == 0 || size == curSize || curSize ==0) :
          "Wrong streams size";
        size = Math.max(size, curSize);
      } catch (IOException e) {
        FSImage.LOG.warn("getEditLogSize: editstream.length failed. removing editlog (" +
            idx + ") " + es.getName());
        if(al==null) al = new ArrayList<EditLogOutputStream>(1);
        al.add(es);
      }
    }
    if(al!=null) processIOError(al, true);
    return size;
  }
  
  /**
   * Closes the current edit log and opens edits.new. 
   */
  synchronized void rollEditLog() throws IOException {
    waitForSyncToFinish();
    Iterator<StorageDirectory> it = fsimage.dirIterator(NameNodeDirType.EDITS);
    if(!it.hasNext()) 
      return;
    //
    // If edits.new already exists in some directory, verify it
    // exists in all directories.
    //
    boolean alreadyExists = existsNew(it.next());
    while(it.hasNext()) {
      StorageDirectory sd = it.next();
      if(alreadyExists != existsNew(sd))
        throw new IOException(getEditNewFile(sd) 
              + "should " + (alreadyExists ? "" : "not ") + "exist.");
    }
    if(alreadyExists)
      return; // nothing to do, edits.new exists!

    // check if any of failed storage is now available and put it back
    fsimage.attemptRestoreRemovedStorage();

    divertFileStreams(
        Storage.STORAGE_DIR_CURRENT + "/" + NameNodeFile.EDITS_NEW.getName());
  }

  /**
   * Divert file streams from file edits to file edits.new.<p>
   * Close file streams, which are currently writing into edits files.
   * Create new streams based on file getRoot()/dest.
   * @param dest new stream path relative to the storage directory root.
   * @throws IOException
   */
  synchronized void divertFileStreams(String dest) throws IOException {
    waitForSyncToFinish();

    assert getNumEditStreams() >= getNumEditsDirs() :
      "Inconsistent number of streams";
    ArrayList<EditLogOutputStream> errorStreams = null;
    EditStreamIterator itE = 
      (EditStreamIterator)getOutputStreamIterator(JournalType.FILE);
    Iterator<StorageDirectory> itD = 
      fsimage.dirIterator(NameNodeDirType.EDITS);
    while(itE.hasNext() && itD.hasNext()) {
      EditLogOutputStream eStream = itE.next();
      StorageDirectory sd = itD.next();
      if(!eStream.getName().startsWith(sd.getRoot().getPath()))
        throw new IOException("Inconsistent order of edit streams: " + eStream);
      try {
        // close old stream
        closeStream(eStream);
        // create new stream
        eStream = new EditLogFileOutputStream(new File(sd.getRoot(), dest),
            sizeOutputFlushBuffer);
        eStream.create();
        // replace by the new stream
        itE.replace(eStream);
      } catch (IOException e) {
        FSNamesystem.LOG.warn("Error in editStream " + eStream.getName(), e);
        if(errorStreams == null)
          errorStreams = new ArrayList<EditLogOutputStream>(1);
        errorStreams.add(eStream);
      }
    }
    processIOError(errorStreams, true);
  }

  /**
   * Removes the old edit log and renames edits.new to edits.
   * Reopens the edits file.
   */
  synchronized void purgeEditLog() throws IOException {
    waitForSyncToFinish();
    revertFileStreams(
        Storage.STORAGE_DIR_CURRENT + "/" + NameNodeFile.EDITS_NEW.getName());
  }


  /**
   * The actual sync activity happens while not synchronized on this object.
   * Thus, synchronized activities that require that they are not concurrent
   * with file operations should wait for any running sync to finish.
   */
  synchronized void waitForSyncToFinish() {
    while (isSyncRunning) {
      try {
        wait(1000);
      } catch (InterruptedException ie) {}
    }
  }

  /**
   * Revert file streams from file edits.new back to file edits.<p>
   * Close file streams, which are currently writing into getRoot()/source.
   * Rename getRoot()/source to edits.
   * Reopen streams so that they start writing into edits files.
   * @param dest new stream path relative to the storage directory root.
   * @throws IOException
   */
  synchronized void revertFileStreams(String source) throws IOException {
    waitForSyncToFinish();

    assert getNumEditStreams() >= getNumEditsDirs() :
      "Inconsistent number of streams";
    ArrayList<EditLogOutputStream> errorStreams = null;
    EditStreamIterator itE = 
      (EditStreamIterator)getOutputStreamIterator(JournalType.FILE);
    Iterator<StorageDirectory> itD = 
      fsimage.dirIterator(NameNodeDirType.EDITS);
    while(itE.hasNext() && itD.hasNext()) {
      EditLogOutputStream eStream = itE.next();
      StorageDirectory sd = itD.next();
      if(!eStream.getName().startsWith(sd.getRoot().getPath()))
        throw new IOException("Inconsistent order of edit streams: " + eStream +
                              " does not start with " + sd.getRoot().getPath());
      try {
        // close old stream
        closeStream(eStream);
        // rename edits.new to edits
        File editFile = getEditFile(sd);
        File prevEditFile = new File(sd.getRoot(), source);
        if(prevEditFile.exists()) {
          if(!prevEditFile.renameTo(editFile)) {
            //
            // renameTo() fails on Windows if the destination
            // file exists.
            //
            if(!editFile.delete() || !prevEditFile.renameTo(editFile)) {
              throw new IOException("Rename failed for " + sd.getRoot());
            }
          }
        }
        // open new stream
        eStream = new EditLogFileOutputStream(editFile, sizeOutputFlushBuffer);
        // replace by the new stream
        itE.replace(eStream);
      } catch (IOException e) {
        FSNamesystem.LOG.warn("Error in editStream " + eStream.getName(), e);
        if(errorStreams == null)
          errorStreams = new ArrayList<EditLogOutputStream>(1);
        errorStreams.add(eStream);
      }
    }
    processIOError(errorStreams, true);
  }

  /**
   * Return the name of the edit file
   */
  synchronized File getFsEditName() {
    StorageDirectory sd = null;   
    for (Iterator<StorageDirectory> it = 
      fsimage.dirIterator(NameNodeDirType.EDITS); it.hasNext();) {
      sd = it.next();   
      if(sd.getRoot().canRead())
        return getEditFile(sd);
    }
    return null;
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
  public void setBufferCapacity(int size) {
    sizeOutputFlushBuffer = size;
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

  static private BlockInfo[] readBlocks(
      DataInputStream in,
      int logVersion,
      boolean isFileUnderConstruction,
      short replication) throws IOException {
    int numBlocks = in.readInt();
    BlockInfo[] blocks = new BlockInfo[numBlocks];
    Block blk = new Block();
    BlockTwo oldblk = new BlockTwo();
    for (int i = 0; i < numBlocks; i++) {
      if (logVersion <= -14) {
        blk.readFields(in);
      } else {
        oldblk.readFields(in);
        blk.set(oldblk.blkid, oldblk.len,
                GenerationStamp.GRANDFATHER_GENERATION_STAMP);
      }
      if(isFileUnderConstruction && i == numBlocks-1)
        blocks[i] = new BlockInfoUnderConstruction(blk, replication);
      else
        blocks[i] = new BlockInfo(blk, replication);
    }
    return blocks;
  }

  boolean isEmpty() throws IOException {
    return getEditLogSize() <= 0;
  }

  /**
   * Create (or find if already exists) an edit output stream, which
   * streams journal records (edits) to the specified backup node.<br>
   * Send a record, prescribing to start journal spool.<br>
   * This should be sent via regular stream of journal records so that
   * the backup node new exactly after which record it should start spooling.
   * 
   * @param bnReg the backup node registration information.
   * @param nnReg this (active) name-node registration.
   * @throws IOException
   */
  synchronized void logJSpoolStart(NamenodeRegistration bnReg, // backup node
                      NamenodeRegistration nnReg) // active name-node
  throws IOException {
    if(bnReg.isRole(NamenodeRole.CHECKPOINT))
      return; // checkpoint node does not stream edits
    if(editStreams == null)
      editStreams = new ArrayList<EditLogOutputStream>();
    EditLogOutputStream boStream = null;
    for(EditLogOutputStream eStream : editStreams) {
      if(eStream.getName().equals(bnReg.getAddress())) {
        boStream = eStream; // already there
        break;
      }
    }
    if(boStream == null) {
      boStream = new EditLogBackupOutputStream(bnReg, nnReg);
      editStreams.add(boStream);
    }
    logEdit(OP_JSPOOL_START, (Writable[])null);
  }

  /**
   * Write an operation to the edit log. Do not sync to persistent
   * store yet.
   */
  synchronized void logEdit(int length, byte[] data) {
    if(getNumEditStreams() == 0)
      throw new java.lang.IllegalStateException(NO_JOURNAL_STREAMS_WARNING);
    ArrayList<EditLogOutputStream> errorStreams = null;
    long start = FSNamesystem.now();
    for(EditLogOutputStream eStream : editStreams) {
      try {
        eStream.write(data, 0, length);
      } catch (IOException ie) {
        FSNamesystem.LOG.warn("Error in editStream " + eStream.getName(), ie);
        if(errorStreams == null)
          errorStreams = new ArrayList<EditLogOutputStream>(1);
        errorStreams.add(eStream);
      }
    }
    processIOError(errorStreams, true);
    recordTransaction(start);
  }

  /**
   * Iterates output streams based of the same type.
   * Type null will iterate over all streams.
   */
  private class EditStreamIterator implements Iterator<EditLogOutputStream> {
    JournalType type;
    int prevIndex; // for remove()
    int nextIndex; // for next()

    EditStreamIterator(JournalType streamType) {
      this.type = streamType;
      this.nextIndex = 0;
      this.prevIndex = 0;
    }

    public boolean hasNext() {
      synchronized(FSEditLog.this) {
        if(editStreams == null || 
           editStreams.isEmpty() || nextIndex >= editStreams.size())
          return false;
        while(nextIndex < editStreams.size()
              && !editStreams.get(nextIndex).getType().isOfType(type))
          nextIndex++;
        return nextIndex < editStreams.size();
      }
    }

    public EditLogOutputStream next() {
      EditLogOutputStream stream = null;
      synchronized(FSEditLog.this) {
        stream = editStreams.get(nextIndex);
        prevIndex = nextIndex;
        nextIndex++;
        while(nextIndex < editStreams.size()
            && !editStreams.get(nextIndex).getType().isOfType(type))
        nextIndex++;
      }
      return stream;
    }

    public void remove() {
      nextIndex = prevIndex; // restore previous state
      removeStream(prevIndex); // remove last returned element
      hasNext(); // reset nextIndex to correct place
    }

    void replace(EditLogOutputStream newStream) {
      synchronized (FSEditLog.this) {
        assert 0 <= prevIndex && prevIndex < editStreams.size() :
                                                          "Index out of bound.";
        editStreams.set(prevIndex, newStream);
      }
    }
  }

  /**
   * Get stream iterator for the specified type.
   */
  public Iterator<EditLogOutputStream>
  getOutputStreamIterator(JournalType streamType) {
    return new EditStreamIterator(streamType);
  }

  private void closeStream(EditLogOutputStream eStream) throws IOException {
    eStream.setReadyToFlush();
    eStream.flush();
    eStream.close();
  }

  void incrementCheckpointTime() {
    fsimage.incrementCheckpointTime();
    Writable[] args = {new LongWritable(fsimage.getCheckpointTime())};
    logEdit(OP_CHECKPOINT_TIME, args);
  }

  synchronized void releaseBackupStream(NamenodeRegistration registration) {
    Iterator<EditLogOutputStream> it =
                                  getOutputStreamIterator(JournalType.BACKUP);
    ArrayList<EditLogOutputStream> errorStreams = null;
    NamenodeRegistration backupNode = null;
    while(it.hasNext()) {
      EditLogBackupOutputStream eStream = (EditLogBackupOutputStream)it.next();
      backupNode = eStream.getRegistration();
      if(backupNode.getAddress().equals(registration.getAddress()) &&
            backupNode.isRole(registration.getRole())) {
        errorStreams = new ArrayList<EditLogOutputStream>(1);
        errorStreams.add(eStream);
        break;
      }
    }
    assert backupNode == null || backupNode.isRole(NamenodeRole.BACKUP) :
      "Not a backup node corresponds to a backup stream";
    processIOError(errorStreams, true);
  }

  synchronized boolean checkBackupRegistration(
      NamenodeRegistration registration) {
    Iterator<EditLogOutputStream> it =
                                  getOutputStreamIterator(JournalType.BACKUP);
    boolean regAllowed = !it.hasNext();
    NamenodeRegistration backupNode = null;
    ArrayList<EditLogOutputStream> errorStreams = null;
    while(it.hasNext()) {
      EditLogBackupOutputStream eStream = (EditLogBackupOutputStream)it.next();
      backupNode = eStream.getRegistration();
      if(backupNode.getAddress().equals(registration.getAddress()) &&
          backupNode.isRole(registration.getRole())) {
        regAllowed = true; // same node re-registers
        break;
      }
      if(!eStream.isAlive()) {
        if(errorStreams == null)
          errorStreams = new ArrayList<EditLogOutputStream>(1);
        errorStreams.add(eStream);
        regAllowed = true; // previous backup node failed
      }
    }
    assert backupNode == null || backupNode.isRole(NamenodeRole.BACKUP) :
      "Not a backup node corresponds to a backup stream";
    processIOError(errorStreams, true);
    return regAllowed;
  }
  
  static Rename[] readRenameOptions(DataInputStream in) throws IOException {
    BytesWritable writable = new BytesWritable();
    writable.readFields(in);
    
    byte[] bytes = writable.getBytes();
    Rename[] options = new Rename[bytes.length];
    
    for (int i = 0; i < bytes.length; i++) {
      options[i] = Rename.valueOf(bytes[i]);
    }
    return options;
  }
  
  static BytesWritable toBytesWritable(Options.Rename... options) {
    byte[] bytes = new byte[options.length];
    for (int i = 0; i < options.length; i++) {
      bytes[i] = options[i].value();
    }
    return new BytesWritable(bytes);
  }
}
