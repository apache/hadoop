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

import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;
import java.util.EnumMap;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.Storage;

import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.*;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.EOFException;

/**
 * Helper classes for reading the ops from an InputStream.
 * All ops derive from FSEditLogOp and are only
 * instantiated from Reader#readOp()
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class FSEditLogOp {
  final FSEditLogOpCodes opCode;
  long txid;


  /**
   * Constructor for an EditLog Op. EditLog ops cannot be constructed
   * directly, but only through Reader#readOp.
   */
  private FSEditLogOp(FSEditLogOpCodes opCode) {
    this.opCode = opCode;
    this.txid = 0;
  }

  public void setTransactionId(long txid) {
    this.txid = txid;
  }

  public abstract void readFields(DataInputStream in, int logVersion)
      throws IOException;

  static class AddCloseOp extends FSEditLogOp {
    int length;
    String path;
    short replication;
    long mtime;
    long atime;
    long blockSize;
    Block[] blocks;
    PermissionStatus permissions;
    String clientName;
    String clientMachine;
    //final DatanodeDescriptor[] dataNodeDescriptors; UNUSED

    private AddCloseOp(FSEditLogOpCodes opCode) {
      super(opCode);
      assert(opCode == OP_ADD || opCode == OP_CLOSE);
    }

    public void readFields(DataInputStream in, int logVersion)
        throws IOException {
      // versions > 0 support per file replication
      // get name and replication
      this.length = in.readInt();
      if (-7 == logVersion && length != 3||
          -17 < logVersion && logVersion < -7 && length != 4 ||
          logVersion <= -17 && length != 5) {
        throw new IOException("Incorrect data format."  +
                              " logVersion is " + logVersion +
                              " but writables.length is " +
                              length + ". ");
      }
      this.path = FSImageSerialization.readString(in);
      this.replication = readShort(in);
      this.mtime = readLong(in);
      if (LayoutVersion.supports(Feature.FILE_ACCESS_TIME, logVersion)) {
        this.atime = readLong(in);
      } else {
        this.atime = 0;
      }
      if (logVersion < -7) {
        this.blockSize = readLong(in);
      } else {
        this.blockSize = 0;
      }

      // get blocks
      this.blocks = readBlocks(in, logVersion);

      if (logVersion <= -11) {
        this.permissions = PermissionStatus.read(in);
      } else {
        this.permissions = null;
      }

      // clientname, clientMachine and block locations of last block.
      if (this.opCode == OP_ADD && logVersion <= -12) {
        this.clientName = FSImageSerialization.readString(in);
        this.clientMachine = FSImageSerialization.readString(in);
        if (-13 <= logVersion) {
          readDatanodeDescriptorArray(in);
        }
      } else {
        this.clientName = "";
        this.clientMachine = "";
      }
    }

    /** This method is defined for compatibility reason. */
    private static DatanodeDescriptor[] readDatanodeDescriptorArray(DataInput in)
        throws IOException {
      DatanodeDescriptor[] locations = new DatanodeDescriptor[in.readInt()];
        for (int i = 0; i < locations.length; i++) {
          locations[i] = new DatanodeDescriptor();
          locations[i].readFieldsFromFSEditLog(in);
        }
        return locations;
    }

    private static Block[] readBlocks(
        DataInputStream in,
        int logVersion) throws IOException {
      int numBlocks = in.readInt();
      Block[] blocks = new Block[numBlocks];
      for (int i = 0; i < numBlocks; i++) {
        Block blk = new Block();
        if (logVersion <= -14) {
          blk.readFields(in);
        } else {
          BlockTwo oldblk = new BlockTwo();
          oldblk.readFields(in);
          blk.set(oldblk.blkid, oldblk.len,
                  GenerationStamp.GRANDFATHER_GENERATION_STAMP);
        }
        blocks[i] = blk;
      }
      return blocks;
    }
  }

  static class SetReplicationOp extends FSEditLogOp {
    String path;
    short replication;

    private SetReplicationOp() {
      super(OP_SET_REPLICATION);
    }

    public void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.path = FSImageSerialization.readString(in);
      this.replication = readShort(in);
    }
  }

  static class ConcatDeleteOp extends FSEditLogOp {
    int length;
    String trg;
    String[] srcs;
    long timestamp;

    private ConcatDeleteOp() {
      super(OP_CONCAT_DELETE);
    }

    public void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.length = in.readInt();
      if (length < 3) { // trg, srcs.., timestam
        throw new IOException("Incorrect data format. "
                              + "Concat delete operation.");
      }
      this.trg = FSImageSerialization.readString(in);
      int srcSize = this.length - 1 - 1; //trg and timestamp
      this.srcs = new String [srcSize];
      for(int i=0; i<srcSize;i++) {
        srcs[i]= FSImageSerialization.readString(in);
      }
      this.timestamp = readLong(in);
    }
  }

  static class RenameOldOp extends FSEditLogOp {
    int length;
    String src;
    String dst;
    long timestamp;

    private RenameOldOp() {
      super(OP_RENAME_OLD);
    }

    public void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.length = in.readInt();
      if (this.length != 3) {
        throw new IOException("Incorrect data format. "
                              + "Old rename operation.");
      }
      this.src = FSImageSerialization.readString(in);
      this.dst = FSImageSerialization.readString(in);
      this.timestamp = readLong(in);
    }
  }

  static class DeleteOp extends FSEditLogOp {
    int length;
    String path;
    long timestamp;

    private DeleteOp() {
      super(OP_DELETE);
    }

    public void readFields(DataInputStream in, int logVersion)
        throws IOException {

      this.length = in.readInt();
      if (this.length != 2) {
        throw new IOException("Incorrect data format. "
                              + "delete operation.");
      }
      this.path = FSImageSerialization.readString(in);
      this.timestamp = readLong(in);
    }
  }

  static class MkdirOp extends FSEditLogOp {
    int length;
    String path;
    long timestamp;
    PermissionStatus permissions;

    private MkdirOp() {
      super(OP_MKDIR);
    }

    public void readFields(DataInputStream in, int logVersion)
        throws IOException {

      this.length = in.readInt();
      if (-17 < logVersion && length != 2 ||
          logVersion <= -17 && length != 3) {
        throw new IOException("Incorrect data format. "
                              + "Mkdir operation.");
      }
      this.path = FSImageSerialization.readString(in);
      this.timestamp = readLong(in);

      // The disk format stores atimes for directories as well.
      // However, currently this is not being updated/used because of
      // performance reasons.
      if (LayoutVersion.supports(Feature.FILE_ACCESS_TIME, logVersion)) {
        /*unused this.atime = */readLong(in);
      }

      if (logVersion <= -11) {
        this.permissions = PermissionStatus.read(in);
      } else {
        this.permissions = null;
      }
    }
  }

  static class SetGenstampOp extends FSEditLogOp {
    long genStamp;

    private SetGenstampOp() {
      super(OP_SET_GENSTAMP);
    }

    public void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.genStamp = in.readLong();
    }
  }

  static class DatanodeAddOp extends FSEditLogOp {
    @SuppressWarnings("deprecation")
    private DatanodeAddOp() {
      super(OP_DATANODE_ADD);
    }

    public void readFields(DataInputStream in, int logVersion)
        throws IOException {
      //Datanodes are not persistent any more.
      FSImageSerialization.DatanodeImage.skipOne(in);
    }
  }

  static class DatanodeRemoveOp extends FSEditLogOp {
    @SuppressWarnings("deprecation")
    private DatanodeRemoveOp() {
      super(OP_DATANODE_REMOVE);
    }

    public void readFields(DataInputStream in, int logVersion)
        throws IOException {
      DatanodeID nodeID = new DatanodeID();
      nodeID.readFields(in);
      //Datanodes are not persistent any more.
    }
  }

  static class SetPermissionsOp extends FSEditLogOp {
    String src;
    FsPermission permissions;

    private SetPermissionsOp() {
      super(OP_SET_PERMISSIONS);
    }

    public void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.src = FSImageSerialization.readString(in);
      this.permissions = FsPermission.read(in);
    }
  }

  static class SetOwnerOp extends FSEditLogOp {
    String src;
    String username;
    String groupname;

    private SetOwnerOp() {
      super(OP_SET_OWNER);
    }

    public void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.src = FSImageSerialization.readString(in);
      this.username = FSImageSerialization.readString_EmptyAsNull(in);
      this.groupname = FSImageSerialization.readString_EmptyAsNull(in);
    }

  }

  static class SetNSQuotaOp extends FSEditLogOp {
    String src;
    long nsQuota;

    private SetNSQuotaOp() {
      super(OP_SET_NS_QUOTA);
    }

    public void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.src = FSImageSerialization.readString(in);
      this.nsQuota = readLongWritable(in);
    }
  }

  static class ClearNSQuotaOp extends FSEditLogOp {
    String src;

    private ClearNSQuotaOp() {
      super(OP_CLEAR_NS_QUOTA);
    }

    public void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.src = FSImageSerialization.readString(in);
    }
  }

  static class SetQuotaOp extends FSEditLogOp {
    String src;
    long nsQuota;
    long dsQuota;

    private SetQuotaOp() {
      super(OP_SET_QUOTA);
    }

    public void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.src = FSImageSerialization.readString(in);
      this.nsQuota = readLongWritable(in);
      this.dsQuota = readLongWritable(in);
    }
  }

  static class TimesOp extends FSEditLogOp {
    int length;
    String path;
    long mtime;
    long atime;

    private TimesOp() {
      super(OP_TIMES);
    }

    public void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.length = in.readInt();
      if (length != 3) {
        throw new IOException("Incorrect data format. "
                              + "times operation.");
      }
      this.path = FSImageSerialization.readString(in);
      this.mtime = readLong(in);
      this.atime = readLong(in);
    }
  }

  static class SymlinkOp extends FSEditLogOp {
    int length;
    String path;
    String value;
    long mtime;
    long atime;
    PermissionStatus permissionStatus;

    private SymlinkOp() {
      super(OP_SYMLINK);
    }

    public void readFields(DataInputStream in, int logVersion)
        throws IOException {

      this.length = in.readInt();
      if (this.length != 4) {
        throw new IOException("Incorrect data format. "
                              + "symlink operation.");
      }
      this.path = FSImageSerialization.readString(in);
      this.value = FSImageSerialization.readString(in);
      this.mtime = readLong(in);
      this.atime = readLong(in);
      this.permissionStatus = PermissionStatus.read(in);
    }
  }

  static class RenameOp extends FSEditLogOp {
    int length;
    String src;
    String dst;
    long timestamp;
    Rename[] options;

    private RenameOp() {
      super(OP_RENAME);
    }

    public void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.length = in.readInt();
      if (this.length != 3) {
        throw new IOException("Incorrect data format. "
                              + "Rename operation.");
      }
      this.src = FSImageSerialization.readString(in);
      this.dst = FSImageSerialization.readString(in);
      this.timestamp = readLong(in);
      this.options = readRenameOptions(in);
    }

    private static Rename[] readRenameOptions(DataInputStream in) throws IOException {
      BytesWritable writable = new BytesWritable();
      writable.readFields(in);

      byte[] bytes = writable.getBytes();
      Rename[] options = new Rename[bytes.length];

      for (int i = 0; i < bytes.length; i++) {
        options[i] = Rename.valueOf(bytes[i]);
      }
      return options;
    }
  }

  static class ReassignLeaseOp extends FSEditLogOp {
    String leaseHolder;
    String path;
    String newHolder;

    private ReassignLeaseOp() {
      super(OP_REASSIGN_LEASE);
    }
    
    public void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.leaseHolder = FSImageSerialization.readString(in);
      this.path = FSImageSerialization.readString(in);
      this.newHolder = FSImageSerialization.readString(in);
    }
  }

  static class GetDelegationTokenOp extends FSEditLogOp {
    DelegationTokenIdentifier token;
    long expiryTime;

    private GetDelegationTokenOp() {
      super(OP_GET_DELEGATION_TOKEN);
    }

    public void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.token = new DelegationTokenIdentifier();
      this.token.readFields(in);
      this.expiryTime = readLong(in);
    }
  }

  static class RenewDelegationTokenOp extends FSEditLogOp {
    DelegationTokenIdentifier token;
    long expiryTime;

    private RenewDelegationTokenOp() {
      super(OP_RENEW_DELEGATION_TOKEN);
    }

    public void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.token = new DelegationTokenIdentifier();
      this.token.readFields(in);
      this.expiryTime = readLong(in);
    }
  }

  static class CancelDelegationTokenOp extends FSEditLogOp {
    DelegationTokenIdentifier token;

    private CancelDelegationTokenOp() {
      super(OP_CANCEL_DELEGATION_TOKEN);
    }

    public void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.token = new DelegationTokenIdentifier();
      this.token.readFields(in);
    }
  }

  static class UpdateMasterKeyOp extends FSEditLogOp {
    DelegationKey key;

    private UpdateMasterKeyOp() {
      super(OP_UPDATE_MASTER_KEY);
    }

    public void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.key = new DelegationKey();
      this.key.readFields(in);
    }
  }
  
  static class LogSegmentOp extends FSEditLogOp {
    private LogSegmentOp(FSEditLogOpCodes code) {
      super(code);
      assert code == OP_START_LOG_SEGMENT ||
             code == OP_END_LOG_SEGMENT : "Bad op: " + code;
    }

    public void readFields(DataInputStream in, int logVersion)
        throws IOException {
      // no data stored in these ops yet
    }
  }

  static private short readShort(DataInputStream in) throws IOException {
    return Short.parseShort(FSImageSerialization.readString(in));
  }

  static private long readLong(DataInputStream in) throws IOException {
    return Long.parseLong(FSImageSerialization.readString(in));
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

    // a place holder for reading a long
  private static final LongWritable longWritable = new LongWritable();

  /** Read an integer from an input stream */
  private static long readLongWritable(DataInputStream in) throws IOException {
    synchronized (longWritable) {
      longWritable.readFields(in);
      return longWritable.get();
    }
  }
  
  /**
   * Class to encapsulate the header at the top of a log file.
   */
  static class LogHeader {
    final int logVersion;
    final Checksum checksum;

    public LogHeader(int logVersion, Checksum checksum) {
      this.logVersion = logVersion;
      this.checksum = checksum;
    }

    static LogHeader read(DataInputStream in) throws IOException {
      int logVersion = 0;

      logVersion = FSEditLogOp.LogHeader.readLogVersion(in);
      Checksum checksum = null;
      if (LayoutVersion.supports(Feature.EDITS_CHESKUM, logVersion)) {
        checksum = FSEditLog.getChecksum();
      }
      return new LogHeader(logVersion, checksum);
    }
    
    /**
     * Read the header of fsedit log
     * @param in fsedit stream
     * @return the edit log version number
     * @throws IOException if error occurs
     */
    private static int readLogVersion(DataInputStream in) throws IOException {
      int logVersion = 0;
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
      return logVersion;
    }
  }

  /**
   * Class for reading editlog ops from a stream
   */
  public static class Reader {
    private final DataInputStream in;
    private final int logVersion;
    private final Checksum checksum;
    private EnumMap<FSEditLogOpCodes, FSEditLogOp> opInstances;
    /**
     * Construct the reader
     * @param in The stream to read from.
     * @param logVersion The version of the data coming from the stream.
     * @param checksum Checksum being used with input stream.
     */
    @SuppressWarnings("deprecation")
    public Reader(DataInputStream in, int logVersion,
                  Checksum checksum) {
      if (checksum != null) {
        this.in = new DataInputStream(
            new CheckedInputStream(in, checksum));
      } else {
        this.in = in;
      }
      this.logVersion = logVersion;
      this.checksum = checksum;
      opInstances = new EnumMap<FSEditLogOpCodes, FSEditLogOp>(
          FSEditLogOpCodes.class);
      opInstances.put(OP_ADD, new AddCloseOp(OP_ADD));
      opInstances.put(OP_CLOSE, new AddCloseOp(OP_CLOSE));
      opInstances.put(OP_SET_REPLICATION, new SetReplicationOp());
      opInstances.put(OP_CONCAT_DELETE, new ConcatDeleteOp());
      opInstances.put(OP_RENAME_OLD, new RenameOldOp());
      opInstances.put(OP_DELETE, new DeleteOp());
      opInstances.put(OP_MKDIR, new MkdirOp());
      opInstances.put(OP_SET_GENSTAMP, new SetGenstampOp());
      opInstances.put(OP_DATANODE_ADD, new DatanodeAddOp());
      opInstances.put(OP_DATANODE_REMOVE, new DatanodeRemoveOp());
      opInstances.put(OP_SET_PERMISSIONS, new SetPermissionsOp());
      opInstances.put(OP_SET_OWNER, new SetOwnerOp());
      opInstances.put(OP_SET_NS_QUOTA, new SetNSQuotaOp());
      opInstances.put(OP_CLEAR_NS_QUOTA, new ClearNSQuotaOp());
      opInstances.put(OP_SET_QUOTA, new SetQuotaOp());
      opInstances.put(OP_TIMES, new TimesOp());
      opInstances.put(OP_SYMLINK, new SymlinkOp());
      opInstances.put(OP_RENAME, new RenameOp());
      opInstances.put(OP_REASSIGN_LEASE, new ReassignLeaseOp());
      opInstances.put(OP_GET_DELEGATION_TOKEN, new GetDelegationTokenOp());
      opInstances.put(OP_RENEW_DELEGATION_TOKEN, new RenewDelegationTokenOp());
      opInstances.put(OP_CANCEL_DELEGATION_TOKEN,
                      new CancelDelegationTokenOp());
      opInstances.put(OP_UPDATE_MASTER_KEY, new UpdateMasterKeyOp());
      opInstances.put(OP_START_LOG_SEGMENT,
                      new LogSegmentOp(OP_START_LOG_SEGMENT));
      opInstances.put(OP_END_LOG_SEGMENT,
                      new LogSegmentOp(OP_END_LOG_SEGMENT));
    }

    /**
     * Read an operation from the input stream.
     * 
     * Note that the objects returned from this method may be re-used by future
     * calls to the same method.
     * 
     * @return the operation read from the stream, or null at the end of the file
     * @throws IOException on error.
     */
    public FSEditLogOp readOp() throws IOException {
      if (checksum != null) {
        checksum.reset();
      }

      in.mark(1);

      byte opCodeByte;
      try {
        opCodeByte = in.readByte();
      } catch (EOFException eof) {
        // EOF at an opcode boundary is expected.
        return null;
      }

      FSEditLogOpCodes opCode = FSEditLogOpCodes.fromByte(opCodeByte);
      if (opCode == OP_INVALID) {
        in.reset(); // reset back to end of file if somebody reads it again
        return null;
      }

      FSEditLogOp op = opInstances.get(opCode);
      if (op == null) {
        throw new IOException("Read invalid opcode " + opCode);
      }

      if (LayoutVersion.supports(Feature.STORED_TXIDS, logVersion)) {
        // Read the txid
        op.setTransactionId(in.readLong());
      }

      op.readFields(in, logVersion);

      validateChecksum(in, checksum, op.txid);
      return op;
    }

    /**
     * Validate a transaction's checksum
     */
    private void validateChecksum(DataInputStream in,
                                  Checksum checksum,
                                  long txid)
        throws IOException {
      if (checksum != null) {
        int calculatedChecksum = (int)checksum.getValue();
        int readChecksum = in.readInt(); // read in checksum
        if (readChecksum != calculatedChecksum) {
          throw new ChecksumException(
              "Transaction is corrupt. Calculated checksum is " +
              calculatedChecksum + " but read checksum " + readChecksum, txid);
        }
      }
    }
  }
}
