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
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.util.PureJavaCrc32;

import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.*;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.hdfs.DeprecatedUTF8;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
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


  @SuppressWarnings("deprecation")
  private static ThreadLocal<EnumMap<FSEditLogOpCodes, FSEditLogOp>> opInstances =
    new ThreadLocal<EnumMap<FSEditLogOpCodes, FSEditLogOp>>() {
      @Override
      protected EnumMap<FSEditLogOpCodes, FSEditLogOp> initialValue() {
        EnumMap<FSEditLogOpCodes, FSEditLogOp> instances 
          = new EnumMap<FSEditLogOpCodes, FSEditLogOp>(FSEditLogOpCodes.class);
        instances.put(OP_ADD, new AddOp());
        instances.put(OP_CLOSE, new CloseOp());
        instances.put(OP_SET_REPLICATION, new SetReplicationOp());
        instances.put(OP_CONCAT_DELETE, new ConcatDeleteOp());
        instances.put(OP_RENAME_OLD, new RenameOldOp());
        instances.put(OP_DELETE, new DeleteOp());
        instances.put(OP_MKDIR, new MkdirOp());
        instances.put(OP_SET_GENSTAMP, new SetGenstampOp());
        instances.put(OP_DATANODE_ADD, new DatanodeAddOp());
        instances.put(OP_DATANODE_REMOVE, new DatanodeRemoveOp());
        instances.put(OP_SET_PERMISSIONS, new SetPermissionsOp());
        instances.put(OP_SET_OWNER, new SetOwnerOp());
        instances.put(OP_SET_NS_QUOTA, new SetNSQuotaOp());
        instances.put(OP_CLEAR_NS_QUOTA, new ClearNSQuotaOp());
        instances.put(OP_SET_QUOTA, new SetQuotaOp());
        instances.put(OP_TIMES, new TimesOp());
        instances.put(OP_SYMLINK, new SymlinkOp());
        instances.put(OP_RENAME, new RenameOp());
        instances.put(OP_REASSIGN_LEASE, new ReassignLeaseOp());
        instances.put(OP_GET_DELEGATION_TOKEN, new GetDelegationTokenOp());
        instances.put(OP_RENEW_DELEGATION_TOKEN, new RenewDelegationTokenOp());
        instances.put(OP_CANCEL_DELEGATION_TOKEN, 
                      new CancelDelegationTokenOp());
        instances.put(OP_UPDATE_MASTER_KEY, new UpdateMasterKeyOp());
        instances.put(OP_START_LOG_SEGMENT,
                      new LogSegmentOp(OP_START_LOG_SEGMENT));
        instances.put(OP_END_LOG_SEGMENT,
                      new LogSegmentOp(OP_END_LOG_SEGMENT));
        return instances;
      }
  };

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

  abstract void readFields(DataInputStream in, int logVersion)
      throws IOException;

  abstract void writeFields(DataOutputStream out)
      throws IOException;

  @SuppressWarnings("unchecked")
  static abstract class AddCloseOp extends FSEditLogOp {
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

    <T extends AddCloseOp> T setPath(String path) {
      this.path = path;
      return (T)this;
    }

    <T extends AddCloseOp> T setReplication(short replication) {
      this.replication = replication;
      return (T)this;
    }

    <T extends AddCloseOp> T setModificationTime(long mtime) {
      this.mtime = mtime;
      return (T)this;
    }

    <T extends AddCloseOp> T setAccessTime(long atime) {
      this.atime = atime;
      return (T)this;
    }

    <T extends AddCloseOp> T setBlockSize(long blockSize) {
      this.blockSize = blockSize;
      return (T)this;
    }

    <T extends AddCloseOp> T setBlocks(Block[] blocks) {
      this.blocks = blocks;
      return (T)this;
    }

    <T extends AddCloseOp> T setPermissionStatus(PermissionStatus permissions) {
      this.permissions = permissions;
      return (T)this;
    }

    <T extends AddCloseOp> T setClientName(String clientName) {
      this.clientName = clientName;
      return (T)this;
    }

    <T extends AddCloseOp> T setClientMachine(String clientMachine) {
      this.clientMachine = clientMachine;
      return (T)this;
    }

    @Override 
    void writeFields(DataOutputStream out) throws IOException {
      DeprecatedUTF8 nameReplicationPair[] = new DeprecatedUTF8[] { 
        new DeprecatedUTF8(path), 
        toLogReplication(replication),
        toLogLong(mtime),
        toLogLong(atime),
        toLogLong(blockSize)};
      new ArrayWritable(DeprecatedUTF8.class, nameReplicationPair).write(out);
      new ArrayWritable(Block.class, blocks).write(out);
      permissions.write(out);

      if (this.opCode == OP_ADD) {
        new DeprecatedUTF8(clientName).write(out);
        new DeprecatedUTF8(clientMachine).write(out);
      }
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
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

  static class AddOp extends AddCloseOp {
    private AddOp() {
      super(OP_ADD);
    }

    static AddOp getInstance() {
      return (AddOp)opInstances.get().get(OP_ADD);
    }
  }

  static class CloseOp extends AddCloseOp {
    private CloseOp() {
      super(OP_CLOSE);
    }

    static CloseOp getInstance() {
      return (CloseOp)opInstances.get().get(OP_CLOSE);
    }
  }

  static class SetReplicationOp extends FSEditLogOp {
    String path;
    short replication;

    private SetReplicationOp() {
      super(OP_SET_REPLICATION);
    }

    static SetReplicationOp getInstance() {
      return (SetReplicationOp)opInstances.get()
        .get(OP_SET_REPLICATION);
    }

    SetReplicationOp setPath(String path) {
      this.path = path;
      return this;
    }

    SetReplicationOp setReplication(short replication) {
      this.replication = replication;
      return this;
    }

    @Override 
    void writeFields(DataOutputStream out) throws IOException {
      new DeprecatedUTF8(path).write(out);
      new DeprecatedUTF8(Short.toString(replication)).write(out);
    }
    
    @Override
    void readFields(DataInputStream in, int logVersion)
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

    static ConcatDeleteOp getInstance() {
      return (ConcatDeleteOp)opInstances.get()
        .get(OP_CONCAT_DELETE);
    }

    ConcatDeleteOp setTarget(String trg) {
      this.trg = trg;
      return this;
    }

    ConcatDeleteOp setSources(String[] srcs) {
      this.srcs = srcs;
      return this;
    }

    ConcatDeleteOp setTimestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    @Override 
    void writeFields(DataOutputStream out) throws IOException {
      int size = 1 + srcs.length + 1; // trg, srcs, timestamp
      DeprecatedUTF8 info[] = new DeprecatedUTF8[size];
      int idx = 0;
      info[idx++] = new DeprecatedUTF8(trg);
      for(int i=0; i<srcs.length; i++) {
        info[idx++] = new DeprecatedUTF8(srcs[i]);
      }
      info[idx] = toLogLong(timestamp);
      new ArrayWritable(DeprecatedUTF8.class, info).write(out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
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

    static RenameOldOp getInstance() {
      return (RenameOldOp)opInstances.get()
        .get(OP_RENAME_OLD);
    }

    RenameOldOp setSource(String src) {
      this.src = src;
      return this;
    }

    RenameOldOp setDestination(String dst) {
      this.dst = dst;
      return this;
    }

    RenameOldOp setTimestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    @Override 
    void writeFields(DataOutputStream out) throws IOException {
      DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
        new DeprecatedUTF8(src),
        new DeprecatedUTF8(dst),
        toLogLong(timestamp)};
      new ArrayWritable(DeprecatedUTF8.class, info).write(out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
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

    static DeleteOp getInstance() {
      return (DeleteOp)opInstances.get()
        .get(OP_DELETE);
    }

    DeleteOp setPath(String path) {
      this.path = path;
      return this;
    }

    DeleteOp setTimestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    @Override 
    void writeFields(DataOutputStream out) throws IOException {
      DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
        new DeprecatedUTF8(path),
        toLogLong(timestamp)};
      new ArrayWritable(DeprecatedUTF8.class, info).write(out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
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
    
    static MkdirOp getInstance() {
      return (MkdirOp)opInstances.get()
        .get(OP_MKDIR);
    }

    MkdirOp setPath(String path) {
      this.path = path;
      return this;
    }

    MkdirOp setTimestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    MkdirOp setPermissionStatus(PermissionStatus permissions) {
      this.permissions = permissions;
      return this;
    }

    @Override 
    void writeFields(DataOutputStream out) throws IOException {
      DeprecatedUTF8 info[] = new DeprecatedUTF8[] {
        new DeprecatedUTF8(path),
        toLogLong(timestamp), // mtime
        toLogLong(timestamp) // atime, unused at this time
      };
      new ArrayWritable(DeprecatedUTF8.class, info).write(out);
      permissions.write(out);
    }
    
    @Override
    void readFields(DataInputStream in, int logVersion)
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

    static SetGenstampOp getInstance() {
      return (SetGenstampOp)opInstances.get()
        .get(OP_SET_GENSTAMP);
    }

    SetGenstampOp setGenerationStamp(long genStamp) {
      this.genStamp = genStamp;
      return this;
    }
    
    @Override 
    void writeFields(DataOutputStream out) throws IOException {
      new LongWritable(genStamp).write(out);
    }
    
    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.genStamp = in.readLong();
    }
  }

  @SuppressWarnings("deprecation")
  static class DatanodeAddOp extends FSEditLogOp {
    private DatanodeAddOp() {
      super(OP_DATANODE_ADD);
    }

    static DatanodeAddOp getInstance() {
      return (DatanodeAddOp)opInstances.get()
        .get(OP_DATANODE_ADD);
    }

    @Override 
    void writeFields(DataOutputStream out) throws IOException {
      throw new IOException("Deprecated, should not write");
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      //Datanodes are not persistent any more.
      FSImageSerialization.DatanodeImage.skipOne(in);
    }
  }

  @SuppressWarnings("deprecation")
  static class DatanodeRemoveOp extends FSEditLogOp {
    private DatanodeRemoveOp() {
      super(OP_DATANODE_REMOVE);
    }

    static DatanodeRemoveOp getInstance() {
      return (DatanodeRemoveOp)opInstances.get()
        .get(OP_DATANODE_REMOVE);
    }

    @Override 
    void writeFields(DataOutputStream out) throws IOException {
      throw new IOException("Deprecated, should not write");
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
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

    static SetPermissionsOp getInstance() {
      return (SetPermissionsOp)opInstances.get()
        .get(OP_SET_PERMISSIONS);
    }

    SetPermissionsOp setSource(String src) {
      this.src = src;
      return this;
    }

    SetPermissionsOp setPermissions(FsPermission permissions) {
      this.permissions = permissions;
      return this;
    }

    @Override 
    void writeFields(DataOutputStream out) throws IOException {
      new DeprecatedUTF8(src).write(out);
      permissions.write(out);
     }
 
    @Override
    void readFields(DataInputStream in, int logVersion)
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

    static SetOwnerOp getInstance() {
      return (SetOwnerOp)opInstances.get()
        .get(OP_SET_OWNER);
    }

    SetOwnerOp setSource(String src) {
      this.src = src;
      return this;
    }

    SetOwnerOp setUser(String username) {
      this.username = username;
      return this;
    }

    SetOwnerOp setGroup(String groupname) {
      this.groupname = groupname;
      return this;
    }

    @Override 
    void writeFields(DataOutputStream out) throws IOException {
      DeprecatedUTF8 u = new DeprecatedUTF8(username == null? "": username);
      DeprecatedUTF8 g = new DeprecatedUTF8(groupname == null? "": groupname);
      new DeprecatedUTF8(src).write(out);
      u.write(out);
      g.write(out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
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

    static SetNSQuotaOp getInstance() {
      return (SetNSQuotaOp)opInstances.get()
        .get(OP_SET_NS_QUOTA);
    }

    @Override 
    void writeFields(DataOutputStream out) throws IOException {
      throw new IOException("Deprecated");      
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
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

    static ClearNSQuotaOp getInstance() {
      return (ClearNSQuotaOp)opInstances.get()
        .get(OP_CLEAR_NS_QUOTA);
    }

    @Override 
    void writeFields(DataOutputStream out) throws IOException {
      throw new IOException("Deprecated");      
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
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

    static SetQuotaOp getInstance() {
      return (SetQuotaOp)opInstances.get()
        .get(OP_SET_QUOTA);
    }

    SetQuotaOp setSource(String src) {
      this.src = src;
      return this;
    }

    SetQuotaOp setNSQuota(long nsQuota) {
      this.nsQuota = nsQuota;
      return this;
    }

    SetQuotaOp setDSQuota(long dsQuota) {
      this.dsQuota = dsQuota;
      return this;
    }

    @Override 
    void writeFields(DataOutputStream out) throws IOException {
      new DeprecatedUTF8(src).write(out);
      new LongWritable(nsQuota).write(out);
      new LongWritable(dsQuota).write(out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
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

    static TimesOp getInstance() {
      return (TimesOp)opInstances.get()
        .get(OP_TIMES);
    }

    TimesOp setPath(String path) {
      this.path = path;
      return this;
    }

    TimesOp setModificationTime(long mtime) {
      this.mtime = mtime;
      return this;
    }

    TimesOp setAccessTime(long atime) {
      this.atime = atime;
      return this;
    }

    @Override 
    void writeFields(DataOutputStream out) throws IOException {
      DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
        new DeprecatedUTF8(path),
        toLogLong(mtime),
        toLogLong(atime)};
      new ArrayWritable(DeprecatedUTF8.class, info).write(out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
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

    static SymlinkOp getInstance() {
      return (SymlinkOp)opInstances.get()
        .get(OP_SYMLINK);
    }

    SymlinkOp setPath(String path) {
      this.path = path;
      return this;
    }

    SymlinkOp setValue(String value) {
      this.value = value;
      return this;
    }

    SymlinkOp setModificationTime(long mtime) {
      this.mtime = mtime;
      return this;
    }

    SymlinkOp setAccessTime(long atime) {
      this.atime = atime;
      return this;
    }

    SymlinkOp setPermissionStatus(PermissionStatus permissionStatus) {
      this.permissionStatus = permissionStatus;
      return this;
    }

    @Override 
    void writeFields(DataOutputStream out) throws IOException {
      DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
        new DeprecatedUTF8(path),
        new DeprecatedUTF8(value),
        toLogLong(mtime),
        toLogLong(atime)};
      new ArrayWritable(DeprecatedUTF8.class, info).write(out);
      permissionStatus.write(out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
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

    static RenameOp getInstance() {
      return (RenameOp)opInstances.get()
        .get(OP_RENAME);
    }

    RenameOp setSource(String src) {
      this.src = src;
      return this;
    }

    RenameOp setDestination(String dst) {
      this.dst = dst;
      return this;
    }
    
    RenameOp setTimestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }
    
    RenameOp setOptions(Rename[] options) {
      this.options = options;
      return this;
    }

    @Override 
    void writeFields(DataOutputStream out) throws IOException {
      DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
        new DeprecatedUTF8(src),
        new DeprecatedUTF8(dst),
        toLogLong(timestamp)};
      new ArrayWritable(DeprecatedUTF8.class, info).write(out);
      toBytesWritable(options).write(out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
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

    static BytesWritable toBytesWritable(Rename... options) {
      byte[] bytes = new byte[options.length];
      for (int i = 0; i < options.length; i++) {
        bytes[i] = options[i].value();
      }
      return new BytesWritable(bytes);
    }
  }

  static class ReassignLeaseOp extends FSEditLogOp {
    String leaseHolder;
    String path;
    String newHolder;

    private ReassignLeaseOp() {
      super(OP_REASSIGN_LEASE);
    }

    static ReassignLeaseOp getInstance() {
      return (ReassignLeaseOp)opInstances.get()
        .get(OP_REASSIGN_LEASE);
    }

    ReassignLeaseOp setLeaseHolder(String leaseHolder) {
      this.leaseHolder = leaseHolder;
      return this;
    }

    ReassignLeaseOp setPath(String path) {
      this.path = path;
      return this;
    }

    ReassignLeaseOp setNewHolder(String newHolder) {
      this.newHolder = newHolder;
      return this;
    }

    @Override 
    void writeFields(DataOutputStream out) throws IOException {
      new DeprecatedUTF8(leaseHolder).write(out);
      new DeprecatedUTF8(path).write(out);
      new DeprecatedUTF8(newHolder).write(out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
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

    static GetDelegationTokenOp getInstance() {
      return (GetDelegationTokenOp)opInstances.get()
        .get(OP_GET_DELEGATION_TOKEN);
    }

    GetDelegationTokenOp setDelegationTokenIdentifier(
        DelegationTokenIdentifier token) {
      this.token = token;
      return this;
    }

    GetDelegationTokenOp setExpiryTime(long expiryTime) {
      this.expiryTime = expiryTime;
      return this;
    }

    @Override 
    void writeFields(DataOutputStream out) throws IOException {
      token.write(out);
      toLogLong(expiryTime).write(out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
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

    static RenewDelegationTokenOp getInstance() {
      return (RenewDelegationTokenOp)opInstances.get()
          .get(OP_RENEW_DELEGATION_TOKEN);
    }

    RenewDelegationTokenOp setDelegationTokenIdentifier(
        DelegationTokenIdentifier token) {
      this.token = token;
      return this;
    }

    RenewDelegationTokenOp setExpiryTime(long expiryTime) {
      this.expiryTime = expiryTime;
      return this;
    }

    @Override 
    void writeFields(DataOutputStream out) throws IOException {
      token.write(out);
      toLogLong(expiryTime).write(out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
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

    static CancelDelegationTokenOp getInstance() {
      return (CancelDelegationTokenOp)opInstances.get()
          .get(OP_CANCEL_DELEGATION_TOKEN);
    }

    CancelDelegationTokenOp setDelegationTokenIdentifier(
        DelegationTokenIdentifier token) {
      this.token = token;
      return this;
    }

    @Override 
    void writeFields(DataOutputStream out) throws IOException {
      token.write(out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
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

    static UpdateMasterKeyOp getInstance() {
      return (UpdateMasterKeyOp)opInstances.get()
          .get(OP_UPDATE_MASTER_KEY);
    }

    UpdateMasterKeyOp setDelegationKey(DelegationKey key) {
      this.key = key;
      return this;
    }
    
    @Override 
    void writeFields(DataOutputStream out) throws IOException {
      key.write(out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
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

    static LogSegmentOp getInstance(FSEditLogOpCodes code) {
      return (LogSegmentOp)opInstances.get().get(code);
    }

    public void readFields(DataInputStream in, int logVersion)
        throws IOException {
      // no data stored in these ops yet
    }

    @Override
    void writeFields(DataOutputStream out) throws IOException {
      // no data stored
    }
  }

  static class InvalidOp extends FSEditLogOp {
    private InvalidOp() {
      super(OP_INVALID);
    }

    static InvalidOp getInstance() {
      return (InvalidOp)opInstances.get().get(OP_INVALID);
    }

    @Override 
    void writeFields(DataOutputStream out) throws IOException {
    }
    
    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      // nothing to read
    }
  }

  static private short readShort(DataInputStream in) throws IOException {
    return Short.parseShort(FSImageSerialization.readString(in));
  }

  static private long readLong(DataInputStream in) throws IOException {
    return Long.parseLong(FSImageSerialization.readString(in));
  }

  static private DeprecatedUTF8 toLogReplication(short replication) {
    return new DeprecatedUTF8(Short.toString(replication));
  }
  
  static private DeprecatedUTF8 toLogLong(long timestamp) {
    return new DeprecatedUTF8(Long.toString(timestamp));
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
   * Class for writing editlog ops
   */
  public static class Writer {
    private final DataOutputBuffer buf;
    private final Checksum checksum;

    public Writer(DataOutputBuffer out) {
      this.buf = out;
      this.checksum = new PureJavaCrc32();
    }

    /**
     * Write an operation to the output stream
     * 
     * @param op The operation to write
     * @throws IOException if an error occurs during writing.
     */
    public void writeOp(FSEditLogOp op) throws IOException {
      int start = buf.getLength();
      buf.writeByte(op.opCode.getOpCode());
      buf.writeLong(op.txid);
      op.writeFields(buf);
      int end = buf.getLength();
      checksum.reset();
      checksum.update(buf.getData(), start, end-start);
      int sum = (int)checksum.getValue();
      buf.writeInt(sum);
    }
  }

  /**
   * Class for reading editlog ops from a stream
   */
  public static class Reader {
    private final DataInputStream in;
    private final int logVersion;
    private final Checksum checksum;

    /**
     * Construct the reader
     * @param in The stream to read from.
     * @param logVersion The version of the data coming from the stream.
     */
    @SuppressWarnings("deprecation")
    public Reader(DataInputStream in, int logVersion) {
      this.logVersion = logVersion;
      if (LayoutVersion.supports(Feature.EDITS_CHESKUM, logVersion)) {
        this.checksum = new PureJavaCrc32();
      } else {
        this.checksum = null;
      }

      if (this.checksum != null) {
        this.in = new DataInputStream(
            new CheckedInputStream(in, this.checksum));
      } else {
        this.in = in;
      }
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

      FSEditLogOp op = opInstances.get().get(opCode);
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
