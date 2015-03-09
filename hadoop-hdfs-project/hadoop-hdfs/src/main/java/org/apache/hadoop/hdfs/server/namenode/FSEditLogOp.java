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

import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_ADD;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_APPEND;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_ADD_BLOCK;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_ADD_CACHE_DIRECTIVE;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_ADD_CACHE_POOL;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_ALLOCATE_BLOCK_ID;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_ALLOW_SNAPSHOT;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_CANCEL_DELEGATION_TOKEN;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_CLEAR_NS_QUOTA;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_CLOSE;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_CONCAT_DELETE;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_CREATE_SNAPSHOT;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_DELETE;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_DELETE_SNAPSHOT;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_DISALLOW_SNAPSHOT;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_END_LOG_SEGMENT;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_GET_DELEGATION_TOKEN;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_INVALID;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_MKDIR;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_MODIFY_CACHE_DIRECTIVE;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_MODIFY_CACHE_POOL;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_REASSIGN_LEASE;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_REMOVE_CACHE_DIRECTIVE;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_REMOVE_CACHE_POOL;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_REMOVE_XATTR;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_RENAME;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_RENAME_OLD;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_RENAME_SNAPSHOT;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_RENEW_DELEGATION_TOKEN;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_SET_ACL;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_ROLLING_UPGRADE_FINALIZE;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_ROLLING_UPGRADE_START;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_SET_GENSTAMP_V1;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_SET_GENSTAMP_V2;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_SET_NS_QUOTA;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_SET_OWNER;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_SET_PERMISSIONS;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_SET_QUOTA;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_SET_REPLICATION;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_SET_XATTR;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_START_LOG_SEGMENT;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_SYMLINK;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_TIMES;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_TRUNCATE;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_UPDATE_BLOCKS;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_UPDATE_MASTER_KEY;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_SET_STORAGE_POLICY;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_SET_QUOTA_BY_STORAGETYPE;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DeprecatedUTF8;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.AclEditLogProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.XAttrEditLogProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.util.XMLUtils;
import org.apache.hadoop.hdfs.util.XMLUtils.InvalidXmlException;
import org.apache.hadoop.hdfs.util.XMLUtils.Stanza;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.ipc.ClientId;
import org.apache.hadoop.ipc.RpcConstants;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * Helper classes for reading the ops from an InputStream.
 * All ops derive from FSEditLogOp and are only
 * instantiated from Reader#readOp()
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class FSEditLogOp {
  public final FSEditLogOpCodes opCode;
  long txid;
  byte[] rpcClientId;
  int rpcCallId;

  final void reset() {
    txid = HdfsConstants.INVALID_TXID;
    rpcClientId = RpcConstants.DUMMY_CLIENT_ID;
    rpcCallId = RpcConstants.INVALID_CALL_ID;
    resetSubFields();
  }

  abstract void resetSubFields();

  final public static class OpInstanceCache {
    private final EnumMap<FSEditLogOpCodes, FSEditLogOp> inst =
        new EnumMap<FSEditLogOpCodes, FSEditLogOp>(FSEditLogOpCodes.class);
    
    public OpInstanceCache() {
      inst.put(OP_ADD, new AddOp());
      inst.put(OP_CLOSE, new CloseOp());
      inst.put(OP_SET_REPLICATION, new SetReplicationOp());
      inst.put(OP_CONCAT_DELETE, new ConcatDeleteOp());
      inst.put(OP_RENAME_OLD, new RenameOldOp());
      inst.put(OP_DELETE, new DeleteOp());
      inst.put(OP_MKDIR, new MkdirOp());
      inst.put(OP_SET_GENSTAMP_V1, new SetGenstampV1Op());
      inst.put(OP_SET_PERMISSIONS, new SetPermissionsOp());
      inst.put(OP_SET_OWNER, new SetOwnerOp());
      inst.put(OP_SET_NS_QUOTA, new SetNSQuotaOp());
      inst.put(OP_CLEAR_NS_QUOTA, new ClearNSQuotaOp());
      inst.put(OP_SET_QUOTA, new SetQuotaOp());
      inst.put(OP_TIMES, new TimesOp());
      inst.put(OP_SYMLINK, new SymlinkOp());
      inst.put(OP_RENAME, new RenameOp());
      inst.put(OP_REASSIGN_LEASE, new ReassignLeaseOp());
      inst.put(OP_GET_DELEGATION_TOKEN, new GetDelegationTokenOp());
      inst.put(OP_RENEW_DELEGATION_TOKEN, new RenewDelegationTokenOp());
      inst.put(OP_CANCEL_DELEGATION_TOKEN, new CancelDelegationTokenOp());
      inst.put(OP_UPDATE_MASTER_KEY, new UpdateMasterKeyOp());
      inst.put(OP_START_LOG_SEGMENT, new LogSegmentOp(OP_START_LOG_SEGMENT));
      inst.put(OP_END_LOG_SEGMENT, new LogSegmentOp(OP_END_LOG_SEGMENT));
      inst.put(OP_UPDATE_BLOCKS, new UpdateBlocksOp());
      inst.put(OP_TRUNCATE, new TruncateOp());

      inst.put(OP_ALLOW_SNAPSHOT, new AllowSnapshotOp());
      inst.put(OP_DISALLOW_SNAPSHOT, new DisallowSnapshotOp());
      inst.put(OP_CREATE_SNAPSHOT, new CreateSnapshotOp());
      inst.put(OP_DELETE_SNAPSHOT, new DeleteSnapshotOp());
      inst.put(OP_RENAME_SNAPSHOT, new RenameSnapshotOp());
      inst.put(OP_SET_GENSTAMP_V2, new SetGenstampV2Op());
      inst.put(OP_ALLOCATE_BLOCK_ID, new AllocateBlockIdOp());
      inst.put(OP_ADD_BLOCK, new AddBlockOp());
      inst.put(OP_ADD_CACHE_DIRECTIVE,
          new AddCacheDirectiveInfoOp());
      inst.put(OP_MODIFY_CACHE_DIRECTIVE,
          new ModifyCacheDirectiveInfoOp());
      inst.put(OP_REMOVE_CACHE_DIRECTIVE,
          new RemoveCacheDirectiveInfoOp());
      inst.put(OP_ADD_CACHE_POOL, new AddCachePoolOp());
      inst.put(OP_MODIFY_CACHE_POOL, new ModifyCachePoolOp());
      inst.put(OP_REMOVE_CACHE_POOL, new RemoveCachePoolOp());

      inst.put(OP_SET_ACL, new SetAclOp());
      inst.put(OP_ROLLING_UPGRADE_START, new RollingUpgradeOp(
          OP_ROLLING_UPGRADE_START, "start"));
      inst.put(OP_ROLLING_UPGRADE_FINALIZE, new RollingUpgradeOp(
          OP_ROLLING_UPGRADE_FINALIZE, "finalize"));
      inst.put(OP_SET_XATTR, new SetXAttrOp());
      inst.put(OP_REMOVE_XATTR, new RemoveXAttrOp());
      inst.put(OP_SET_STORAGE_POLICY, new SetStoragePolicyOp());
      inst.put(OP_APPEND, new AppendOp());
      inst.put(OP_SET_QUOTA_BY_STORAGETYPE, new SetQuotaByStorageTypeOp());
    }
    
    public FSEditLogOp get(FSEditLogOpCodes opcode) {
      return inst.get(opcode);
    }
  }

  private static ImmutableMap<String, FsAction> fsActionMap() {
    ImmutableMap.Builder<String, FsAction> b = ImmutableMap.builder();
    for (FsAction v : FsAction.values())
      b.put(v.SYMBOL, v);
    return b.build();
  }

  private static final ImmutableMap<String, FsAction> FSACTION_SYMBOL_MAP
    = fsActionMap();

  /**
   * Constructor for an EditLog Op. EditLog ops cannot be constructed
   * directly, but only through Reader#readOp.
   */
  @VisibleForTesting
  protected FSEditLogOp(FSEditLogOpCodes opCode) {
    this.opCode = opCode;
    reset();
  }

  public long getTransactionId() {
    Preconditions.checkState(txid != HdfsConstants.INVALID_TXID);
    return txid;
  }

  public String getTransactionIdStr() {
    return (txid == HdfsConstants.INVALID_TXID) ? "(none)" : "" + txid;
  }
  
  public boolean hasTransactionId() {
    return (txid != HdfsConstants.INVALID_TXID);
  }

  public void setTransactionId(long txid) {
    this.txid = txid;
  }
  
  public boolean hasRpcIds() {
    return rpcClientId != RpcConstants.DUMMY_CLIENT_ID
        && rpcCallId != RpcConstants.INVALID_CALL_ID;
  }
  
  /** this has to be called after calling {@link #hasRpcIds()} */
  public byte[] getClientId() {
    Preconditions.checkState(rpcClientId != RpcConstants.DUMMY_CLIENT_ID);
    return rpcClientId;
  }
  
  public void setRpcClientId(byte[] clientId) {
    this.rpcClientId = clientId;
  }
  
  /** this has to be called after calling {@link #hasRpcIds()} */
  public int getCallId() {
    Preconditions.checkState(rpcCallId != RpcConstants.INVALID_CALL_ID);
    return rpcCallId;
  }
  
  public void setRpcCallId(int callId) {
    this.rpcCallId = callId;
  }

  abstract void readFields(DataInputStream in, int logVersion)
      throws IOException;

  public abstract void writeFields(DataOutputStream out)
      throws IOException;

  static interface BlockListUpdatingOp {
    Block[] getBlocks();
    String getPath();
    boolean shouldCompleteLastBlock();
  }
  
  private static void writeRpcIds(final byte[] clientId, final int callId,
      DataOutputStream out) throws IOException {
    FSImageSerialization.writeBytes(clientId, out);
    FSImageSerialization.writeInt(callId, out);
  }
  
  void readRpcIds(DataInputStream in, int logVersion)
      throws IOException {
    if (NameNodeLayoutVersion.supports(
        LayoutVersion.Feature.EDITLOG_SUPPORT_RETRYCACHE, logVersion)) {
      this.rpcClientId = FSImageSerialization.readBytes(in);
      this.rpcCallId = FSImageSerialization.readInt(in);
    }
  }
  
  void readRpcIdsFromXml(Stanza st) {
    this.rpcClientId = st.hasChildren("RPC_CLIENTID") ? 
        ClientId.toBytes(st.getValue("RPC_CLIENTID"))
        : RpcConstants.DUMMY_CLIENT_ID;
    this.rpcCallId = st.hasChildren("RPC_CALLID") ? 
        Integer.parseInt(st.getValue("RPC_CALLID"))
        : RpcConstants.INVALID_CALL_ID;
  }
  
  private static void appendRpcIdsToString(final StringBuilder builder,
      final byte[] clientId, final int callId) {
    builder.append(", RpcClientId=");
    builder.append(ClientId.toString(clientId));
    builder.append(", RpcCallId=");
    builder.append(callId);
  }
  
  private static void appendRpcIdsToXml(ContentHandler contentHandler,
      final byte[] clientId, final int callId) throws SAXException {
    XMLUtils.addSaxString(contentHandler, "RPC_CLIENTID",
        ClientId.toString(clientId));
    XMLUtils.addSaxString(contentHandler, "RPC_CALLID", 
        Integer.toString(callId));
  }

  private static final class AclEditLogUtil {
    private static final int ACL_EDITLOG_ENTRY_HAS_NAME_OFFSET = 6;
    private static final int ACL_EDITLOG_ENTRY_TYPE_OFFSET = 3;
    private static final int ACL_EDITLOG_ENTRY_SCOPE_OFFSET = 5;
    private static final int ACL_EDITLOG_PERM_MASK = 7;
    private static final int ACL_EDITLOG_ENTRY_TYPE_MASK = 3;
    private static final int ACL_EDITLOG_ENTRY_SCOPE_MASK = 1;

    private static final FsAction[] FSACTION_VALUES = FsAction.values();
    private static final AclEntryScope[] ACL_ENTRY_SCOPE_VALUES = AclEntryScope
        .values();
    private static final AclEntryType[] ACL_ENTRY_TYPE_VALUES = AclEntryType
        .values();

    private static List<AclEntry> read(DataInputStream in, int logVersion)
        throws IOException {
      if (!NameNodeLayoutVersion.supports(Feature.EXTENDED_ACL, logVersion)) {
        return null;
      }

      int size = in.readInt();
      if (size == 0) {
        return null;
      }

      List<AclEntry> aclEntries = Lists.newArrayListWithCapacity(size);
      for (int i = 0; i < size; ++i) {
        int v = in.read();
        int p = v & ACL_EDITLOG_PERM_MASK;
        int t = (v >> ACL_EDITLOG_ENTRY_TYPE_OFFSET)
            & ACL_EDITLOG_ENTRY_TYPE_MASK;
        int s = (v >> ACL_EDITLOG_ENTRY_SCOPE_OFFSET)
            & ACL_EDITLOG_ENTRY_SCOPE_MASK;
        boolean hasName = ((v >> ACL_EDITLOG_ENTRY_HAS_NAME_OFFSET) & 1) == 1;
        String name = hasName ? FSImageSerialization.readString(in) : null;
        aclEntries.add(new AclEntry.Builder().setName(name)
            .setPermission(FSACTION_VALUES[p])
            .setScope(ACL_ENTRY_SCOPE_VALUES[s])
            .setType(ACL_ENTRY_TYPE_VALUES[t]).build());
      }

      return aclEntries;
    }

    private static void write(List<AclEntry> aclEntries, DataOutputStream out)
        throws IOException {
      if (aclEntries == null) {
        out.writeInt(0);
        return;
      }

      out.writeInt(aclEntries.size());
      for (AclEntry e : aclEntries) {
        boolean hasName = e.getName() != null;
        int v = (e.getScope().ordinal() << ACL_EDITLOG_ENTRY_SCOPE_OFFSET)
            | (e.getType().ordinal() << ACL_EDITLOG_ENTRY_TYPE_OFFSET)
            | e.getPermission().ordinal();

        if (hasName) {
          v |= 1 << ACL_EDITLOG_ENTRY_HAS_NAME_OFFSET;
        }
        out.write(v);
        if (hasName) {
          FSImageSerialization.writeString(e.getName(), out);
        }
      }
    }
  }

  private static List<XAttr> readXAttrsFromEditLog(DataInputStream in,
      int logVersion) throws IOException {
    if (!NameNodeLayoutVersion.supports(NameNodeLayoutVersion.Feature.XATTRS,
        logVersion)) {
      return null;
    }
    XAttrEditLogProto proto = XAttrEditLogProto.parseDelimitedFrom(in);
    return PBHelper.convertXAttrs(proto.getXAttrsList());
  }

  @SuppressWarnings("unchecked")
  static abstract class AddCloseOp extends FSEditLogOp implements BlockListUpdatingOp {
    int length;
    long inodeId;
    String path;
    short replication;
    long mtime;
    long atime;
    long blockSize;
    Block[] blocks;
    PermissionStatus permissions;
    List<AclEntry> aclEntries;
    List<XAttr> xAttrs;
    String clientName;
    String clientMachine;
    boolean overwrite;
    byte storagePolicyId;
    
    private AddCloseOp(FSEditLogOpCodes opCode) {
      super(opCode);
      storagePolicyId = BlockStoragePolicySuite.ID_UNSPECIFIED;
      assert(opCode == OP_ADD || opCode == OP_CLOSE || opCode == OP_APPEND);
    }

    @Override
    void resetSubFields() {
      length = 0;
      inodeId = 0L;
      path = null;
      replication = 0;
      mtime = 0L;
      atime = 0L;
      blockSize = 0L;
      blocks = null;
      permissions = null;
      aclEntries = null;
      xAttrs = null;
      clientName = null;
      clientMachine = null;
      overwrite = false;
      storagePolicyId = 0;
    }

    <T extends AddCloseOp> T setInodeId(long inodeId) {
      this.inodeId = inodeId;
      return (T)this;
    }

    <T extends AddCloseOp> T setPath(String path) {
      this.path = path;
      return (T)this;
    }
    
    @Override
    public String getPath() {
      return path;
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
      if (blocks.length > MAX_BLOCKS) {
        throw new RuntimeException("Can't have more than " + MAX_BLOCKS +
            " in an AddCloseOp.");
      }
      this.blocks = blocks;
      return (T)this;
    }
    
    @Override
    public Block[] getBlocks() {
      return blocks;
    }

    <T extends AddCloseOp> T setPermissionStatus(PermissionStatus permissions) {
      this.permissions = permissions;
      return (T)this;
    }

    <T extends AddCloseOp> T setAclEntries(List<AclEntry> aclEntries) {
      this.aclEntries = aclEntries;
      return (T)this;
    }

    <T extends AddCloseOp> T setXAttrs(List<XAttr> xAttrs) {
      this.xAttrs = xAttrs;
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
    
    <T extends AddCloseOp> T setOverwrite(boolean overwrite) {
      this.overwrite = overwrite;
      return (T)this;
    }

    <T extends AddCloseOp> T setStoragePolicyId(byte storagePolicyId) {
      this.storagePolicyId = storagePolicyId;
      return (T)this;
    }

    @Override
    public void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeLong(inodeId, out);
      FSImageSerialization.writeString(path, out);
      FSImageSerialization.writeShort(replication, out);
      FSImageSerialization.writeLong(mtime, out);
      FSImageSerialization.writeLong(atime, out);
      FSImageSerialization.writeLong(blockSize, out);
      new ArrayWritable(Block.class, blocks).write(out);
      permissions.write(out);

      if (this.opCode == OP_ADD) {
        AclEditLogUtil.write(aclEntries, out);
        XAttrEditLogProto.Builder b = XAttrEditLogProto.newBuilder();
        b.addAllXAttrs(PBHelper.convertXAttrProto(xAttrs));
        b.build().writeDelimitedTo(out);
        FSImageSerialization.writeString(clientName,out);
        FSImageSerialization.writeString(clientMachine,out);
        FSImageSerialization.writeBoolean(overwrite, out);
        FSImageSerialization.writeByte(storagePolicyId, out);
        // write clientId and callId
        writeRpcIds(rpcClientId, rpcCallId, out);
      }
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      if (!NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.length = in.readInt();
      }
      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.ADD_INODE_ID, logVersion)) {
        this.inodeId = in.readLong();
      } else {
        // The inodeId should be updated when this editLogOp is applied
        this.inodeId = INodeId.GRANDFATHER_INODE_ID;
      }
      if ((-17 < logVersion && length != 4) ||
          (logVersion <= -17 && length != 5 && !NameNodeLayoutVersion.supports(
              LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion))) {
        throw new IOException("Incorrect data format."  +
                              " logVersion is " + logVersion +
                              " but writables.length is " +
                              length + ". ");
      }
      this.path = FSImageSerialization.readString(in);

      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.replication = FSImageSerialization.readShort(in);
        this.mtime = FSImageSerialization.readLong(in);
      } else {
        this.replication = readShort(in);
        this.mtime = readLong(in);
      }

      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.FILE_ACCESS_TIME, logVersion)) {
        if (NameNodeLayoutVersion.supports(
            LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
          this.atime = FSImageSerialization.readLong(in);
        } else {
          this.atime = readLong(in);
        }
      } else {
        this.atime = 0;
      }

      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.blockSize = FSImageSerialization.readLong(in);
      } else {
        this.blockSize = readLong(in);
      }

      this.blocks = readBlocks(in, logVersion);
      this.permissions = PermissionStatus.read(in);

      if (this.opCode == OP_ADD) {
        aclEntries = AclEditLogUtil.read(in, logVersion);
        this.xAttrs = readXAttrsFromEditLog(in, logVersion);
        this.clientName = FSImageSerialization.readString(in);
        this.clientMachine = FSImageSerialization.readString(in);
        if (NameNodeLayoutVersion.supports(
            NameNodeLayoutVersion.Feature.CREATE_OVERWRITE, logVersion)) {
          this.overwrite = FSImageSerialization.readBoolean(in);
        } else {
          this.overwrite = false;
        }
        if (NameNodeLayoutVersion.supports(
            NameNodeLayoutVersion.Feature.BLOCK_STORAGE_POLICY, logVersion)) {
          this.storagePolicyId = FSImageSerialization.readByte(in);
        } else {
          this.storagePolicyId = BlockStoragePolicySuite.ID_UNSPECIFIED;
        }
        // read clientId and callId
        readRpcIds(in, logVersion);
      } else {
        this.clientName = "";
        this.clientMachine = "";
      }
    }

    static final public int MAX_BLOCKS = 1024 * 1024 * 64;
    
    private static Block[] readBlocks(
        DataInputStream in,
        int logVersion) throws IOException {
      int numBlocks = in.readInt();
      if (numBlocks < 0) {
        throw new IOException("invalid negative number of blocks");
      } else if (numBlocks > MAX_BLOCKS) {
        throw new IOException("invalid number of blocks: " + numBlocks +
            ".  The maximum number of blocks per file is " + MAX_BLOCKS);
      }
      Block[] blocks = new Block[numBlocks];
      for (int i = 0; i < numBlocks; i++) {
        Block blk = new Block();
        blk.readFields(in);
        blocks[i] = blk;
      }
      return blocks;
    }

    public String stringifyMembers() {
      StringBuilder builder = new StringBuilder();
      builder.append("[length=");
      builder.append(length);
      builder.append(", inodeId=");
      builder.append(inodeId);
      builder.append(", path=");
      builder.append(path);
      builder.append(", replication=");
      builder.append(replication);
      builder.append(", mtime=");
      builder.append(mtime);
      builder.append(", atime=");
      builder.append(atime);
      builder.append(", blockSize=");
      builder.append(blockSize);
      builder.append(", blocks=");
      builder.append(Arrays.toString(blocks));
      builder.append(", permissions=");
      builder.append(permissions);
      builder.append(", aclEntries=");
      builder.append(aclEntries);
      builder.append(", clientName=");
      builder.append(clientName);
      builder.append(", clientMachine=");
      builder.append(clientMachine);
      builder.append(", overwrite=");
      builder.append(overwrite);
      if (this.opCode == OP_ADD) {
        appendRpcIdsToString(builder, rpcClientId, rpcCallId);
      }
      builder.append(", storagePolicyId=");
      builder.append(storagePolicyId);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }
    
    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "LENGTH",
          Integer.toString(length));
      XMLUtils.addSaxString(contentHandler, "INODEID",
          Long.toString(inodeId));
      XMLUtils.addSaxString(contentHandler, "PATH", path);
      XMLUtils.addSaxString(contentHandler, "REPLICATION",
          Short.valueOf(replication).toString());
      XMLUtils.addSaxString(contentHandler, "MTIME",
          Long.toString(mtime));
      XMLUtils.addSaxString(contentHandler, "ATIME",
          Long.toString(atime));
      XMLUtils.addSaxString(contentHandler, "BLOCKSIZE",
          Long.toString(blockSize));
      XMLUtils.addSaxString(contentHandler, "CLIENT_NAME", clientName);
      XMLUtils.addSaxString(contentHandler, "CLIENT_MACHINE", clientMachine);
      XMLUtils.addSaxString(contentHandler, "OVERWRITE", 
          Boolean.toString(overwrite));
      for (Block b : blocks) {
        FSEditLogOp.blockToXml(contentHandler, b);
      }
      FSEditLogOp.permissionStatusToXml(contentHandler, permissions);
      if (this.opCode == OP_ADD) {
        if (aclEntries != null) {
          appendAclEntriesToXml(contentHandler, aclEntries);
        }
        appendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
      }
    }

    @Override 
    void fromXml(Stanza st) throws InvalidXmlException {
      this.length = Integer.parseInt(st.getValue("LENGTH"));
      this.inodeId = Long.parseLong(st.getValue("INODEID"));
      this.path = st.getValue("PATH");
      this.replication = Short.valueOf(st.getValue("REPLICATION"));
      this.mtime = Long.parseLong(st.getValue("MTIME"));
      this.atime = Long.parseLong(st.getValue("ATIME"));
      this.blockSize = Long.parseLong(st.getValue("BLOCKSIZE"));

      this.clientName = st.getValue("CLIENT_NAME");
      this.clientMachine = st.getValue("CLIENT_MACHINE");
      this.overwrite = Boolean.parseBoolean(st.getValueOrNull("OVERWRITE"));
      if (st.hasChildren("BLOCK")) {
        List<Stanza> blocks = st.getChildren("BLOCK");
        this.blocks = new Block[blocks.size()];
        for (int i = 0; i < blocks.size(); i++) {
          this.blocks[i] = FSEditLogOp.blockFromXml(blocks.get(i));
        }
      } else {
        this.blocks = new Block[0];
      }
      this.permissions = permissionStatusFromXml(st);
      aclEntries = readAclEntriesFromXml(st);
      readRpcIdsFromXml(st);
    }
  }

  /**
   * {@literal @AtMostOnce} for {@link ClientProtocol#create} and
   * {@link ClientProtocol#append}
   */
  static class AddOp extends AddCloseOp {
    private AddOp() {
      super(OP_ADD);
    }

    static AddOp getInstance(OpInstanceCache cache) {
      return (AddOp) cache.get(OP_ADD);
    }

    @Override
    public boolean shouldCompleteLastBlock() {
      return false;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("AddOp ");
      builder.append(stringifyMembers());
      return builder.toString();
    }
  }

  /**
   * Although {@link ClientProtocol#append} may also log a close op, we do
   * not need to record the rpc ids here since a successful appendFile op will
   * finally log an AddOp.
   */
  static class CloseOp extends AddCloseOp {
    private CloseOp() {
      super(OP_CLOSE);
    }

    static CloseOp getInstance(OpInstanceCache cache) {
      return (CloseOp)cache.get(OP_CLOSE);
    }

    @Override
    public boolean shouldCompleteLastBlock() {
      return true;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("CloseOp ");
      builder.append(stringifyMembers());
      return builder.toString();
    }
  }

  static class AppendOp extends FSEditLogOp {
    String path;
    String clientName;
    String clientMachine;
    boolean newBlock;

    private AppendOp() {
      super(OP_APPEND);
    }

    static AppendOp getInstance(OpInstanceCache cache) {
      return (AppendOp) cache.get(OP_APPEND);
    }

    AppendOp setPath(String path) {
      this.path = path;
      return this;
    }

    AppendOp setClientName(String clientName) {
      this.clientName = clientName;
      return this;
    }

    AppendOp setClientMachine(String clientMachine) {
      this.clientMachine = clientMachine;
      return this;
    }

    AppendOp setNewBlock(boolean newBlock) {
      this.newBlock = newBlock;
      return this;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("AppendOp ");
      builder.append("[path=").append(path);
      builder.append(", clientName=").append(clientName);
      builder.append(", clientMachine=").append(clientMachine);
      builder.append(", newBlock=").append(newBlock).append("]");
      return builder.toString();
    }

    @Override
    void resetSubFields() {
      this.path = null;
      this.clientName = null;
      this.clientMachine = null;
      this.newBlock = false;
    }

    @Override
    void readFields(DataInputStream in, int logVersion) throws IOException {
      this.path = FSImageSerialization.readString(in);
      this.clientName = FSImageSerialization.readString(in);
      this.clientMachine = FSImageSerialization.readString(in);
      this.newBlock = FSImageSerialization.readBoolean(in);
      readRpcIds(in, logVersion);
    }

    @Override
    public void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(path, out);
      FSImageSerialization.writeString(clientName, out);
      FSImageSerialization.writeString(clientMachine, out);
      FSImageSerialization.writeBoolean(newBlock, out);
      writeRpcIds(rpcClientId, rpcCallId, out);
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "PATH", path);
      XMLUtils.addSaxString(contentHandler, "CLIENT_NAME", clientName);
      XMLUtils.addSaxString(contentHandler, "CLIENT_MACHINE", clientMachine);
      XMLUtils.addSaxString(contentHandler, "NEWBLOCK",
          Boolean.toString(newBlock));
      appendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
    }

    @Override
    void fromXml(Stanza st) throws InvalidXmlException {
      this.path = st.getValue("PATH");
      this.clientName = st.getValue("CLIENT_NAME");
      this.clientMachine = st.getValue("CLIENT_MACHINE");
      this.newBlock = Boolean.parseBoolean(st.getValue("NEWBLOCK"));
      readRpcIdsFromXml(st);
    }
  }
  
  static class AddBlockOp extends FSEditLogOp {
    private String path;
    private Block penultimateBlock;
    private Block lastBlock;
    
    private AddBlockOp() {
      super(OP_ADD_BLOCK);
    }
    
    static AddBlockOp getInstance(OpInstanceCache cache) {
      return (AddBlockOp) cache.get(OP_ADD_BLOCK);
    }

    @Override
    void resetSubFields() {
      path = null;
      penultimateBlock = null;
      lastBlock = null;
    }
    
    AddBlockOp setPath(String path) {
      this.path = path;
      return this;
    }
    
    public String getPath() {
      return path;
    }

    AddBlockOp setPenultimateBlock(Block pBlock) {
      this.penultimateBlock = pBlock;
      return this;
    }
    
    Block getPenultimateBlock() {
      return penultimateBlock;
    }
    
    AddBlockOp setLastBlock(Block lastBlock) {
      this.lastBlock = lastBlock;
      return this;
    }
    
    Block getLastBlock() {
      return lastBlock;
    }

    @Override
    public void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(path, out);
      int size = penultimateBlock != null ? 2 : 1;
      Block[] blocks = new Block[size];
      if (penultimateBlock != null) {
        blocks[0] = penultimateBlock;
      }
      blocks[size - 1] = lastBlock;
      FSImageSerialization.writeCompactBlockArray(blocks, out);
      // clientId and callId
      writeRpcIds(rpcClientId, rpcCallId, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion) throws IOException {
      path = FSImageSerialization.readString(in);
      Block[] blocks = FSImageSerialization.readCompactBlockArray(in,
          logVersion);
      Preconditions.checkState(blocks.length == 2 || blocks.length == 1);
      penultimateBlock = blocks.length == 1 ? null : blocks[0];
      lastBlock = blocks[blocks.length - 1];
      readRpcIds(in, logVersion);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("AddBlockOp [path=")
        .append(path)
        .append(", penultimateBlock=")
        .append(penultimateBlock == null ? "NULL" : penultimateBlock)
        .append(", lastBlock=")
        .append(lastBlock);
      appendRpcIdsToString(sb, rpcClientId, rpcCallId);
      sb.append("]");
      return sb.toString();
    }
    
    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "PATH", path);
      if (penultimateBlock != null) {
        FSEditLogOp.blockToXml(contentHandler, penultimateBlock);
      }
      FSEditLogOp.blockToXml(contentHandler, lastBlock);
      appendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
    }
    
    @Override 
    void fromXml(Stanza st) throws InvalidXmlException {
      this.path = st.getValue("PATH");
      List<Stanza> blocks = st.getChildren("BLOCK");
      int size = blocks.size();
      Preconditions.checkState(size == 1 || size == 2);
      this.penultimateBlock = size == 2 ? 
          FSEditLogOp.blockFromXml(blocks.get(0)) : null;
      this.lastBlock = FSEditLogOp.blockFromXml(blocks.get(size - 1));
      readRpcIdsFromXml(st);
    }
  }
  
  /**
   * {@literal @AtMostOnce} for {@link ClientProtocol#updatePipeline}, but 
   * {@literal @Idempotent} for some other ops.
   */
  static class UpdateBlocksOp extends FSEditLogOp implements BlockListUpdatingOp {
    String path;
    Block[] blocks;
    
    private UpdateBlocksOp() {
      super(OP_UPDATE_BLOCKS);
    }
    
    static UpdateBlocksOp getInstance(OpInstanceCache cache) {
      return (UpdateBlocksOp)cache.get(OP_UPDATE_BLOCKS);
    }

    @Override
    void resetSubFields() {
      path = null;
      blocks = null;
    }
    
    UpdateBlocksOp setPath(String path) {
      this.path = path;
      return this;
    }
    
    @Override
    public String getPath() {
      return path;
    }

    UpdateBlocksOp setBlocks(Block[] blocks) {
      this.blocks = blocks;
      return this;
    }
    
    @Override
    public Block[] getBlocks() {
      return blocks;
    }

    @Override
    public
    void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(path, out);
      FSImageSerialization.writeCompactBlockArray(blocks, out);
      // clientId and callId
      writeRpcIds(rpcClientId, rpcCallId, out);
    }
    
    @Override
    void readFields(DataInputStream in, int logVersion) throws IOException {
      path = FSImageSerialization.readString(in);
      this.blocks = FSImageSerialization.readCompactBlockArray(
          in, logVersion);
      readRpcIds(in, logVersion);
    }

    @Override
    public boolean shouldCompleteLastBlock() {
      return false;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("UpdateBlocksOp [path=")
        .append(path)
        .append(", blocks=")
        .append(Arrays.toString(blocks));
      appendRpcIdsToString(sb, rpcClientId, rpcCallId);
      sb.append("]");
      return sb.toString();
    }
    
    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "PATH", path);
      for (Block b : blocks) {
        FSEditLogOp.blockToXml(contentHandler, b);
      }
      appendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.path = st.getValue("PATH");
      List<Stanza> blocks = st.getChildren("BLOCK");
      this.blocks = new Block[blocks.size()];
      for (int i = 0; i < blocks.size(); i++) {
        this.blocks[i] = FSEditLogOp.blockFromXml(blocks.get(i));
      }
      readRpcIdsFromXml(st);
    }
  }

  /** {@literal @Idempotent} for {@link ClientProtocol#setReplication} */
  static class SetReplicationOp extends FSEditLogOp {
    String path;
    short replication;

    private SetReplicationOp() {
      super(OP_SET_REPLICATION);
    }

    static SetReplicationOp getInstance(OpInstanceCache cache) {
      return (SetReplicationOp)cache.get(OP_SET_REPLICATION);
    }

    @Override
    void resetSubFields() {
      path = null;
      replication = 0;
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
    public 
    void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(path, out);
      FSImageSerialization.writeShort(replication, out);
    }
    
    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.path = FSImageSerialization.readString(in);
      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.replication = FSImageSerialization.readShort(in);
      } else {
        this.replication = readShort(in);
      }
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("SetReplicationOp [path=");
      builder.append(path);
      builder.append(", replication=");
      builder.append(replication);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }
    
    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "PATH", path);
      XMLUtils.addSaxString(contentHandler, "REPLICATION",
          Short.valueOf(replication).toString());
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.path = st.getValue("PATH");
      this.replication = Short.valueOf(st.getValue("REPLICATION"));
    }
  }

  /** {@literal @AtMostOnce} for {@link ClientProtocol#concat} */
  static class ConcatDeleteOp extends FSEditLogOp {
    int length;
    String trg;
    String[] srcs;
    long timestamp;
    final static public int MAX_CONCAT_SRC = 1024 * 1024;

    private ConcatDeleteOp() {
      super(OP_CONCAT_DELETE);
    }

    static ConcatDeleteOp getInstance(OpInstanceCache cache) {
      return (ConcatDeleteOp)cache.get(OP_CONCAT_DELETE);
    }

    @Override
    void resetSubFields() {
      length = 0;
      trg = null;
      srcs = null;
      timestamp = 0L;
    }

    ConcatDeleteOp setTarget(String trg) {
      this.trg = trg;
      return this;
    }

    ConcatDeleteOp setSources(String[] srcs) {
      if (srcs.length > MAX_CONCAT_SRC) {
        throw new RuntimeException("ConcatDeleteOp can only have " +
            MAX_CONCAT_SRC + " sources at most.");
      }
      this.srcs = srcs;

      return this;
    }

    ConcatDeleteOp setTimestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    @Override
    public void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(trg, out);
            
      DeprecatedUTF8 info[] = new DeprecatedUTF8[srcs.length];
      int idx = 0;
      for(int i=0; i<srcs.length; i++) {
        info[idx++] = new DeprecatedUTF8(srcs[i]);
      }
      new ArrayWritable(DeprecatedUTF8.class, info).write(out);

      FSImageSerialization.writeLong(timestamp, out);
      
      // rpc ids
      writeRpcIds(rpcClientId, rpcCallId, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      if (!NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.length = in.readInt();
        if (length < 3) { // trg, srcs.., timestamp
          throw new IOException("Incorrect data format " +
              "for ConcatDeleteOp.");
        }
      }
      this.trg = FSImageSerialization.readString(in);
      int srcSize = 0;
      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        srcSize = in.readInt();
      } else {
        srcSize = this.length - 1 - 1; // trg and timestamp
      }
      if (srcSize < 0) {
          throw new IOException("Incorrect data format. "
              + "ConcatDeleteOp cannot have a negative number of data " +
              " sources.");
      } else if (srcSize > MAX_CONCAT_SRC) {
          throw new IOException("Incorrect data format. "
              + "ConcatDeleteOp can have at most " + MAX_CONCAT_SRC +
              " sources, but we tried to have " + (length - 3) + " sources.");
      }
      this.srcs = new String [srcSize];
      for(int i=0; i<srcSize;i++) {
        srcs[i]= FSImageSerialization.readString(in);
      }
      
      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.timestamp = FSImageSerialization.readLong(in);
      } else {
        this.timestamp = readLong(in);
      }
      // read RPC ids if necessary
      readRpcIds(in, logVersion);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("ConcatDeleteOp [length=");
      builder.append(length);
      builder.append(", trg=");
      builder.append(trg);
      builder.append(", srcs=");
      builder.append(Arrays.toString(srcs));
      builder.append(", timestamp=");
      builder.append(timestamp);
      appendRpcIdsToString(builder, rpcClientId, rpcCallId);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }
    
    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "LENGTH",
          Integer.toString(length));
      XMLUtils.addSaxString(contentHandler, "TRG", trg);
      XMLUtils.addSaxString(contentHandler, "TIMESTAMP",
          Long.toString(timestamp));
      contentHandler.startElement("", "", "SOURCES", new AttributesImpl());
      for (int i = 0; i < srcs.length; ++i) {
        XMLUtils.addSaxString(contentHandler,
            "SOURCE" + (i + 1), srcs[i]);
      }
      contentHandler.endElement("", "", "SOURCES");
      appendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.length = Integer.parseInt(st.getValue("LENGTH"));
      this.trg = st.getValue("TRG");
      this.timestamp = Long.parseLong(st.getValue("TIMESTAMP"));
      List<Stanza> sources = st.getChildren("SOURCES");
      int i = 0;
      while (true) {
        if (!sources.get(0).hasChildren("SOURCE" + (i + 1)))
          break;
        i++;
      }
      srcs = new String[i];
      for (i = 0; i < srcs.length; i++) {
        srcs[i] = sources.get(0).getValue("SOURCE" + (i + 1));
      }
      readRpcIdsFromXml(st);
    }
  }

  /** {@literal @AtMostOnce} for {@link ClientProtocol#rename} */
  static class RenameOldOp extends FSEditLogOp {
    int length;
    String src;
    String dst;
    long timestamp;

    private RenameOldOp() {
      super(OP_RENAME_OLD);
    }

    static RenameOldOp getInstance(OpInstanceCache cache) {
      return (RenameOldOp)cache.get(OP_RENAME_OLD);
    }

    @Override
    void resetSubFields() {
      length = 0;
      src = null;
      dst = null;
      timestamp = 0L;
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
    public 
    void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(src, out);
      FSImageSerialization.writeString(dst, out);
      FSImageSerialization.writeLong(timestamp, out);
      writeRpcIds(rpcClientId, rpcCallId, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      if (!NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.length = in.readInt();
        if (this.length != 3) {
          throw new IOException("Incorrect data format. "
              + "Old rename operation.");
        }
      }
      this.src = FSImageSerialization.readString(in);
      this.dst = FSImageSerialization.readString(in);
      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.timestamp = FSImageSerialization.readLong(in);
      } else {
        this.timestamp = readLong(in);
      }
      
      // read RPC ids if necessary
      readRpcIds(in, logVersion);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("RenameOldOp [length=");
      builder.append(length);
      builder.append(", src=");
      builder.append(src);
      builder.append(", dst=");
      builder.append(dst);
      builder.append(", timestamp=");
      builder.append(timestamp);
      appendRpcIdsToString(builder, rpcClientId, rpcCallId);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }
    
    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "LENGTH",
          Integer.toString(length));
      XMLUtils.addSaxString(contentHandler, "SRC", src);
      XMLUtils.addSaxString(contentHandler, "DST", dst);
      XMLUtils.addSaxString(contentHandler, "TIMESTAMP",
          Long.toString(timestamp));
      appendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
    }
    
    @Override 
    void fromXml(Stanza st) throws InvalidXmlException {
      this.length = Integer.parseInt(st.getValue("LENGTH"));
      this.src = st.getValue("SRC");
      this.dst = st.getValue("DST");
      this.timestamp = Long.parseLong(st.getValue("TIMESTAMP"));
      
      readRpcIdsFromXml(st);
    }
  }

  /** {@literal @AtMostOnce} for {@link ClientProtocol#delete} */
  static class DeleteOp extends FSEditLogOp {
    int length;
    String path;
    long timestamp;

    private DeleteOp() {
      super(OP_DELETE);
    }

    static DeleteOp getInstance(OpInstanceCache cache) {
      return (DeleteOp)cache.get(OP_DELETE);
    }

    @Override
    void resetSubFields() {
      length = 0;
      path = null;
      timestamp = 0L;
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
    public 
    void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(path, out);
      FSImageSerialization.writeLong(timestamp, out);
      writeRpcIds(rpcClientId, rpcCallId, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      if (!NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.length = in.readInt();
        if (this.length != 2) {
          throw new IOException("Incorrect data format. " + "delete operation.");
        }
      }
      this.path = FSImageSerialization.readString(in);
      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.timestamp = FSImageSerialization.readLong(in);
      } else {
        this.timestamp = readLong(in);
      }
      // read RPC ids if necessary
      readRpcIds(in, logVersion);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("DeleteOp [length=");
      builder.append(length);
      builder.append(", path=");
      builder.append(path);
      builder.append(", timestamp=");
      builder.append(timestamp);
      appendRpcIdsToString(builder, rpcClientId, rpcCallId);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }
    
    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "LENGTH",
          Integer.toString(length));
      XMLUtils.addSaxString(contentHandler, "PATH", path);
      XMLUtils.addSaxString(contentHandler, "TIMESTAMP",
          Long.toString(timestamp));
      appendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.length = Integer.parseInt(st.getValue("LENGTH"));
      this.path = st.getValue("PATH");
      this.timestamp = Long.parseLong(st.getValue("TIMESTAMP"));
      
      readRpcIdsFromXml(st);
    }
  }

  /** {@literal @Idempotent} for {@link ClientProtocol#mkdirs} */
  static class MkdirOp extends FSEditLogOp {
    int length;
    long inodeId;
    String path;
    long timestamp;
    PermissionStatus permissions;
    List<AclEntry> aclEntries;
    List<XAttr> xAttrs;

    private MkdirOp() {
      super(OP_MKDIR);
    }
    
    static MkdirOp getInstance(OpInstanceCache cache) {
      return (MkdirOp)cache.get(OP_MKDIR);
    }

    @Override
    void resetSubFields() {
      length = 0;
      inodeId = 0L;
      path = null;
      timestamp = 0L;
      permissions = null;
      aclEntries = null;
      xAttrs = null;
    }

    MkdirOp setInodeId(long inodeId) {
      this.inodeId = inodeId;
      return this;
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

    MkdirOp setAclEntries(List<AclEntry> aclEntries) {
      this.aclEntries = aclEntries;
      return this;
    }

    MkdirOp setXAttrs(List<XAttr> xAttrs) {
      this.xAttrs = xAttrs;
      return this;
    }

    @Override
    public 
    void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeLong(inodeId, out);
      FSImageSerialization.writeString(path, out);
      FSImageSerialization.writeLong(timestamp, out); // mtime
      FSImageSerialization.writeLong(timestamp, out); // atime, unused at this
      permissions.write(out);
      AclEditLogUtil.write(aclEntries, out);
      XAttrEditLogProto.Builder b = XAttrEditLogProto.newBuilder();
      b.addAllXAttrs(PBHelper.convertXAttrProto(xAttrs));
      b.build().writeDelimitedTo(out);
    }
    
    @Override
    void readFields(DataInputStream in, int logVersion) throws IOException {
      if (!NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.length = in.readInt();
      }
      if (-17 < logVersion && length != 2 ||
          logVersion <= -17 && length != 3
          && !NameNodeLayoutVersion.supports(
              LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        throw new IOException("Incorrect data format. Mkdir operation.");
      }
      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.ADD_INODE_ID, logVersion)) {
        this.inodeId = FSImageSerialization.readLong(in);
      } else {
        // This id should be updated when this editLogOp is applied
        this.inodeId = INodeId.GRANDFATHER_INODE_ID;
      }
      this.path = FSImageSerialization.readString(in);
      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.timestamp = FSImageSerialization.readLong(in);
      } else {
        this.timestamp = readLong(in);
      }

      // The disk format stores atimes for directories as well.
      // However, currently this is not being updated/used because of
      // performance reasons.
      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.FILE_ACCESS_TIME, logVersion)) {
        if (NameNodeLayoutVersion.supports(
            LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
          FSImageSerialization.readLong(in);
        } else {
          readLong(in);
        }
      }

      this.permissions = PermissionStatus.read(in);
      aclEntries = AclEditLogUtil.read(in, logVersion);

      xAttrs = readXAttrsFromEditLog(in, logVersion);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("MkdirOp [length=");
      builder.append(length);
      builder.append(", inodeId=");
      builder.append(inodeId);
      builder.append(", path=");
      builder.append(path);
      builder.append(", timestamp=");
      builder.append(timestamp);
      builder.append(", permissions=");
      builder.append(permissions);
      builder.append(", aclEntries=");
      builder.append(aclEntries);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append(", xAttrs=");
      builder.append(xAttrs);
      builder.append("]");
      return builder.toString();
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "LENGTH",
          Integer.toString(length));
      XMLUtils.addSaxString(contentHandler, "INODEID",
          Long.toString(inodeId));
      XMLUtils.addSaxString(contentHandler, "PATH", path);
      XMLUtils.addSaxString(contentHandler, "TIMESTAMP",
          Long.toString(timestamp));
      FSEditLogOp.permissionStatusToXml(contentHandler, permissions);
      if (aclEntries != null) {
        appendAclEntriesToXml(contentHandler, aclEntries);
      }
      if (xAttrs != null) {
        appendXAttrsToXml(contentHandler, xAttrs);
      }
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.length = Integer.parseInt(st.getValue("LENGTH"));
      this.inodeId = Long.parseLong(st.getValue("INODEID"));
      this.path = st.getValue("PATH");
      this.timestamp = Long.parseLong(st.getValue("TIMESTAMP"));
      this.permissions = permissionStatusFromXml(st);
      aclEntries = readAclEntriesFromXml(st);
      xAttrs = readXAttrsFromXml(st);
    }
  }

  /**
   * The corresponding operations are either {@literal @Idempotent} (
   * {@link ClientProtocol#updateBlockForPipeline},
   * {@link ClientProtocol#recoverLease}, {@link ClientProtocol#addBlock}) or
   * already bound with other editlog op which records rpc ids (
   * {@link ClientProtocol#create}). Thus no need to record rpc ids here.
   */
  static class SetGenstampV1Op extends FSEditLogOp {
    long genStampV1;

    private SetGenstampV1Op() {
      super(OP_SET_GENSTAMP_V1);
    }

    static SetGenstampV1Op getInstance(OpInstanceCache cache) {
      return (SetGenstampV1Op)cache.get(OP_SET_GENSTAMP_V1);
    }

    @Override
    void resetSubFields() {
      genStampV1 = 0L;
    }

    SetGenstampV1Op setGenerationStamp(long genStamp) {
      this.genStampV1 = genStamp;
      return this;
    }

    @Override
    public
    void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeLong(genStampV1, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.genStampV1 = FSImageSerialization.readLong(in);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("SetGenstampOp [GenStamp=");
      builder.append(genStampV1);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "GENSTAMP",
                            Long.toString(genStampV1));
    }

    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.genStampV1 = Long.parseLong(st.getValue("GENSTAMP"));
    }
  }

  /** Similar with {@link SetGenstampV1Op} */
  static class SetGenstampV2Op extends FSEditLogOp {
    long genStampV2;

    private SetGenstampV2Op() {
      super(OP_SET_GENSTAMP_V2);
    }

    static SetGenstampV2Op getInstance(OpInstanceCache cache) {
      return (SetGenstampV2Op)cache.get(OP_SET_GENSTAMP_V2);
    }

    @Override
    void resetSubFields() {
      genStampV2 = 0L;
    }

    SetGenstampV2Op setGenerationStamp(long genStamp) {
      this.genStampV2 = genStamp;
      return this;
    }

    @Override
    public
    void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeLong(genStampV2, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.genStampV2 = FSImageSerialization.readLong(in);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("SetGenstampV2Op [GenStampV2=");
      builder.append(genStampV2);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "GENSTAMPV2",
                            Long.toString(genStampV2));
    }

    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.genStampV2 = Long.parseLong(st.getValue("GENSTAMPV2"));
    }
  }

  /** {@literal @Idempotent} for {@link ClientProtocol#addBlock} */
  static class AllocateBlockIdOp extends FSEditLogOp {
    long blockId;

    private AllocateBlockIdOp() {
      super(OP_ALLOCATE_BLOCK_ID);
    }

    static AllocateBlockIdOp getInstance(OpInstanceCache cache) {
      return (AllocateBlockIdOp)cache.get(OP_ALLOCATE_BLOCK_ID);
    }

    @Override
    void resetSubFields() {
      blockId = 0L;
    }

    AllocateBlockIdOp setBlockId(long blockId) {
      this.blockId = blockId;
      return this;
    }

    @Override
    public
    void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeLong(blockId, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.blockId = FSImageSerialization.readLong(in);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("AllocateBlockIdOp [blockId=");
      builder.append(blockId);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "BLOCK_ID",
                            Long.toString(blockId));
    }

    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.blockId = Long.parseLong(st.getValue("BLOCK_ID"));
    }
  }

  /** {@literal @Idempotent} for {@link ClientProtocol#setPermission} */
  static class SetPermissionsOp extends FSEditLogOp {
    String src;
    FsPermission permissions;

    private SetPermissionsOp() {
      super(OP_SET_PERMISSIONS);
    }

    static SetPermissionsOp getInstance(OpInstanceCache cache) {
      return (SetPermissionsOp)cache.get(OP_SET_PERMISSIONS);
    }

    @Override
    void resetSubFields() {
      src = null;
      permissions = null;
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
    public 
    void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(src, out);
      permissions.write(out);
     }
 
    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.src = FSImageSerialization.readString(in);
      this.permissions = FsPermission.read(in);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("SetPermissionsOp [src=");
      builder.append(src);
      builder.append(", permissions=");
      builder.append(permissions);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }
    
    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "SRC", src);
      XMLUtils.addSaxString(contentHandler, "MODE",
          Short.valueOf(permissions.toShort()).toString());
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.src = st.getValue("SRC");
      this.permissions = new FsPermission(
          Short.valueOf(st.getValue("MODE")));
    }
  }

  /** {@literal @Idempotent} for {@link ClientProtocol#setOwner} */
  static class SetOwnerOp extends FSEditLogOp {
    String src;
    String username;
    String groupname;

    private SetOwnerOp() {
      super(OP_SET_OWNER);
    }

    static SetOwnerOp getInstance(OpInstanceCache cache) {
      return (SetOwnerOp)cache.get(OP_SET_OWNER);
    }

    @Override
    void resetSubFields() {
      src = null;
      username = null;
      groupname = null;
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
    public 
    void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(src, out);
      FSImageSerialization.writeString(username == null ? "" : username, out);
      FSImageSerialization.writeString(groupname == null ? "" : groupname, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.src = FSImageSerialization.readString(in);
      this.username = FSImageSerialization.readString_EmptyAsNull(in);
      this.groupname = FSImageSerialization.readString_EmptyAsNull(in);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("SetOwnerOp [src=");
      builder.append(src);
      builder.append(", username=");
      builder.append(username);
      builder.append(", groupname=");
      builder.append(groupname);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }
    
    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "SRC", src);
      if (username != null) {
        XMLUtils.addSaxString(contentHandler, "USERNAME", username);
      }
      if (groupname != null) {
        XMLUtils.addSaxString(contentHandler, "GROUPNAME", groupname);
      }
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.src = st.getValue("SRC");
      this.username = (st.hasChildren("USERNAME")) ? 
          st.getValue("USERNAME") : null;
      this.groupname = (st.hasChildren("GROUPNAME")) ? 
          st.getValue("GROUPNAME") : null;
    }
  }
  
  static class SetNSQuotaOp extends FSEditLogOp {
    String src;
    long nsQuota;

    private SetNSQuotaOp() {
      super(OP_SET_NS_QUOTA);
    }

    static SetNSQuotaOp getInstance(OpInstanceCache cache) {
      return (SetNSQuotaOp)cache.get(OP_SET_NS_QUOTA);
    }

    @Override
    void resetSubFields() {
      src = null;
      nsQuota = 0L;
    }

    @Override
    public 
    void writeFields(DataOutputStream out) throws IOException {
      throw new IOException("Deprecated");      
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.src = FSImageSerialization.readString(in);
      this.nsQuota = FSImageSerialization.readLong(in);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("SetNSQuotaOp [src=");
      builder.append(src);
      builder.append(", nsQuota=");
      builder.append(nsQuota);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }
    
    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "SRC", src);
      XMLUtils.addSaxString(contentHandler, "NSQUOTA",
          Long.toString(nsQuota));
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.src = st.getValue("SRC");
      this.nsQuota = Long.parseLong(st.getValue("NSQUOTA"));
    }
  }

  static class ClearNSQuotaOp extends FSEditLogOp {
    String src;

    private ClearNSQuotaOp() {
      super(OP_CLEAR_NS_QUOTA);
    }

    static ClearNSQuotaOp getInstance(OpInstanceCache cache) {
      return (ClearNSQuotaOp)cache.get(OP_CLEAR_NS_QUOTA);
    }

    @Override
    void resetSubFields() {
      src = null;
    }

    @Override
    public 
    void writeFields(DataOutputStream out) throws IOException {
      throw new IOException("Deprecated");      
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.src = FSImageSerialization.readString(in);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("ClearNSQuotaOp [src=");
      builder.append(src);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }
    
    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "SRC", src);
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.src = st.getValue("SRC");
    }
  }

  /** {@literal @Idempotent} for {@link ClientProtocol#setQuota} */
  static class SetQuotaOp extends FSEditLogOp {
    String src;
    long nsQuota;
    long dsQuota;

    private SetQuotaOp() {
      super(OP_SET_QUOTA);
    }

    static SetQuotaOp getInstance(OpInstanceCache cache) {
      return (SetQuotaOp)cache.get(OP_SET_QUOTA);
    }

    @Override
    void resetSubFields() {
      src = null;
      nsQuota = 0L;
      dsQuota = 0L;
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
    public 
    void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(src, out);
      FSImageSerialization.writeLong(nsQuota, out);
      FSImageSerialization.writeLong(dsQuota, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.src = FSImageSerialization.readString(in);
      this.nsQuota = FSImageSerialization.readLong(in);
      this.dsQuota = FSImageSerialization.readLong(in);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("SetQuotaOp [src=");
      builder.append(src);
      builder.append(", nsQuota=");
      builder.append(nsQuota);
      builder.append(", dsQuota=");
      builder.append(dsQuota);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }
    
    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "SRC", src);
      XMLUtils.addSaxString(contentHandler, "NSQUOTA",
          Long.toString(nsQuota));
      XMLUtils.addSaxString(contentHandler, "DSQUOTA",
          Long.toString(dsQuota));
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.src = st.getValue("SRC");
      this.nsQuota = Long.parseLong(st.getValue("NSQUOTA"));
      this.dsQuota = Long.parseLong(st.getValue("DSQUOTA"));
    }
  }

  /** {@literal @Idempotent} for {@link ClientProtocol#setQuota} */
  static class SetQuotaByStorageTypeOp extends FSEditLogOp {
    String src;
    long dsQuota;
    StorageType type;

    private SetQuotaByStorageTypeOp() {
      super(OP_SET_QUOTA_BY_STORAGETYPE);
    }

    static SetQuotaByStorageTypeOp getInstance(OpInstanceCache cache) {
      return (SetQuotaByStorageTypeOp)cache.get(OP_SET_QUOTA_BY_STORAGETYPE);
    }

    @Override
    void resetSubFields() {
      src = null;
      dsQuota = -1L;
      type = StorageType.DEFAULT;
    }

    SetQuotaByStorageTypeOp setSource(String src) {
      this.src = src;
      return this;
    }

    SetQuotaByStorageTypeOp setQuotaByStorageType(long dsQuota, StorageType type) {
      this.type = type;
      this.dsQuota = dsQuota;
      return this;
    }

    @Override
    public
    void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(src, out);
      FSImageSerialization.writeInt(type.ordinal(), out);
      FSImageSerialization.writeLong(dsQuota, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
      throws IOException {
      this.src = FSImageSerialization.readString(in);
      this.type = StorageType.parseStorageType(FSImageSerialization.readInt(in));
      this.dsQuota = FSImageSerialization.readLong(in);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("SetTypeQuotaOp [src=");
      builder.append(src);
      builder.append(", storageType=");
      builder.append(type);
      builder.append(", dsQuota=");
      builder.append(dsQuota);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "SRC", src);
      XMLUtils.addSaxString(contentHandler, "STORAGETYPE",
        Integer.toString(type.ordinal()));
      XMLUtils.addSaxString(contentHandler, "DSQUOTA",
        Long.toString(dsQuota));
    }

    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.src = st.getValue("SRC");
      this.type = StorageType.parseStorageType(
          Integer.parseInt(st.getValue("STORAGETYPE")));
      this.dsQuota = Long.parseLong(st.getValue("DSQUOTA"));
    }
  }

  /** {@literal @Idempotent} for {@link ClientProtocol#setTimes} */
  static class TimesOp extends FSEditLogOp {
    int length;
    String path;
    long mtime;
    long atime;

    private TimesOp() {
      super(OP_TIMES);
    }

    static TimesOp getInstance(OpInstanceCache cache) {
      return (TimesOp)cache.get(OP_TIMES);
    }

    @Override
    void resetSubFields() {
      length = 0;
      path = null;
      mtime = 0L;
      atime = 0L;
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
    public 
    void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(path, out);
      FSImageSerialization.writeLong(mtime, out);
      FSImageSerialization.writeLong(atime, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      if (!NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.length = in.readInt();
        if (length != 3) {
          throw new IOException("Incorrect data format. " + "times operation.");
        }
      }
      this.path = FSImageSerialization.readString(in);

      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.mtime = FSImageSerialization.readLong(in);
        this.atime = FSImageSerialization.readLong(in);
      } else {
        this.mtime = readLong(in);
        this.atime = readLong(in);
      }
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("TimesOp [length=");
      builder.append(length);
      builder.append(", path=");
      builder.append(path);
      builder.append(", mtime=");
      builder.append(mtime);
      builder.append(", atime=");
      builder.append(atime);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }
    
    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "LENGTH",
          Integer.toString(length));
      XMLUtils.addSaxString(contentHandler, "PATH", path);
      XMLUtils.addSaxString(contentHandler, "MTIME",
          Long.toString(mtime));
      XMLUtils.addSaxString(contentHandler, "ATIME",
          Long.toString(atime));
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.length = Integer.parseInt(st.getValue("LENGTH"));
      this.path = st.getValue("PATH");
      this.mtime = Long.parseLong(st.getValue("MTIME"));
      this.atime = Long.parseLong(st.getValue("ATIME"));
    }
  }

  /** {@literal @AtMostOnce} for {@link ClientProtocol#createSymlink} */
  static class SymlinkOp extends FSEditLogOp {
    int length;
    long inodeId;
    String path;
    String value;
    long mtime;
    long atime;
    PermissionStatus permissionStatus;

    private SymlinkOp() {
      super(OP_SYMLINK);
    }

    static SymlinkOp getInstance(OpInstanceCache cache) {
      return (SymlinkOp)cache.get(OP_SYMLINK);
    }

    @Override
    void resetSubFields() {
      length = 0;
      inodeId = 0L;
      path = null;
      value = null;
      mtime = 0L;
      atime = 0L;
      permissionStatus = null;
    }

    SymlinkOp setId(long inodeId) {
      this.inodeId = inodeId;
      return this;
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
    public void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeLong(inodeId, out);      
      FSImageSerialization.writeString(path, out);
      FSImageSerialization.writeString(value, out);
      FSImageSerialization.writeLong(mtime, out);
      FSImageSerialization.writeLong(atime, out);
      permissionStatus.write(out);
      writeRpcIds(rpcClientId, rpcCallId, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      if (!NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.length = in.readInt();
        if (this.length != 4) {
          throw new IOException("Incorrect data format. "
              + "symlink operation.");
        }
      }
      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.ADD_INODE_ID, logVersion)) {
        this.inodeId = FSImageSerialization.readLong(in);
      } else {
        // This id should be updated when the editLogOp is applied
        this.inodeId = INodeId.GRANDFATHER_INODE_ID;
      }
      this.path = FSImageSerialization.readString(in);
      this.value = FSImageSerialization.readString(in);

      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.mtime = FSImageSerialization.readLong(in);
        this.atime = FSImageSerialization.readLong(in);
      } else {
        this.mtime = readLong(in);
        this.atime = readLong(in);
      }
      this.permissionStatus = PermissionStatus.read(in);
      
      // read RPC ids if necessary
      readRpcIds(in, logVersion);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("SymlinkOp [length=");
      builder.append(length);
      builder.append(", inodeId=");
      builder.append(inodeId);
      builder.append(", path=");
      builder.append(path);
      builder.append(", value=");
      builder.append(value);
      builder.append(", mtime=");
      builder.append(mtime);
      builder.append(", atime=");
      builder.append(atime);
      builder.append(", permissionStatus=");
      builder.append(permissionStatus);
      appendRpcIdsToString(builder, rpcClientId, rpcCallId);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }
    
    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "LENGTH",
          Integer.toString(length));
      XMLUtils.addSaxString(contentHandler, "INODEID",
          Long.toString(inodeId));
      XMLUtils.addSaxString(contentHandler, "PATH", path);
      XMLUtils.addSaxString(contentHandler, "VALUE", value);
      XMLUtils.addSaxString(contentHandler, "MTIME",
          Long.toString(mtime));
      XMLUtils.addSaxString(contentHandler, "ATIME",
          Long.toString(atime));
      FSEditLogOp.permissionStatusToXml(contentHandler, permissionStatus);
      appendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
    }

    @Override 
    void fromXml(Stanza st) throws InvalidXmlException {
      this.length = Integer.parseInt(st.getValue("LENGTH"));
      this.inodeId = Long.parseLong(st.getValue("INODEID"));
      this.path = st.getValue("PATH");
      this.value = st.getValue("VALUE");
      this.mtime = Long.parseLong(st.getValue("MTIME"));
      this.atime = Long.parseLong(st.getValue("ATIME"));
      this.permissionStatus = permissionStatusFromXml(st);
      
      readRpcIdsFromXml(st);
    }
  }

  /** {@literal @AtMostOnce} for {@link ClientProtocol#rename2} */
  static class RenameOp extends FSEditLogOp {
    int length;
    String src;
    String dst;
    long timestamp;
    Rename[] options;

    private RenameOp() {
      super(OP_RENAME);
    }

    static RenameOp getInstance(OpInstanceCache cache) {
      return (RenameOp)cache.get(OP_RENAME);
    }

    @Override
    void resetSubFields() {
      length = 0;
      src = null;
      dst = null;
      timestamp = 0L;
      options = null;
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
    public 
    void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(src, out);
      FSImageSerialization.writeString(dst, out);
      FSImageSerialization.writeLong(timestamp, out);
      toBytesWritable(options).write(out);
      writeRpcIds(rpcClientId, rpcCallId, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      if (!NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.length = in.readInt();
        if (this.length != 3) {
          throw new IOException("Incorrect data format. " + "Rename operation.");
        }
      }
      this.src = FSImageSerialization.readString(in);
      this.dst = FSImageSerialization.readString(in);

      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.timestamp = FSImageSerialization.readLong(in);
      } else {
        this.timestamp = readLong(in);
      }
      this.options = readRenameOptions(in);
      
      // read RPC ids if necessary
      readRpcIds(in, logVersion);
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

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("RenameOp [length=");
      builder.append(length);
      builder.append(", src=");
      builder.append(src);
      builder.append(", dst=");
      builder.append(dst);
      builder.append(", timestamp=");
      builder.append(timestamp);
      builder.append(", options=");
      builder.append(Arrays.toString(options));
      appendRpcIdsToString(builder, rpcClientId, rpcCallId);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }
    
    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "LENGTH",
          Integer.toString(length));
      XMLUtils.addSaxString(contentHandler, "SRC", src);
      XMLUtils.addSaxString(contentHandler, "DST", dst);
      XMLUtils.addSaxString(contentHandler, "TIMESTAMP",
          Long.toString(timestamp));
      StringBuilder bld = new StringBuilder();
      String prefix = "";
      for (Rename r : options) {
        bld.append(prefix).append(r.toString());
        prefix = "|";
      }
      XMLUtils.addSaxString(contentHandler, "OPTIONS", bld.toString());
      appendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.length = Integer.parseInt(st.getValue("LENGTH"));
      this.src = st.getValue("SRC");
      this.dst = st.getValue("DST");
      this.timestamp = Long.parseLong(st.getValue("TIMESTAMP"));
      String opts = st.getValue("OPTIONS");
      String o[] = opts.split("\\|");
      this.options = new Rename[o.length];
      for (int i = 0; i < o.length; i++) {
        if (o[i].equals(""))
          continue;
        try {
          this.options[i] = Rename.valueOf(o[i]);
        } finally {
          if (this.options[i] == null) {
            System.err.println("error parsing Rename value: \"" + o[i] + "\"");
          }
        }
      }
      readRpcIdsFromXml(st);
    }
  }

  static class TruncateOp extends FSEditLogOp {
    String src;
    String clientName;
    String clientMachine;
    long newLength;
    long timestamp;
    Block truncateBlock;

    private TruncateOp() {
      super(OP_TRUNCATE);
    }

    static TruncateOp getInstance(OpInstanceCache cache) {
      return (TruncateOp)cache.get(OP_TRUNCATE);
    }

    @Override
    void resetSubFields() {
      src = null;
      clientName = null;
      clientMachine = null;
      newLength = 0L;
      timestamp = 0L;
    }

    TruncateOp setPath(String src) {
      this.src = src;
      return this;
    }

    TruncateOp setClientName(String clientName) {
      this.clientName = clientName;
      return this;
    }

    TruncateOp setClientMachine(String clientMachine) {
      this.clientMachine = clientMachine;
      return this;
    }

    TruncateOp setNewLength(long newLength) {
      this.newLength = newLength;
      return this;
    }

    TruncateOp setTimestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    TruncateOp setTruncateBlock(Block truncateBlock) {
      this.truncateBlock = truncateBlock;
      return this;
    }

    @Override
    void readFields(DataInputStream in, int logVersion) throws IOException {
      src = FSImageSerialization.readString(in);
      clientName = FSImageSerialization.readString(in);
      clientMachine = FSImageSerialization.readString(in);
      newLength = FSImageSerialization.readLong(in);
      timestamp = FSImageSerialization.readLong(in);
      Block[] blocks =
          FSImageSerialization.readCompactBlockArray(in, logVersion);
      assert blocks.length <= 1 : "Truncate op should have 1 or 0 blocks";
      truncateBlock = (blocks.length == 0) ? null : blocks[0];
    }

    @Override
    public void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(src, out);
      FSImageSerialization.writeString(clientName, out);
      FSImageSerialization.writeString(clientMachine, out);
      FSImageSerialization.writeLong(newLength, out);
      FSImageSerialization.writeLong(timestamp, out);
      int size = truncateBlock != null ? 1 : 0;
      Block[] blocks = new Block[size];
      if (truncateBlock != null) {
        blocks[0] = truncateBlock;
      }
      FSImageSerialization.writeCompactBlockArray(blocks, out);
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "SRC", src);
      XMLUtils.addSaxString(contentHandler, "CLIENTNAME", clientName);
      XMLUtils.addSaxString(contentHandler, "CLIENTMACHINE", clientMachine);
      XMLUtils.addSaxString(contentHandler, "NEWLENGTH",
          Long.toString(newLength));
      XMLUtils.addSaxString(contentHandler, "TIMESTAMP",
          Long.toString(timestamp));
      if(truncateBlock != null)
        FSEditLogOp.blockToXml(contentHandler, truncateBlock);
    }

    @Override
    void fromXml(Stanza st) throws InvalidXmlException {
      this.src = st.getValue("SRC");
      this.clientName = st.getValue("CLIENTNAME");
      this.clientMachine = st.getValue("CLIENTMACHINE");
      this.newLength = Long.parseLong(st.getValue("NEWLENGTH"));
      this.timestamp = Long.parseLong(st.getValue("TIMESTAMP"));
      if (st.hasChildren("BLOCK"))
        this.truncateBlock = FSEditLogOp.blockFromXml(st);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("TruncateOp [src=");
      builder.append(src);
      builder.append(", clientName=");
      builder.append(clientName);
      builder.append(", clientMachine=");
      builder.append(clientMachine);
      builder.append(", newLength=");
      builder.append(newLength);
      builder.append(", timestamp=");
      builder.append(timestamp);
      builder.append(", truncateBlock=");
      builder.append(truncateBlock);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }
  }
 
  /**
   * {@literal @Idempotent} for {@link ClientProtocol#recoverLease}. In the
   * meanwhile, startFile and appendFile both have their own corresponding
   * editlog op.
   */
  static class ReassignLeaseOp extends FSEditLogOp {
    String leaseHolder;
    String path;
    String newHolder;

    private ReassignLeaseOp() {
      super(OP_REASSIGN_LEASE);
    }

    static ReassignLeaseOp getInstance(OpInstanceCache cache) {
      return (ReassignLeaseOp)cache.get(OP_REASSIGN_LEASE);
    }

    @Override
    void resetSubFields() {
      leaseHolder = null;
      path = null;
      newHolder = null;
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
    public 
    void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(leaseHolder, out);
      FSImageSerialization.writeString(path, out);
      FSImageSerialization.writeString(newHolder, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.leaseHolder = FSImageSerialization.readString(in);
      this.path = FSImageSerialization.readString(in);
      this.newHolder = FSImageSerialization.readString(in);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("ReassignLeaseOp [leaseHolder=");
      builder.append(leaseHolder);
      builder.append(", path=");
      builder.append(path);
      builder.append(", newHolder=");
      builder.append(newHolder);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }
    
    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "LEASEHOLDER", leaseHolder);
      XMLUtils.addSaxString(contentHandler, "PATH", path);
      XMLUtils.addSaxString(contentHandler, "NEWHOLDER", newHolder);
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.leaseHolder = st.getValue("LEASEHOLDER");
      this.path = st.getValue("PATH");
      this.newHolder = st.getValue("NEWHOLDER");
    }
  }

  /** {@literal @Idempotent} for {@link ClientProtocol#getDelegationToken} */
  static class GetDelegationTokenOp extends FSEditLogOp {
    DelegationTokenIdentifier token;
    long expiryTime;

    private GetDelegationTokenOp() {
      super(OP_GET_DELEGATION_TOKEN);
    }

    static GetDelegationTokenOp getInstance(OpInstanceCache cache) {
      return (GetDelegationTokenOp)cache.get(OP_GET_DELEGATION_TOKEN);
    }

    @Override
    void resetSubFields() {
      token = null;
      expiryTime = 0L;
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
    public 
    void writeFields(DataOutputStream out) throws IOException {
      token.write(out);
      FSImageSerialization.writeLong(expiryTime, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.token = new DelegationTokenIdentifier();
      this.token.readFields(in);
      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.expiryTime = FSImageSerialization.readLong(in);
      } else {
        this.expiryTime = readLong(in);
      }
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("GetDelegationTokenOp [token=");
      builder.append(token);
      builder.append(", expiryTime=");
      builder.append(expiryTime);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }
    
    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      FSEditLogOp.delegationTokenToXml(contentHandler, token);
      XMLUtils.addSaxString(contentHandler, "EXPIRY_TIME",
          Long.toString(expiryTime));
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.token = delegationTokenFromXml(st.getChildren(
          "DELEGATION_TOKEN_IDENTIFIER").get(0));
      this.expiryTime = Long.parseLong(st.getValue("EXPIRY_TIME"));
    }
  }

  /** {@literal @Idempotent} for {@link ClientProtocol#renewDelegationToken} */
  static class RenewDelegationTokenOp extends FSEditLogOp {
    DelegationTokenIdentifier token;
    long expiryTime;

    private RenewDelegationTokenOp() {
      super(OP_RENEW_DELEGATION_TOKEN);
    }

    static RenewDelegationTokenOp getInstance(OpInstanceCache cache) {
      return (RenewDelegationTokenOp)cache.get(OP_RENEW_DELEGATION_TOKEN);
    }

    @Override
    void resetSubFields() {
      token = null;
      expiryTime = 0L;
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
    public 
    void writeFields(DataOutputStream out) throws IOException {
      token.write(out);
      FSImageSerialization.writeLong(expiryTime, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.token = new DelegationTokenIdentifier();
      this.token.readFields(in);
      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.expiryTime = FSImageSerialization.readLong(in);
      } else {
        this.expiryTime = readLong(in);
      }
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("RenewDelegationTokenOp [token=");
      builder.append(token);
      builder.append(", expiryTime=");
      builder.append(expiryTime);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }
    
    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      FSEditLogOp.delegationTokenToXml(contentHandler, token);
      XMLUtils.addSaxString(contentHandler, "EXPIRY_TIME",
          Long.toString(expiryTime));
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.token = delegationTokenFromXml(st.getChildren(
          "DELEGATION_TOKEN_IDENTIFIER").get(0));
      this.expiryTime = Long.parseLong(st.getValue("EXPIRY_TIME"));
    }
  }

  /** {@literal @Idempotent} for {@link ClientProtocol#cancelDelegationToken} */
  static class CancelDelegationTokenOp extends FSEditLogOp {
    DelegationTokenIdentifier token;

    private CancelDelegationTokenOp() {
      super(OP_CANCEL_DELEGATION_TOKEN);
    }

    static CancelDelegationTokenOp getInstance(OpInstanceCache cache) {
      return (CancelDelegationTokenOp)cache.get(OP_CANCEL_DELEGATION_TOKEN);
    }

    @Override
    void resetSubFields() {
      token = null;
    }

    CancelDelegationTokenOp setDelegationTokenIdentifier(
        DelegationTokenIdentifier token) {
      this.token = token;
      return this;
    }

    @Override
    public 
    void writeFields(DataOutputStream out) throws IOException {
      token.write(out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.token = new DelegationTokenIdentifier();
      this.token.readFields(in);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("CancelDelegationTokenOp [token=");
      builder.append(token);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }
    
    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      FSEditLogOp.delegationTokenToXml(contentHandler, token);
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.token = delegationTokenFromXml(st.getChildren(
          "DELEGATION_TOKEN_IDENTIFIER").get(0));
    }
  }

  static class UpdateMasterKeyOp extends FSEditLogOp {
    DelegationKey key;

    private UpdateMasterKeyOp() {
      super(OP_UPDATE_MASTER_KEY);
    }

    static UpdateMasterKeyOp getInstance(OpInstanceCache cache) {
      return (UpdateMasterKeyOp)cache.get(OP_UPDATE_MASTER_KEY);
    }

    @Override
    void resetSubFields() {
      key = null;
    }

    UpdateMasterKeyOp setDelegationKey(DelegationKey key) {
      this.key = key;
      return this;
    }
    
    @Override
    public 
    void writeFields(DataOutputStream out) throws IOException {
      key.write(out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.key = new DelegationKey();
      this.key.readFields(in);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("UpdateMasterKeyOp [key=");
      builder.append(key);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }
    
    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      FSEditLogOp.delegationKeyToXml(contentHandler, key);
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.key = delegationKeyFromXml(st.getChildren(
          "DELEGATION_KEY").get(0));
    }
  }
  
  static class LogSegmentOp extends FSEditLogOp {
    private LogSegmentOp(FSEditLogOpCodes code) {
      super(code);
      assert code == OP_START_LOG_SEGMENT ||
             code == OP_END_LOG_SEGMENT : "Bad op: " + code;
    }

    static LogSegmentOp getInstance(OpInstanceCache cache,
        FSEditLogOpCodes code) {
      return (LogSegmentOp)cache.get(code);
    }

    @Override
    void resetSubFields() {
      // no data stored in these ops yet
    }

    @Override
    public void readFields(DataInputStream in, int logVersion)
        throws IOException {
      // no data stored in these ops yet
    }

    @Override
    public
    void writeFields(DataOutputStream out) throws IOException {
      // no data stored
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("LogSegmentOp [opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      // no data stored
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      // do nothing
    }
  }

  static class InvalidOp extends FSEditLogOp {
    private InvalidOp() {
      super(OP_INVALID);
    }

    static InvalidOp getInstance(OpInstanceCache cache) {
      return (InvalidOp)cache.get(OP_INVALID);
    }

    @Override
    void resetSubFields() {
    }

    @Override
    public 
    void writeFields(DataOutputStream out) throws IOException {
    }
    
    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      // nothing to read
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("InvalidOp [opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }
    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      // no data stored
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      // do nothing
    }
  }

  /**
   * Operation corresponding to creating a snapshot.
   * {@literal @AtMostOnce} for {@link ClientProtocol#createSnapshot}.
   */
  static class CreateSnapshotOp extends FSEditLogOp {
    String snapshotRoot;
    String snapshotName;
    
    public CreateSnapshotOp() {
      super(OP_CREATE_SNAPSHOT);
    }
    
    static CreateSnapshotOp getInstance(OpInstanceCache cache) {
      return (CreateSnapshotOp)cache.get(OP_CREATE_SNAPSHOT);
    }

    @Override
    void resetSubFields() {
      snapshotRoot = null;
      snapshotName = null;
    }

    CreateSnapshotOp setSnapshotName(String snapName) {
      this.snapshotName = snapName;
      return this;
    }

    public CreateSnapshotOp setSnapshotRoot(String snapRoot) {
      snapshotRoot = snapRoot;
      return this;
    }
    
    @Override
    void readFields(DataInputStream in, int logVersion) throws IOException {
      snapshotRoot = FSImageSerialization.readString(in);
      snapshotName = FSImageSerialization.readString(in);
      
      // read RPC ids if necessary
      readRpcIds(in, logVersion);
    }

    @Override
    public void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(snapshotRoot, out);
      FSImageSerialization.writeString(snapshotName, out);
      writeRpcIds(rpcClientId, rpcCallId, out);
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "SNAPSHOTROOT", snapshotRoot);
      XMLUtils.addSaxString(contentHandler, "SNAPSHOTNAME", snapshotName);
      appendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
    }

    @Override
    void fromXml(Stanza st) throws InvalidXmlException {
      snapshotRoot = st.getValue("SNAPSHOTROOT");
      snapshotName = st.getValue("SNAPSHOTNAME");
      
      readRpcIdsFromXml(st);
    }
    
    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("CreateSnapshotOp [snapshotRoot=");
      builder.append(snapshotRoot);
      builder.append(", snapshotName=");
      builder.append(snapshotName);
      appendRpcIdsToString(builder, rpcClientId, rpcCallId);
      builder.append("]");
      return builder.toString();
    }
  }
  
  /**
   * Operation corresponding to delete a snapshot.
   * {@literal @AtMostOnce} for {@link ClientProtocol#deleteSnapshot}.
   */
  static class DeleteSnapshotOp extends FSEditLogOp {
    String snapshotRoot;
    String snapshotName;
    
    DeleteSnapshotOp() {
      super(OP_DELETE_SNAPSHOT);
    }
    
    static DeleteSnapshotOp getInstance(OpInstanceCache cache) {
      return (DeleteSnapshotOp)cache.get(OP_DELETE_SNAPSHOT);
    }

    @Override
    void resetSubFields() {
      snapshotRoot = null;
      snapshotName = null;
    }
    
    DeleteSnapshotOp setSnapshotName(String snapName) {
      this.snapshotName = snapName;
      return this;
    }

    DeleteSnapshotOp setSnapshotRoot(String snapRoot) {
      snapshotRoot = snapRoot;
      return this;
    }
    
    @Override
    void readFields(DataInputStream in, int logVersion) throws IOException {
      snapshotRoot = FSImageSerialization.readString(in);
      snapshotName = FSImageSerialization.readString(in);
      
      // read RPC ids if necessary
      readRpcIds(in, logVersion);
    }

    @Override
    public void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(snapshotRoot, out);
      FSImageSerialization.writeString(snapshotName, out);
      writeRpcIds(rpcClientId, rpcCallId, out);
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "SNAPSHOTROOT", snapshotRoot);
      XMLUtils.addSaxString(contentHandler, "SNAPSHOTNAME", snapshotName);
      appendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
    }

    @Override
    void fromXml(Stanza st) throws InvalidXmlException {
      snapshotRoot = st.getValue("SNAPSHOTROOT");
      snapshotName = st.getValue("SNAPSHOTNAME");
      
      readRpcIdsFromXml(st);
    }
    
    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("DeleteSnapshotOp [snapshotRoot=");
      builder.append(snapshotRoot);
      builder.append(", snapshotName=");
      builder.append(snapshotName);
      appendRpcIdsToString(builder, rpcClientId, rpcCallId);
      builder.append("]");
      return builder.toString();
    }
  }
  
  /**
   * Operation corresponding to rename a snapshot.
   * {@literal @AtMostOnce} for {@link ClientProtocol#renameSnapshot}.
   */
  static class RenameSnapshotOp extends FSEditLogOp {
    String snapshotRoot;
    String snapshotOldName;
    String snapshotNewName;
    
    RenameSnapshotOp() {
      super(OP_RENAME_SNAPSHOT);
    }
    
    static RenameSnapshotOp getInstance(OpInstanceCache cache) {
      return (RenameSnapshotOp) cache.get(OP_RENAME_SNAPSHOT);
    }

    @Override
    void resetSubFields() {
      snapshotRoot = null;
      snapshotOldName = null;
      snapshotNewName = null;
    }
    
    RenameSnapshotOp setSnapshotOldName(String snapshotOldName) {
      this.snapshotOldName = snapshotOldName;
      return this;
    }

    RenameSnapshotOp setSnapshotNewName(String snapshotNewName) {
      this.snapshotNewName = snapshotNewName;
      return this;
    }
    
    RenameSnapshotOp setSnapshotRoot(String snapshotRoot) {
      this.snapshotRoot = snapshotRoot;
      return this;
    }
    
    @Override
    void readFields(DataInputStream in, int logVersion) throws IOException {
      snapshotRoot = FSImageSerialization.readString(in);
      snapshotOldName = FSImageSerialization.readString(in);
      snapshotNewName = FSImageSerialization.readString(in);
      
      // read RPC ids if necessary
      readRpcIds(in, logVersion);
    }

    @Override
    public void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(snapshotRoot, out);
      FSImageSerialization.writeString(snapshotOldName, out);
      FSImageSerialization.writeString(snapshotNewName, out);
      
      writeRpcIds(rpcClientId, rpcCallId, out);
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "SNAPSHOTROOT", snapshotRoot);
      XMLUtils.addSaxString(contentHandler, "SNAPSHOTOLDNAME", snapshotOldName);
      XMLUtils.addSaxString(contentHandler, "SNAPSHOTNEWNAME", snapshotNewName);
      appendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
    }

    @Override
    void fromXml(Stanza st) throws InvalidXmlException {
      snapshotRoot = st.getValue("SNAPSHOTROOT");
      snapshotOldName = st.getValue("SNAPSHOTOLDNAME");
      snapshotNewName = st.getValue("SNAPSHOTNEWNAME");
      
      readRpcIdsFromXml(st);
    }
    
    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("RenameSnapshotOp [snapshotRoot=");
      builder.append(snapshotRoot);
      builder.append(", snapshotOldName=");
      builder.append(snapshotOldName);
      builder.append(", snapshotNewName=");
      builder.append(snapshotNewName);
      appendRpcIdsToString(builder, rpcClientId, rpcCallId);
      builder.append("]");
      return builder.toString();
    }
  }

  /**
   * Operation corresponding to allow creating snapshot on a directory
   */
  static class AllowSnapshotOp extends FSEditLogOp { // @Idempotent
    String snapshotRoot;

    public AllowSnapshotOp() {
      super(OP_ALLOW_SNAPSHOT);
    }

    public AllowSnapshotOp(String snapRoot) {
      super(OP_ALLOW_SNAPSHOT);
      snapshotRoot = snapRoot;
    }

    static AllowSnapshotOp getInstance(OpInstanceCache cache) {
      return (AllowSnapshotOp) cache.get(OP_ALLOW_SNAPSHOT);
    }

    @Override
    void resetSubFields() {
      snapshotRoot = null;
    }

    public AllowSnapshotOp setSnapshotRoot(String snapRoot) {
      snapshotRoot = snapRoot;
      return this;
    }

    @Override
    void readFields(DataInputStream in, int logVersion) throws IOException {
      snapshotRoot = FSImageSerialization.readString(in);
    }

    @Override
    public void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(snapshotRoot, out);
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "SNAPSHOTROOT", snapshotRoot);
    }

    @Override
    void fromXml(Stanza st) throws InvalidXmlException {
      snapshotRoot = st.getValue("SNAPSHOTROOT");
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("AllowSnapshotOp [snapshotRoot=");
      builder.append(snapshotRoot);
      builder.append("]");
      return builder.toString();
    }
  }

  /**
   * Operation corresponding to disallow creating snapshot on a directory
   */
  static class DisallowSnapshotOp extends FSEditLogOp { // @Idempotent
    String snapshotRoot;

    public DisallowSnapshotOp() {
      super(OP_DISALLOW_SNAPSHOT);
    }

    public DisallowSnapshotOp(String snapRoot) {
      super(OP_DISALLOW_SNAPSHOT);
      snapshotRoot = snapRoot;
    }

    static DisallowSnapshotOp getInstance(OpInstanceCache cache) {
      return (DisallowSnapshotOp) cache.get(OP_DISALLOW_SNAPSHOT);
    }

    void resetSubFields() {
      snapshotRoot = null;
    }

    public DisallowSnapshotOp setSnapshotRoot(String snapRoot) {
      snapshotRoot = snapRoot;
      return this;
    }

    @Override
    void readFields(DataInputStream in, int logVersion) throws IOException {
      snapshotRoot = FSImageSerialization.readString(in);
    }

    @Override
    public void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(snapshotRoot, out);
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "SNAPSHOTROOT", snapshotRoot);
    }

    @Override
    void fromXml(Stanza st) throws InvalidXmlException {
      snapshotRoot = st.getValue("SNAPSHOTROOT");
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("DisallowSnapshotOp [snapshotRoot=");
      builder.append(snapshotRoot);
      builder.append("]");
      return builder.toString();
    }
  }

  /**
   * {@literal @AtMostOnce} for
   * {@link ClientProtocol#addCacheDirective}
   */
  static class AddCacheDirectiveInfoOp extends FSEditLogOp {
    CacheDirectiveInfo directive;

    public AddCacheDirectiveInfoOp() {
      super(OP_ADD_CACHE_DIRECTIVE);
    }

    static AddCacheDirectiveInfoOp getInstance(OpInstanceCache cache) {
      return (AddCacheDirectiveInfoOp) cache
          .get(OP_ADD_CACHE_DIRECTIVE);
    }

    @Override
    void resetSubFields() {
      directive = null;
    }

    public AddCacheDirectiveInfoOp setDirective(
        CacheDirectiveInfo directive) {
      this.directive = directive;
      assert(directive.getId() != null);
      assert(directive.getPath() != null);
      assert(directive.getReplication() != null);
      assert(directive.getPool() != null);
      assert(directive.getExpiration() != null);
      return this;
    }

    @Override
    void readFields(DataInputStream in, int logVersion) throws IOException {
      directive = FSImageSerialization.readCacheDirectiveInfo(in);
      readRpcIds(in, logVersion);
    }

    @Override
    public void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeCacheDirectiveInfo(out, directive);
      writeRpcIds(rpcClientId, rpcCallId, out);
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      FSImageSerialization.writeCacheDirectiveInfo(contentHandler, directive);
      appendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
    }

    @Override
    void fromXml(Stanza st) throws InvalidXmlException {
      directive = FSImageSerialization.readCacheDirectiveInfo(st);
      readRpcIdsFromXml(st);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("AddCacheDirectiveInfo [");
      builder.append("id=" + directive.getId() + ",");
      builder.append("path=" + directive.getPath().toUri().getPath() + ",");
      builder.append("replication=" + directive.getReplication() + ",");
      builder.append("pool=" + directive.getPool() + ",");
      builder.append("expiration=" + directive.getExpiration().getMillis());
      appendRpcIdsToString(builder, rpcClientId, rpcCallId);
      builder.append("]");
      return builder.toString();
    }
  }

  /**
   * {@literal @AtMostOnce} for
   * {@link ClientProtocol#modifyCacheDirective}
   */
  static class ModifyCacheDirectiveInfoOp extends FSEditLogOp {
    CacheDirectiveInfo directive;

    public ModifyCacheDirectiveInfoOp() {
      super(OP_MODIFY_CACHE_DIRECTIVE);
    }

    static ModifyCacheDirectiveInfoOp getInstance(OpInstanceCache cache) {
      return (ModifyCacheDirectiveInfoOp) cache
          .get(OP_MODIFY_CACHE_DIRECTIVE);
    }

    @Override
    void resetSubFields() {
      directive = null;
    }

    public ModifyCacheDirectiveInfoOp setDirective(
        CacheDirectiveInfo directive) {
      this.directive = directive;
      assert(directive.getId() != null);
      return this;
    }

    @Override
    void readFields(DataInputStream in, int logVersion) throws IOException {
      this.directive = FSImageSerialization.readCacheDirectiveInfo(in);
      readRpcIds(in, logVersion);
    }

    @Override
    public void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeCacheDirectiveInfo(out, directive);
      writeRpcIds(rpcClientId, rpcCallId, out);
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      FSImageSerialization.writeCacheDirectiveInfo(contentHandler, directive);
      appendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
    }

    @Override
    void fromXml(Stanza st) throws InvalidXmlException {
      this.directive = FSImageSerialization.readCacheDirectiveInfo(st);
      readRpcIdsFromXml(st);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("ModifyCacheDirectiveInfoOp[");
      builder.append("id=").append(directive.getId());
      if (directive.getPath() != null) {
        builder.append(",").append("path=").append(directive.getPath());
      }
      if (directive.getReplication() != null) {
        builder.append(",").append("replication=").
            append(directive.getReplication());
      }
      if (directive.getPool() != null) {
        builder.append(",").append("pool=").append(directive.getPool());
      }
      if (directive.getExpiration() != null) {
        builder.append(",").append("expiration=").
            append(directive.getExpiration().getMillis());
      }
      appendRpcIdsToString(builder, rpcClientId, rpcCallId);
      builder.append("]");
      return builder.toString();
    }
  }

  /**
   * {@literal @AtMostOnce} for
   * {@link ClientProtocol#removeCacheDirective}
   */
  static class RemoveCacheDirectiveInfoOp extends FSEditLogOp {
    long id;

    public RemoveCacheDirectiveInfoOp() {
      super(OP_REMOVE_CACHE_DIRECTIVE);
    }

    static RemoveCacheDirectiveInfoOp getInstance(OpInstanceCache cache) {
      return (RemoveCacheDirectiveInfoOp) cache
          .get(OP_REMOVE_CACHE_DIRECTIVE);
    }

    @Override
    void resetSubFields() {
      id = 0L;
    }

    public RemoveCacheDirectiveInfoOp setId(long id) {
      this.id = id;
      return this;
    }

    @Override
    void readFields(DataInputStream in, int logVersion) throws IOException {
      this.id = FSImageSerialization.readLong(in);
      readRpcIds(in, logVersion);
    }

    @Override
    public void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeLong(id, out);
      writeRpcIds(rpcClientId, rpcCallId, out);
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "ID", Long.toString(id));
      appendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
    }

    @Override
    void fromXml(Stanza st) throws InvalidXmlException {
      this.id = Long.parseLong(st.getValue("ID"));
      readRpcIdsFromXml(st);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("RemoveCacheDirectiveInfo [");
      builder.append("id=" + Long.toString(id));
      appendRpcIdsToString(builder, rpcClientId, rpcCallId);
      builder.append("]");
      return builder.toString();
    }
  }

  /** {@literal @AtMostOnce} for {@link ClientProtocol#addCachePool} */
  static class AddCachePoolOp extends FSEditLogOp {
    CachePoolInfo info;

    public AddCachePoolOp() {
      super(OP_ADD_CACHE_POOL);
    }

    static AddCachePoolOp getInstance(OpInstanceCache cache) {
      return (AddCachePoolOp) cache.get(OP_ADD_CACHE_POOL);
    }

    @Override
    void resetSubFields() {
      info = null;
    }

    public AddCachePoolOp setPool(CachePoolInfo info) {
      this.info = info;
      assert(info.getPoolName() != null);
      assert(info.getOwnerName() != null);
      assert(info.getGroupName() != null);
      assert(info.getMode() != null);
      assert(info.getLimit() != null);
      return this;
    }

    @Override
    void readFields(DataInputStream in, int logVersion) throws IOException {
      info = FSImageSerialization.readCachePoolInfo(in);
      readRpcIds(in, logVersion);
    }

    @Override
    public void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeCachePoolInfo(out, info);
      writeRpcIds(rpcClientId, rpcCallId, out);
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      FSImageSerialization.writeCachePoolInfo(contentHandler, info);
      appendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
    }

    @Override
    void fromXml(Stanza st) throws InvalidXmlException {
      this.info = FSImageSerialization.readCachePoolInfo(st);
      readRpcIdsFromXml(st);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("AddCachePoolOp [");
      builder.append("poolName=" + info.getPoolName() + ",");
      builder.append("ownerName=" + info.getOwnerName() + ",");
      builder.append("groupName=" + info.getGroupName() + ",");
      builder.append("mode=" + Short.toString(info.getMode().toShort()) + ",");
      builder.append("limit=" + Long.toString(info.getLimit()));
      appendRpcIdsToString(builder, rpcClientId, rpcCallId);
      builder.append("]");
      return builder.toString();
    }
  }

  /** {@literal @AtMostOnce} for {@link ClientProtocol#modifyCachePool} */
  static class ModifyCachePoolOp extends FSEditLogOp {
    CachePoolInfo info;

    public ModifyCachePoolOp() {
      super(OP_MODIFY_CACHE_POOL);
    }

    static ModifyCachePoolOp getInstance(OpInstanceCache cache) {
      return (ModifyCachePoolOp) cache.get(OP_MODIFY_CACHE_POOL);
    }

    @Override
    void resetSubFields() {
      info = null;
    }

    public ModifyCachePoolOp setInfo(CachePoolInfo info) {
      this.info = info;
      return this;
    }

    @Override
    void readFields(DataInputStream in, int logVersion) throws IOException {
      info = FSImageSerialization.readCachePoolInfo(in);
      readRpcIds(in, logVersion);
    }

    @Override
    public void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeCachePoolInfo(out, info);
      writeRpcIds(rpcClientId, rpcCallId, out);
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      FSImageSerialization.writeCachePoolInfo(contentHandler, info);
      appendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
    }

    @Override
    void fromXml(Stanza st) throws InvalidXmlException {
      this.info = FSImageSerialization.readCachePoolInfo(st);
      readRpcIdsFromXml(st);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("ModifyCachePoolOp [");
      ArrayList<String> fields = new ArrayList<String>(5);
      if (info.getPoolName() != null) {
        fields.add("poolName=" + info.getPoolName());
      }
      if (info.getOwnerName() != null) {
        fields.add("ownerName=" + info.getOwnerName());
      }
      if (info.getGroupName() != null) {
        fields.add("groupName=" + info.getGroupName());
      }
      if (info.getMode() != null) {
        fields.add("mode=" + info.getMode().toString());
      }
      if (info.getLimit() != null) {
        fields.add("limit=" + info.getLimit());
      }
      builder.append(Joiner.on(",").join(fields));
      appendRpcIdsToString(builder, rpcClientId, rpcCallId);
      builder.append("]");
      return builder.toString();
    }
  }

  /** {@literal @AtMostOnce} for {@link ClientProtocol#removeCachePool} */
  static class RemoveCachePoolOp extends FSEditLogOp {
    String poolName;

    public RemoveCachePoolOp() {
      super(OP_REMOVE_CACHE_POOL);
    }

    static RemoveCachePoolOp getInstance(OpInstanceCache cache) {
      return (RemoveCachePoolOp) cache.get(OP_REMOVE_CACHE_POOL);
    }

    @Override
    void resetSubFields() {
      poolName = null;
    }

    public RemoveCachePoolOp setPoolName(String poolName) {
      this.poolName = poolName;
      return this;
    }

    @Override
    void readFields(DataInputStream in, int logVersion) throws IOException {
      poolName = FSImageSerialization.readString(in);
      readRpcIds(in, logVersion);
    }

    @Override
    public void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(poolName, out);
      writeRpcIds(rpcClientId, rpcCallId, out);
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "POOLNAME", poolName);
      appendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
    }

    @Override
    void fromXml(Stanza st) throws InvalidXmlException {
      this.poolName = st.getValue("POOLNAME");
      readRpcIdsFromXml(st);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("RemoveCachePoolOp [");
      builder.append("poolName=" + poolName);
      appendRpcIdsToString(builder, rpcClientId, rpcCallId);
      builder.append("]");
      return builder.toString();
    }
  }
  
  static class RemoveXAttrOp extends FSEditLogOp {
    List<XAttr> xAttrs;
    String src;
    
    private RemoveXAttrOp() {
      super(OP_REMOVE_XATTR);
    }
    
    static RemoveXAttrOp getInstance() {
      return new RemoveXAttrOp();
    }

    @Override
    void resetSubFields() {
      xAttrs = null;
      src = null;
    }

    @Override
    void readFields(DataInputStream in, int logVersion) throws IOException {
      XAttrEditLogProto p = XAttrEditLogProto.parseDelimitedFrom(in);
      src = p.getSrc();
      xAttrs = PBHelper.convertXAttrs(p.getXAttrsList());
      readRpcIds(in, logVersion);
    }

    @Override
    public void writeFields(DataOutputStream out) throws IOException {
      XAttrEditLogProto.Builder b = XAttrEditLogProto.newBuilder();
      if (src != null) {
        b.setSrc(src);
      }
      b.addAllXAttrs(PBHelper.convertXAttrProto(xAttrs));
      b.build().writeDelimitedTo(out);
      // clientId and callId
      writeRpcIds(rpcClientId, rpcCallId, out);
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "SRC", src);
      appendXAttrsToXml(contentHandler, xAttrs);
      appendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
    }

    @Override
    void fromXml(Stanza st) throws InvalidXmlException {
      src = st.getValue("SRC");
      xAttrs = readXAttrsFromXml(st);
      readRpcIdsFromXml(st);
    }
  }
  
  static class SetXAttrOp extends FSEditLogOp {
    List<XAttr> xAttrs;
    String src;
    
    private SetXAttrOp() {
      super(OP_SET_XATTR);
    }
    
    static SetXAttrOp getInstance() {
      return new SetXAttrOp();
    }

    @Override
    void resetSubFields() {
      xAttrs = null;
      src = null;
    }

    @Override
    void readFields(DataInputStream in, int logVersion) throws IOException {
      XAttrEditLogProto p = XAttrEditLogProto.parseDelimitedFrom(in);
      src = p.getSrc();
      xAttrs = PBHelper.convertXAttrs(p.getXAttrsList());
      readRpcIds(in, logVersion);
    }

    @Override
    public void writeFields(DataOutputStream out) throws IOException {
      XAttrEditLogProto.Builder b = XAttrEditLogProto.newBuilder();
      if (src != null) {
        b.setSrc(src);
      }
      b.addAllXAttrs(PBHelper.convertXAttrProto(xAttrs));
      b.build().writeDelimitedTo(out);
      // clientId and callId
      writeRpcIds(rpcClientId, rpcCallId, out);
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "SRC", src);
      appendXAttrsToXml(contentHandler, xAttrs);
      appendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
    }

    @Override
    void fromXml(Stanza st) throws InvalidXmlException {
      src = st.getValue("SRC");
      xAttrs = readXAttrsFromXml(st);
      readRpcIdsFromXml(st);
    }
  }

  static class SetAclOp extends FSEditLogOp {
    List<AclEntry> aclEntries = Lists.newArrayList();
    String src;

    private SetAclOp() {
      super(OP_SET_ACL);
    }

    static SetAclOp getInstance() {
      return new SetAclOp();
    }

    @Override
    void resetSubFields() {
      aclEntries = null;
      src = null;
    }

    @Override
    void readFields(DataInputStream in, int logVersion) throws IOException {
      AclEditLogProto p = AclEditLogProto.parseDelimitedFrom(in);
      if (p == null) {
        throw new IOException("Failed to read fields from SetAclOp");
      }
      src = p.getSrc();
      aclEntries = PBHelper.convertAclEntry(p.getEntriesList());
    }

    @Override
    public void writeFields(DataOutputStream out) throws IOException {
      AclEditLogProto.Builder b = AclEditLogProto.newBuilder();
      if (src != null)
        b.setSrc(src);
      b.addAllEntries(PBHelper.convertAclEntryProto(aclEntries));
      b.build().writeDelimitedTo(out);
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "SRC", src);
      appendAclEntriesToXml(contentHandler, aclEntries);
    }

    @Override
    void fromXml(Stanza st) throws InvalidXmlException {
      src = st.getValue("SRC");
      aclEntries = readAclEntriesFromXml(st);
      if (aclEntries == null) {
        aclEntries = Lists.newArrayList();
      }
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
           @Override
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
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(blkid);
      out.writeLong(len);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.blkid = in.readLong();
      this.len = in.readLong();
    }
  }
  /**
   * Operation corresponding to upgrade
   */
  static class RollingUpgradeOp extends FSEditLogOp { // @Idempotent
    private final String name;
    private long time;

    public RollingUpgradeOp(FSEditLogOpCodes code, String name) {
      super(code);
      this.name = StringUtils.toUpperCase(name);
    }

    static RollingUpgradeOp getStartInstance(OpInstanceCache cache) {
      return (RollingUpgradeOp) cache.get(OP_ROLLING_UPGRADE_START);
    }

    static RollingUpgradeOp getFinalizeInstance(OpInstanceCache cache) {
      return (RollingUpgradeOp) cache.get(OP_ROLLING_UPGRADE_FINALIZE);
    }

    @Override
    void resetSubFields() {
      time = 0L;
    }

    long getTime() {
      return time;
    }

    void setTime(long time) {
      this.time = time;
    }

    @Override
    void readFields(DataInputStream in, int logVersion) throws IOException {
      time = in.readLong();
    }

    @Override
    public void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeLong(time, out);
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, name + "TIME",
          Long.toString(time));
    }

    @Override
    void fromXml(Stanza st) throws InvalidXmlException {
      this.time = Long.parseLong(st.getValue(name + "TIME"));
    }

    @Override
    public String toString() {
      return new StringBuilder().append("RollingUpgradeOp [").append(name)
          .append(", time=").append(time).append("]").toString();
    }
    
    static class RollbackException extends IOException {
      private static final long serialVersionUID = 1L;
    }
  }

  /** {@literal @Idempotent} for {@link ClientProtocol#setStoragePolicy} */
  static class SetStoragePolicyOp extends FSEditLogOp {
    String path;
    byte policyId;

    private SetStoragePolicyOp() {
      super(OP_SET_STORAGE_POLICY);
    }

    static SetStoragePolicyOp getInstance(OpInstanceCache cache) {
      return (SetStoragePolicyOp) cache.get(OP_SET_STORAGE_POLICY);
    }

    @Override
    void resetSubFields() {
      path = null;
      policyId = 0;
    }

    SetStoragePolicyOp setPath(String path) {
      this.path = path;
      return this;
    }

    SetStoragePolicyOp setPolicyId(byte policyId) {
      this.policyId = policyId;
      return this;
    }

    @Override
    public void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(path, out);
      out.writeByte(policyId);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.path = FSImageSerialization.readString(in);
      this.policyId = in.readByte();
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("SetStoragePolicyOp [path=");
      builder.append(path);
      builder.append(", policyId=");
      builder.append(policyId);
      builder.append(", opCode=");
      builder.append(opCode);
      builder.append(", txid=");
      builder.append(txid);
      builder.append("]");
      return builder.toString();
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "PATH", path);
      XMLUtils.addSaxString(contentHandler, "POLICYID",
          Byte.valueOf(policyId).toString());
    }

    @Override
    void fromXml(Stanza st) throws InvalidXmlException {
      this.path = st.getValue("PATH");
      this.policyId = Byte.valueOf(st.getValue("POLICYID"));
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
      this.checksum = DataChecksum.newCrc32();
    }

    /**
     * Write an operation to the output stream
     * 
     * @param op The operation to write
     * @throws IOException if an error occurs during writing.
     */
    public void writeOp(FSEditLogOp op) throws IOException {
      int start = buf.getLength();
      // write the op code first to make padding and terminator verification
      // work
      buf.writeByte(op.opCode.getOpCode());
      buf.writeInt(0); // write 0 for the length first
      buf.writeLong(op.txid);
      op.writeFields(buf);
      int end = buf.getLength();
      
      // write the length back: content of the op + 4 bytes checksum - op_code
      int length = end - start - 1;
      buf.writeInt(length, start + 1);

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
    private final StreamLimiter limiter;
    private final int logVersion;
    private final Checksum checksum;
    private final OpInstanceCache cache;
    private int maxOpSize;
    private final boolean supportEditLogLength;

    /**
     * Construct the reader
     * @param in The stream to read from.
     * @param logVersion The version of the data coming from the stream.
     */
    public Reader(DataInputStream in, StreamLimiter limiter, int logVersion) {
      this.logVersion = logVersion;
      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.EDITS_CHESKUM, logVersion)) {
        this.checksum = DataChecksum.newCrc32();
      } else {
        this.checksum = null;
      }
      // It is possible that the logVersion is actually a future layoutversion
      // during the rolling upgrade (e.g., the NN gets upgraded first). We
      // assume future layout will also support length of editlog op.
      this.supportEditLogLength = NameNodeLayoutVersion.supports(
          NameNodeLayoutVersion.Feature.EDITLOG_LENGTH, logVersion)
          || logVersion < NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION;

      if (this.checksum != null) {
        this.in = new DataInputStream(
            new CheckedInputStream(in, this.checksum));
      } else {
        this.in = in;
      }
      this.limiter = limiter;
      this.cache = new OpInstanceCache();
      this.maxOpSize = DFSConfigKeys.DFS_NAMENODE_MAX_OP_SIZE_DEFAULT;
    }

    public void setMaxOpSize(int maxOpSize) {
      this.maxOpSize = maxOpSize;
    }

    /**
     * Read an operation from the input stream.
     * 
     * Note that the objects returned from this method may be re-used by future
     * calls to the same method.
     * 
     * @param skipBrokenEdits    If true, attempt to skip over damaged parts of
     * the input stream, rather than throwing an IOException
     * @return the operation read from the stream, or null at the end of the 
     *         file
     * @throws IOException on error.  This function should only throw an
     *         exception when skipBrokenEdits is false.
     */
    public FSEditLogOp readOp(boolean skipBrokenEdits) throws IOException {
      while (true) {
        try {
          return decodeOp();
        } catch (IOException e) {
          in.reset();
          if (!skipBrokenEdits) {
            throw e;
          }
        } catch (RuntimeException e) {
          // FSEditLogOp#decodeOp is not supposed to throw RuntimeException.
          // However, we handle it here for recovery mode, just to be more
          // robust.
          in.reset();
          if (!skipBrokenEdits) {
            throw e;
          }
        } catch (Throwable e) {
          in.reset();
          if (!skipBrokenEdits) {
            throw new IOException("got unexpected exception " +
                e.getMessage(), e);
          }
        }
        // Move ahead one byte and re-try the decode process.
        if (in.skip(1) < 1) {
          return null;
        }
      }
    }

    private void verifyTerminator() throws IOException {
      /** The end of the edit log should contain only 0x00 or 0xff bytes.
       * If it contains other bytes, the log itself may be corrupt.
       * It is important to check this; if we don't, a stray OP_INVALID byte 
       * could make us stop reading the edit log halfway through, and we'd never
       * know that we had lost data.
       */
      byte[] buf = new byte[4096];
      limiter.clearLimit();
      int numRead = -1, idx = 0;
      while (true) {
        try {
          numRead = -1;
          idx = 0;
          numRead = in.read(buf);
          if (numRead == -1) {
            return;
          }
          while (idx < numRead) {
            if ((buf[idx] != (byte)0) && (buf[idx] != (byte)-1)) {
              throw new IOException("Read extra bytes after " +
                "the terminator!");
            }
            idx++;
          }
        } finally {
          // After reading each group of bytes, we reposition the mark one
          // byte before the next group.  Similarly, if there is an error, we
          // want to reposition the mark one byte before the error
          if (numRead != -1) { 
            in.reset();
            IOUtils.skipFully(in, idx);
            in.mark(buf.length + 1);
            IOUtils.skipFully(in, 1);
          }
        }
      }
    }

    /**
     * Read an opcode from the input stream.
     *
     * @return   the opcode, or null on EOF.
     *
     * If an exception is thrown, the stream's mark will be set to the first
     * problematic byte.  This usually means the beginning of the opcode.
     */
    private FSEditLogOp decodeOp() throws IOException {
      limiter.setLimit(maxOpSize);
      in.mark(maxOpSize);

      if (checksum != null) {
        checksum.reset();
      }

      byte opCodeByte;
      try {
        opCodeByte = in.readByte();
      } catch (EOFException eof) {
        // EOF at an opcode boundary is expected.
        return null;
      }

      FSEditLogOpCodes opCode = FSEditLogOpCodes.fromByte(opCodeByte);
      if (opCode == OP_INVALID) {
        verifyTerminator();
        return null;
      }

      FSEditLogOp op = cache.get(opCode);
      if (op == null) {
        throw new IOException("Read invalid opcode " + opCode);
      }

      if (supportEditLogLength) {
        in.readInt();
      }

      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.STORED_TXIDS, logVersion)) {
        // Read the txid
        op.setTransactionId(in.readLong());
      } else {
        op.setTransactionId(HdfsConstants.INVALID_TXID);
      }

      op.readFields(in, logVersion);

      validateChecksum(in, checksum, op.txid);
      return op;
    }

    /**
     * Similar with decodeOp(), but instead of doing the real decoding, we skip
     * the content of the op if the length of the editlog is supported.
     * @return the last txid of the segment, or INVALID_TXID on exception
     */
    public long scanOp() throws IOException {
      if (supportEditLogLength) {
        limiter.setLimit(maxOpSize);
        in.mark(maxOpSize);

        final byte opCodeByte;
        try {
          opCodeByte = in.readByte(); // op code
        } catch (EOFException e) {
          return HdfsConstants.INVALID_TXID;
        }

        FSEditLogOpCodes opCode = FSEditLogOpCodes.fromByte(opCodeByte);
        if (opCode == OP_INVALID) {
          verifyTerminator();
          return HdfsConstants.INVALID_TXID;
        }

        int length = in.readInt(); // read the length of the op
        long txid = in.readLong(); // read the txid

        // skip the remaining content
        IOUtils.skipFully(in, length - 8); 
        // TODO: do we want to verify checksum for JN? For now we don't.
        return txid;
      } else {
        FSEditLogOp op = decodeOp();
        return op == null ? HdfsConstants.INVALID_TXID : op.getTransactionId();
      }
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

  public void outputToXml(ContentHandler contentHandler) throws SAXException {
    contentHandler.startElement("", "", "RECORD", new AttributesImpl());
    XMLUtils.addSaxString(contentHandler, "OPCODE", opCode.toString());
    contentHandler.startElement("", "", "DATA", new AttributesImpl());
    XMLUtils.addSaxString(contentHandler, "TXID", "" + txid);
    toXml(contentHandler);
    contentHandler.endElement("", "", "DATA");
    contentHandler.endElement("", "", "RECORD");
  }

  protected abstract void toXml(ContentHandler contentHandler)
      throws SAXException;
  
  abstract void fromXml(Stanza st) throws InvalidXmlException;
  
  public void decodeXml(Stanza st) throws InvalidXmlException {
    this.txid = Long.parseLong(st.getValue("TXID"));
    fromXml(st);
  }
  
  public static void blockToXml(ContentHandler contentHandler, Block block) 
      throws SAXException {
    contentHandler.startElement("", "", "BLOCK", new AttributesImpl());
    XMLUtils.addSaxString(contentHandler, "BLOCK_ID",
        Long.toString(block.getBlockId()));
    XMLUtils.addSaxString(contentHandler, "NUM_BYTES",
        Long.toString(block.getNumBytes()));
    XMLUtils.addSaxString(contentHandler, "GENSTAMP",
        Long.toString(block.getGenerationStamp()));
    contentHandler.endElement("", "", "BLOCK");
  }

  public static Block blockFromXml(Stanza st)
      throws InvalidXmlException {
    long blockId = Long.parseLong(st.getValue("BLOCK_ID"));
    long numBytes = Long.parseLong(st.getValue("NUM_BYTES"));
    long generationStamp = Long.parseLong(st.getValue("GENSTAMP"));
    return new Block(blockId, numBytes, generationStamp);
  }

  public static void delegationTokenToXml(ContentHandler contentHandler,
      DelegationTokenIdentifier token) throws SAXException {
    contentHandler.startElement("", "", "DELEGATION_TOKEN_IDENTIFIER", new AttributesImpl());
    XMLUtils.addSaxString(contentHandler, "KIND", token.getKind().toString());
    XMLUtils.addSaxString(contentHandler, "SEQUENCE_NUMBER",
        Integer.toString(token.getSequenceNumber()));
    XMLUtils.addSaxString(contentHandler, "OWNER",
        token.getOwner().toString());
    XMLUtils.addSaxString(contentHandler, "RENEWER",
        token.getRenewer().toString());
    XMLUtils.addSaxString(contentHandler, "REALUSER",
        token.getRealUser().toString());
    XMLUtils.addSaxString(contentHandler, "ISSUE_DATE",
        Long.toString(token.getIssueDate()));
    XMLUtils.addSaxString(contentHandler, "MAX_DATE",
        Long.toString(token.getMaxDate()));
    XMLUtils.addSaxString(contentHandler, "MASTER_KEY_ID",
        Integer.toString(token.getMasterKeyId()));
    contentHandler.endElement("", "", "DELEGATION_TOKEN_IDENTIFIER");
  }

  public static DelegationTokenIdentifier delegationTokenFromXml(Stanza st)
      throws InvalidXmlException {
    String kind = st.getValue("KIND");
    if (!kind.equals(DelegationTokenIdentifier.
        HDFS_DELEGATION_KIND.toString())) {
      throw new InvalidXmlException("can't understand " +
        "DelegationTokenIdentifier KIND " + kind);
    }
    int seqNum = Integer.parseInt(st.getValue("SEQUENCE_NUMBER"));
    String owner = st.getValue("OWNER");
    String renewer = st.getValue("RENEWER");
    String realuser = st.getValue("REALUSER");
    long issueDate = Long.parseLong(st.getValue("ISSUE_DATE"));
    long maxDate = Long.parseLong(st.getValue("MAX_DATE"));
    int masterKeyId = Integer.parseInt(st.getValue("MASTER_KEY_ID"));
    DelegationTokenIdentifier token =
        new DelegationTokenIdentifier(new Text(owner),
            new Text(renewer), new Text(realuser));
    token.setSequenceNumber(seqNum);
    token.setIssueDate(issueDate);
    token.setMaxDate(maxDate);
    token.setMasterKeyId(masterKeyId);
    return token;
  }

  public static void delegationKeyToXml(ContentHandler contentHandler,
      DelegationKey key) throws SAXException {
    contentHandler.startElement("", "", "DELEGATION_KEY", new AttributesImpl());
    XMLUtils.addSaxString(contentHandler, "KEY_ID",
        Integer.toString(key.getKeyId()));
    XMLUtils.addSaxString(contentHandler, "EXPIRY_DATE",
        Long.toString(key.getExpiryDate()));
    if (key.getEncodedKey() != null) {
      XMLUtils.addSaxString(contentHandler, "KEY",
          Hex.encodeHexString(key.getEncodedKey()));
    }
    contentHandler.endElement("", "", "DELEGATION_KEY");
  }
  
  public static DelegationKey delegationKeyFromXml(Stanza st)
      throws InvalidXmlException {
    int keyId = Integer.parseInt(st.getValue("KEY_ID"));
    long expiryDate = Long.parseLong(st.getValue("EXPIRY_DATE"));
    byte key[] = null;
    try {
      key = Hex.decodeHex(st.getValue("KEY").toCharArray());
    } catch (DecoderException e) {
      throw new InvalidXmlException(e.toString());
    } catch (InvalidXmlException e) {
    }
    return new DelegationKey(keyId, expiryDate, key);
  }

  public static void permissionStatusToXml(ContentHandler contentHandler,
      PermissionStatus perm) throws SAXException {
    contentHandler.startElement("", "", "PERMISSION_STATUS", new AttributesImpl());
    XMLUtils.addSaxString(contentHandler, "USERNAME", perm.getUserName());
    XMLUtils.addSaxString(contentHandler, "GROUPNAME", perm.getGroupName());
    fsPermissionToXml(contentHandler, perm.getPermission());
    contentHandler.endElement("", "", "PERMISSION_STATUS");
  }

  public static PermissionStatus permissionStatusFromXml(Stanza st)
      throws InvalidXmlException {
    Stanza status = st.getChildren("PERMISSION_STATUS").get(0);
    String username = status.getValue("USERNAME");
    String groupname = status.getValue("GROUPNAME");
    FsPermission mode = fsPermissionFromXml(status);
    return new PermissionStatus(username, groupname, mode);
  }

  public static void fsPermissionToXml(ContentHandler contentHandler,
      FsPermission mode) throws SAXException {
    XMLUtils.addSaxString(contentHandler, "MODE", Short.valueOf(mode.toShort())
        .toString());
  }

  public static FsPermission fsPermissionFromXml(Stanza st)
      throws InvalidXmlException {
    short mode = Short.valueOf(st.getValue("MODE"));
    return new FsPermission(mode);
  }

  private static void fsActionToXml(ContentHandler contentHandler, FsAction v)
      throws SAXException {
    XMLUtils.addSaxString(contentHandler, "PERM", v.SYMBOL);
  }

  private static FsAction fsActionFromXml(Stanza st) throws InvalidXmlException {
    FsAction v = FSACTION_SYMBOL_MAP.get(st.getValue("PERM"));
    if (v == null)
      throw new InvalidXmlException("Invalid value for FsAction");
    return v;
  }

  private static void appendAclEntriesToXml(ContentHandler contentHandler,
      List<AclEntry> aclEntries) throws SAXException {
    for (AclEntry e : aclEntries) {
      contentHandler.startElement("", "", "ENTRY", new AttributesImpl());
      XMLUtils.addSaxString(contentHandler, "SCOPE", e.getScope().name());
      XMLUtils.addSaxString(contentHandler, "TYPE", e.getType().name());
      if (e.getName() != null) {
        XMLUtils.addSaxString(contentHandler, "NAME", e.getName());
      }
      fsActionToXml(contentHandler, e.getPermission());
      contentHandler.endElement("", "", "ENTRY");
    }
  }

  private static List<AclEntry> readAclEntriesFromXml(Stanza st) {
    List<AclEntry> aclEntries = Lists.newArrayList();
    if (!st.hasChildren("ENTRY"))
      return null;

    List<Stanza> stanzas = st.getChildren("ENTRY");
    for (Stanza s : stanzas) {
      AclEntry e = new AclEntry.Builder()
        .setScope(AclEntryScope.valueOf(s.getValue("SCOPE")))
        .setType(AclEntryType.valueOf(s.getValue("TYPE")))
        .setName(s.getValueOrNull("NAME"))
        .setPermission(fsActionFromXml(s)).build();
      aclEntries.add(e);
    }
    return aclEntries;
  }

  private static void appendXAttrsToXml(ContentHandler contentHandler,
      List<XAttr> xAttrs) throws SAXException {
    for (XAttr xAttr: xAttrs) {
      contentHandler.startElement("", "", "XATTR", new AttributesImpl());
      XMLUtils.addSaxString(contentHandler, "NAMESPACE",
          xAttr.getNameSpace().toString());
      XMLUtils.addSaxString(contentHandler, "NAME", xAttr.getName());
      if (xAttr.getValue() != null) {
        try {
          XMLUtils.addSaxString(contentHandler, "VALUE",
              XAttrCodec.encodeValue(xAttr.getValue(), XAttrCodec.HEX));
        } catch (IOException e) {
          throw new SAXException(e);
        }
      }
      contentHandler.endElement("", "", "XATTR");
    }
  }

  private static List<XAttr> readXAttrsFromXml(Stanza st)
      throws InvalidXmlException {
    if (!st.hasChildren("XATTR")) {
      return null;
    }

    List<Stanza> stanzas = st.getChildren("XATTR");
    List<XAttr> xattrs = Lists.newArrayListWithCapacity(stanzas.size());
    for (Stanza a: stanzas) {
      XAttr.Builder builder = new XAttr.Builder();
      builder.setNameSpace(XAttr.NameSpace.valueOf(a.getValue("NAMESPACE"))).
          setName(a.getValue("NAME"));
      String v = a.getValueOrNull("VALUE");
      if (v != null) {
        try {
          builder.setValue(XAttrCodec.decodeValue(v));
        } catch (IOException e) {
          throw new InvalidXmlException(e.toString());
        }
      }
      xattrs.add(builder.build());
    }
    return xattrs;
  }
}
