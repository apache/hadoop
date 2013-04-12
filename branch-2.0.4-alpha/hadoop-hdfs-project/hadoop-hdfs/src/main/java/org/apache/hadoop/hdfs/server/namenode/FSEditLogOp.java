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
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.util.PureJavaCrc32;

import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.*;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.hdfs.util.XMLUtils;
import org.apache.hadoop.hdfs.util.XMLUtils.InvalidXmlException;
import org.apache.hadoop.hdfs.util.XMLUtils.Stanza;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DeprecatedUTF8;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import com.google.common.base.Preconditions;

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
  public final FSEditLogOpCodes opCode;
  long txid;

  @SuppressWarnings("deprecation")
  final public static class OpInstanceCache {
    private EnumMap<FSEditLogOpCodes, FSEditLogOp> inst = 
        new EnumMap<FSEditLogOpCodes, FSEditLogOp>(FSEditLogOpCodes.class);
    
    public OpInstanceCache() {
      inst.put(OP_ADD, new AddOp());
      inst.put(OP_CLOSE, new CloseOp());
      inst.put(OP_SET_REPLICATION, new SetReplicationOp());
      inst.put(OP_CONCAT_DELETE, new ConcatDeleteOp());
      inst.put(OP_RENAME_OLD, new RenameOldOp());
      inst.put(OP_DELETE, new DeleteOp());
      inst.put(OP_MKDIR, new MkdirOp());
      inst.put(OP_SET_GENSTAMP, new SetGenstampOp());
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
      inst.put(OP_CANCEL_DELEGATION_TOKEN, 
                    new CancelDelegationTokenOp());
      inst.put(OP_UPDATE_MASTER_KEY, new UpdateMasterKeyOp());
      inst.put(OP_START_LOG_SEGMENT,
                    new LogSegmentOp(OP_START_LOG_SEGMENT));
      inst.put(OP_END_LOG_SEGMENT,
                    new LogSegmentOp(OP_END_LOG_SEGMENT));
      inst.put(OP_UPDATE_BLOCKS, new UpdateBlocksOp());
    }
    
    public FSEditLogOp get(FSEditLogOpCodes opcode) {
      return inst.get(opcode);
    }
  }

  /**
   * Constructor for an EditLog Op. EditLog ops cannot be constructed
   * directly, but only through Reader#readOp.
   */
  private FSEditLogOp(FSEditLogOpCodes opCode) {
    this.opCode = opCode;
    this.txid = HdfsConstants.INVALID_TXID;
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

  abstract void readFields(DataInputStream in, int logVersion)
      throws IOException;

  public abstract void writeFields(DataOutputStream out)
      throws IOException;

  static interface BlockListUpdatingOp {
    Block[] getBlocks();
    String getPath();
    boolean shouldCompleteLastBlock();
  }
  
  @SuppressWarnings("unchecked")
  static abstract class AddCloseOp extends FSEditLogOp implements BlockListUpdatingOp {
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

    private AddCloseOp(FSEditLogOpCodes opCode) {
      super(opCode);
      assert(opCode == OP_ADD || opCode == OP_CLOSE);
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

    <T extends AddCloseOp> T setClientName(String clientName) {
      this.clientName = clientName;
      return (T)this;
    }

    <T extends AddCloseOp> T setClientMachine(String clientMachine) {
      this.clientMachine = clientMachine;
      return (T)this;
    }

    @Override
    public 
    void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(path, out);
      FSImageSerialization.writeShort(replication, out);
      FSImageSerialization.writeLong(mtime, out);
      FSImageSerialization.writeLong(atime, out);
      FSImageSerialization.writeLong(blockSize, out);
      new ArrayWritable(Block.class, blocks).write(out);
      permissions.write(out);

      if (this.opCode == OP_ADD) {
        FSImageSerialization.writeString(clientName,out);
        FSImageSerialization.writeString(clientMachine,out);
      }
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      if (!LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.length = in.readInt();
      }
      if ((-17 < logVersion && length != 4) ||
          (logVersion <= -17 && length != 5 && !LayoutVersion.supports(
              Feature.EDITLOG_OP_OPTIMIZATION, logVersion))) {
        throw new IOException("Incorrect data format."  +
                              " logVersion is " + logVersion +
                              " but writables.length is " +
                              length + ". ");
      }
      this.path = FSImageSerialization.readString(in);

      if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.replication = FSImageSerialization.readShort(in);
        this.mtime = FSImageSerialization.readLong(in);
      } else {
        this.replication = readShort(in);
        this.mtime = readLong(in);
      }

      if (LayoutVersion.supports(Feature.FILE_ACCESS_TIME, logVersion)) {
        if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
          this.atime = FSImageSerialization.readLong(in);
        } else {
          this.atime = readLong(in);
        }
      } else {
        this.atime = 0;
      }

      if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.blockSize = FSImageSerialization.readLong(in);
      } else {
        this.blockSize = readLong(in);
      }

      this.blocks = readBlocks(in, logVersion);
      this.permissions = PermissionStatus.read(in);

      // clientname, clientMachine and block locations of last block.
      if (this.opCode == OP_ADD) {
        this.clientName = FSImageSerialization.readString(in);
        this.clientMachine = FSImageSerialization.readString(in);
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
      builder.append(", clientName=");
      builder.append(clientName);
      builder.append(", clientMachine=");
      builder.append(clientMachine);
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
          Integer.valueOf(length).toString());
      XMLUtils.addSaxString(contentHandler, "PATH", path);
      XMLUtils.addSaxString(contentHandler, "REPLICATION",
          Short.valueOf(replication).toString());
      XMLUtils.addSaxString(contentHandler, "MTIME",
          Long.valueOf(mtime).toString());
      XMLUtils.addSaxString(contentHandler, "ATIME",
          Long.valueOf(atime).toString());
      XMLUtils.addSaxString(contentHandler, "BLOCKSIZE",
          Long.valueOf(blockSize).toString());
      XMLUtils.addSaxString(contentHandler, "CLIENT_NAME", clientName);
      XMLUtils.addSaxString(contentHandler, "CLIENT_MACHINE", clientMachine);
      for (Block b : blocks) {
        FSEditLogOp.blockToXml(contentHandler, b);
      }
      FSEditLogOp.permissionStatusToXml(contentHandler, permissions);
    }

    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.length = Integer.valueOf(st.getValue("LENGTH"));
      this.path = st.getValue("PATH");
      this.replication = Short.valueOf(st.getValue("REPLICATION"));
      this.mtime = Long.valueOf(st.getValue("MTIME"));
      this.atime = Long.valueOf(st.getValue("ATIME"));
      this.blockSize = Long.valueOf(st.getValue("BLOCKSIZE"));
      this.clientName = st.getValue("CLIENT_NAME");
      this.clientMachine = st.getValue("CLIENT_MACHINE");
      if (st.hasChildren("BLOCK")) {
        List<Stanza> blocks = st.getChildren("BLOCK");
        this.blocks = new Block[blocks.size()];
        for (int i = 0; i < blocks.size(); i++) {
          this.blocks[i] = FSEditLogOp.blockFromXml(blocks.get(i));
        }
      } else {
        this.blocks = new Block[0];
      }
      this.permissions =
          permissionStatusFromXml(st.getChildren("PERMISSION_STATUS").get(0));
    }
  }

  static class AddOp extends AddCloseOp {
    private AddOp() {
      super(OP_ADD);
    }

    static AddOp getInstance(OpInstanceCache cache) {
      return (AddOp)cache.get(OP_ADD);
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
  
  static class UpdateBlocksOp extends FSEditLogOp implements BlockListUpdatingOp {
    String path;
    Block[] blocks;
    
    private UpdateBlocksOp() {
      super(OP_UPDATE_BLOCKS);
    }
    
    static UpdateBlocksOp getInstance(OpInstanceCache cache) {
      return (UpdateBlocksOp)cache.get(OP_UPDATE_BLOCKS);
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
    }
    
    @Override
    void readFields(DataInputStream in, int logVersion) throws IOException {
      path = FSImageSerialization.readString(in);
      this.blocks = FSImageSerialization.readCompactBlockArray(
          in, logVersion);
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
        .append(Arrays.toString(blocks))
        .append("]");
      return sb.toString();
    }
    
    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
      XMLUtils.addSaxString(contentHandler, "PATH", path);
      for (Block b : blocks) {
        FSEditLogOp.blockToXml(contentHandler, b);
      }
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.path = st.getValue("PATH");
      List<Stanza> blocks = st.getChildren("BLOCK");
      this.blocks = new Block[blocks.size()];
      for (int i = 0; i < blocks.size(); i++) {
        this.blocks[i] = FSEditLogOp.blockFromXml(blocks.get(i));
      }
    }
  }

  static class SetReplicationOp extends FSEditLogOp {
    String path;
    short replication;

    private SetReplicationOp() {
      super(OP_SET_REPLICATION);
    }

    static SetReplicationOp getInstance(OpInstanceCache cache) {
      return (SetReplicationOp)cache.get(OP_SET_REPLICATION);
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
      if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
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
    public 
    void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(trg, out);
            
      DeprecatedUTF8 info[] = new DeprecatedUTF8[srcs.length];
      int idx = 0;
      for(int i=0; i<srcs.length; i++) {
        info[idx++] = new DeprecatedUTF8(srcs[i]);
      }
      new ArrayWritable(DeprecatedUTF8.class, info).write(out);

      FSImageSerialization.writeLong(timestamp, out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      if (!LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.length = in.readInt();
        if (length < 3) { // trg, srcs.., timestamp
          throw new IOException("Incorrect data format " +
              "for ConcatDeleteOp.");
        }
      }
      this.trg = FSImageSerialization.readString(in);
      int srcSize = 0;
      if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
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
      
      if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.timestamp = FSImageSerialization.readLong(in);
      } else {
        this.timestamp = readLong(in);
      }
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
          Integer.valueOf(length).toString());
      XMLUtils.addSaxString(contentHandler, "TRG", trg);
      XMLUtils.addSaxString(contentHandler, "TIMESTAMP",
          Long.valueOf(timestamp).toString());
      contentHandler.startElement("", "", "SOURCES", new AttributesImpl());
      for (int i = 0; i < srcs.length; ++i) {
        XMLUtils.addSaxString(contentHandler,
            "SOURCE" + (i + 1), srcs[i]);
      }
      contentHandler.endElement("", "", "SOURCES");
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.length = Integer.valueOf(st.getValue("LENGTH"));
      this.trg = st.getValue("TRG");
      this.timestamp = Long.valueOf(st.getValue("TIMESTAMP"));
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

    static RenameOldOp getInstance(OpInstanceCache cache) {
      return (RenameOldOp)cache.get(OP_RENAME_OLD);
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
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      if (!LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.length = in.readInt();
        if (this.length != 3) {
          throw new IOException("Incorrect data format. "
              + "Old rename operation.");
        }
      }
      this.src = FSImageSerialization.readString(in);
      this.dst = FSImageSerialization.readString(in);
      if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.timestamp = FSImageSerialization.readLong(in);
      } else {
        this.timestamp = readLong(in);
      }
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
          Integer.valueOf(length).toString());
      XMLUtils.addSaxString(contentHandler, "SRC", src);
      XMLUtils.addSaxString(contentHandler, "DST", dst);
      XMLUtils.addSaxString(contentHandler, "TIMESTAMP",
          Long.valueOf(timestamp).toString());
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.length = Integer.valueOf(st.getValue("LENGTH"));
      this.src = st.getValue("SRC");
      this.dst = st.getValue("DST");
      this.timestamp = Long.valueOf(st.getValue("TIMESTAMP"));
    }
  }

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
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      if (!LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.length = in.readInt();
        if (this.length != 2) {
          throw new IOException("Incorrect data format. " + "delete operation.");
        }
      }
      this.path = FSImageSerialization.readString(in);
      if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.timestamp = FSImageSerialization.readLong(in);
      } else {
        this.timestamp = readLong(in);
      }
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
          Integer.valueOf(length).toString());
      XMLUtils.addSaxString(contentHandler, "PATH", path);
      XMLUtils.addSaxString(contentHandler, "TIMESTAMP",
          Long.valueOf(timestamp).toString());
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.length = Integer.valueOf(st.getValue("LENGTH"));
      this.path = st.getValue("PATH");
      this.timestamp = Long.valueOf(st.getValue("TIMESTAMP"));
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
    
    static MkdirOp getInstance(OpInstanceCache cache) {
      return (MkdirOp)cache.get(OP_MKDIR);
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
    public 
    void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(path, out);
      FSImageSerialization.writeLong(timestamp, out); // mtime
      FSImageSerialization.writeLong(timestamp, out); // atime, unused at this
      permissions.write(out);
    }
    
    @Override
    void readFields(DataInputStream in, int logVersion) throws IOException {
      if (!LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.length = in.readInt();
      }
      if (-17 < logVersion && length != 2 ||
          logVersion <= -17 && length != 3
          && !LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        throw new IOException("Incorrect data format. Mkdir operation.");
      }
      this.path = FSImageSerialization.readString(in);
      if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.timestamp = FSImageSerialization.readLong(in);
      } else {
        this.timestamp = readLong(in);
      }

      // The disk format stores atimes for directories as well.
      // However, currently this is not being updated/used because of
      // performance reasons.
      if (LayoutVersion.supports(Feature.FILE_ACCESS_TIME, logVersion)) {
        if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
          FSImageSerialization.readLong(in);
        } else {
          readLong(in);
        }
      }

      this.permissions = PermissionStatus.read(in);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("MkdirOp [length=");
      builder.append(length);
      builder.append(", path=");
      builder.append(path);
      builder.append(", timestamp=");
      builder.append(timestamp);
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
      XMLUtils.addSaxString(contentHandler, "LENGTH",
          Integer.valueOf(length).toString());
      XMLUtils.addSaxString(contentHandler, "PATH", path);
      XMLUtils.addSaxString(contentHandler, "TIMESTAMP",
          Long.valueOf(timestamp).toString());
      FSEditLogOp.permissionStatusToXml(contentHandler, permissions);
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.length = Integer.valueOf(st.getValue("LENGTH"));
      this.path = st.getValue("PATH");
      this.timestamp = Long.valueOf(st.getValue("TIMESTAMP"));
      this.permissions =
          permissionStatusFromXml(st.getChildren("PERMISSION_STATUS").get(0));
    }
  }

  static class SetGenstampOp extends FSEditLogOp {
    long genStamp;

    private SetGenstampOp() {
      super(OP_SET_GENSTAMP);
    }

    static SetGenstampOp getInstance(OpInstanceCache cache) {
      return (SetGenstampOp)cache.get(OP_SET_GENSTAMP);
    }

    SetGenstampOp setGenerationStamp(long genStamp) {
      this.genStamp = genStamp;
      return this;
    }
    
    @Override
    public 
    void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeLong(genStamp, out);
    }
    
    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      this.genStamp = FSImageSerialization.readLong(in);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("SetGenstampOp [genStamp=");
      builder.append(genStamp);
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
          Long.valueOf(genStamp).toString());
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.genStamp = Long.valueOf(st.getValue("GENSTAMP"));
    }
  }

  static class SetPermissionsOp extends FSEditLogOp {
    String src;
    FsPermission permissions;

    private SetPermissionsOp() {
      super(OP_SET_PERMISSIONS);
    }

    static SetPermissionsOp getInstance(OpInstanceCache cache) {
      return (SetPermissionsOp)cache.get(OP_SET_PERMISSIONS);
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
          Long.valueOf(nsQuota).toString());
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.src = st.getValue("SRC");
      this.nsQuota = Long.valueOf(st.getValue("NSQUOTA"));
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
          Long.valueOf(nsQuota).toString());
      XMLUtils.addSaxString(contentHandler, "DSQUOTA",
          Long.valueOf(dsQuota).toString());
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.src = st.getValue("SRC");
      this.nsQuota = Long.valueOf(st.getValue("NSQUOTA"));
      this.dsQuota = Long.valueOf(st.getValue("DSQUOTA"));
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

    static TimesOp getInstance(OpInstanceCache cache) {
      return (TimesOp)cache.get(OP_TIMES);
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
      if (!LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.length = in.readInt();
        if (length != 3) {
          throw new IOException("Incorrect data format. " + "times operation.");
        }
      }
      this.path = FSImageSerialization.readString(in);

      if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
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
          Integer.valueOf(length).toString());
      XMLUtils.addSaxString(contentHandler, "PATH", path);
      XMLUtils.addSaxString(contentHandler, "MTIME",
          Long.valueOf(mtime).toString());
      XMLUtils.addSaxString(contentHandler, "ATIME",
          Long.valueOf(atime).toString());
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.length = Integer.valueOf(st.getValue("LENGTH"));
      this.path = st.getValue("PATH");
      this.mtime = Long.valueOf(st.getValue("MTIME"));
      this.atime = Long.valueOf(st.getValue("ATIME"));
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

    static SymlinkOp getInstance(OpInstanceCache cache) {
      return (SymlinkOp)cache.get(OP_SYMLINK);
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
    public 
    void writeFields(DataOutputStream out) throws IOException {
      FSImageSerialization.writeString(path, out);
      FSImageSerialization.writeString(value, out);
      FSImageSerialization.writeLong(mtime, out);
      FSImageSerialization.writeLong(atime, out);
      permissionStatus.write(out);
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      if (!LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.length = in.readInt();
        if (this.length != 4) {
          throw new IOException("Incorrect data format. "
              + "symlink operation.");
        }
      }
      this.path = FSImageSerialization.readString(in);
      this.value = FSImageSerialization.readString(in);

      if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.mtime = FSImageSerialization.readLong(in);
        this.atime = FSImageSerialization.readLong(in);
      } else {
        this.mtime = readLong(in);
        this.atime = readLong(in);
      }
      this.permissionStatus = PermissionStatus.read(in);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("SymlinkOp [length=");
      builder.append(length);
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
          Integer.valueOf(length).toString());
      XMLUtils.addSaxString(contentHandler, "PATH", path);
      XMLUtils.addSaxString(contentHandler, "VALUE", value);
      XMLUtils.addSaxString(contentHandler, "MTIME",
          Long.valueOf(mtime).toString());
      XMLUtils.addSaxString(contentHandler, "ATIME",
          Long.valueOf(atime).toString());
      FSEditLogOp.permissionStatusToXml(contentHandler, permissionStatus);
    }

    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.length = Integer.valueOf(st.getValue("LENGTH"));
      this.path = st.getValue("PATH");
      this.value = st.getValue("VALUE");
      this.mtime = Long.valueOf(st.getValue("MTIME"));
      this.atime = Long.valueOf(st.getValue("ATIME"));
      this.permissionStatus =
          permissionStatusFromXml(st.getChildren("PERMISSION_STATUS").get(0));
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

    static RenameOp getInstance(OpInstanceCache cache) {
      return (RenameOp)cache.get(OP_RENAME);
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
    }

    @Override
    void readFields(DataInputStream in, int logVersion)
        throws IOException {
      if (!LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.length = in.readInt();
        if (this.length != 3) {
          throw new IOException("Incorrect data format. " + "Rename operation.");
        }
      }
      this.src = FSImageSerialization.readString(in);
      this.dst = FSImageSerialization.readString(in);

      if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
        this.timestamp = FSImageSerialization.readLong(in);
      } else {
        this.timestamp = readLong(in);
      }
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
          Integer.valueOf(length).toString());
      XMLUtils.addSaxString(contentHandler, "SRC", src);
      XMLUtils.addSaxString(contentHandler, "DST", dst);
      XMLUtils.addSaxString(contentHandler, "TIMESTAMP",
          Long.valueOf(timestamp).toString());
      StringBuilder bld = new StringBuilder();
      String prefix = "";
      for (Rename r : options) {
        bld.append(prefix).append(r.toString());
        prefix = "|";
      }
      XMLUtils.addSaxString(contentHandler, "OPTIONS", bld.toString());
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.length = Integer.valueOf(st.getValue("LENGTH"));
      this.src = st.getValue("SRC");
      this.dst = st.getValue("DST");
      this.timestamp = Long.valueOf(st.getValue("TIMESTAMP"));
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
    }
  }

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

  static class GetDelegationTokenOp extends FSEditLogOp {
    DelegationTokenIdentifier token;
    long expiryTime;

    private GetDelegationTokenOp() {
      super(OP_GET_DELEGATION_TOKEN);
    }

    static GetDelegationTokenOp getInstance(OpInstanceCache cache) {
      return (GetDelegationTokenOp)cache.get(OP_GET_DELEGATION_TOKEN);
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
      if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
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
          Long.valueOf(expiryTime).toString());
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.token = delegationTokenFromXml(st.getChildren(
          "DELEGATION_TOKEN_IDENTIFIER").get(0));
      this.expiryTime = Long.valueOf(st.getValue("EXPIRY_TIME"));
    }
  }

  static class RenewDelegationTokenOp extends FSEditLogOp {
    DelegationTokenIdentifier token;
    long expiryTime;

    private RenewDelegationTokenOp() {
      super(OP_RENEW_DELEGATION_TOKEN);
    }

    static RenewDelegationTokenOp getInstance(OpInstanceCache cache) {
      return (RenewDelegationTokenOp)cache.get(OP_RENEW_DELEGATION_TOKEN);
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
      if (LayoutVersion.supports(Feature.EDITLOG_OP_OPTIMIZATION, logVersion)) {
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
          Long.valueOf(expiryTime).toString());
    }
    
    @Override void fromXml(Stanza st) throws InvalidXmlException {
      this.token = delegationTokenFromXml(st.getChildren(
          "DELEGATION_TOKEN_IDENTIFIER").get(0));
      this.expiryTime = Long.valueOf(st.getValue("EXPIRY_TIME"));
    }
  }

  static class CancelDelegationTokenOp extends FSEditLogOp {
    DelegationTokenIdentifier token;

    private CancelDelegationTokenOp() {
      super(OP_CANCEL_DELEGATION_TOKEN);
    }

    static CancelDelegationTokenOp getInstance(OpInstanceCache cache) {
      return (CancelDelegationTokenOp)cache.get(OP_CANCEL_DELEGATION_TOKEN);
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
    private final StreamLimiter limiter;
    private final int logVersion;
    private final Checksum checksum;
    private final OpInstanceCache cache;
    private int maxOpSize;

    /**
     * Construct the reader
     * @param in The stream to read from.
     * @param logVersion The version of the data coming from the stream.
     */
    @SuppressWarnings("deprecation")
    public Reader(DataInputStream in, StreamLimiter limiter,
        int logVersion) {
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

      if (LayoutVersion.supports(Feature.STORED_TXIDS, logVersion)) {
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
    this.txid = Long.valueOf(st.getValue("TXID"));
    fromXml(st);
  }
  
  public static void blockToXml(ContentHandler contentHandler, Block block) 
      throws SAXException {
    contentHandler.startElement("", "", "BLOCK", new AttributesImpl());
    XMLUtils.addSaxString(contentHandler, "BLOCK_ID",
        Long.valueOf(block.getBlockId()).toString());
    XMLUtils.addSaxString(contentHandler, "NUM_BYTES",
        Long.valueOf(block.getNumBytes()).toString());
    XMLUtils.addSaxString(contentHandler, "GENSTAMP",
        Long.valueOf(block.getGenerationStamp()).toString());
    contentHandler.endElement("", "", "BLOCK");
  }

  public static Block blockFromXml(Stanza st)
      throws InvalidXmlException {
    long blockId = Long.valueOf(st.getValue("BLOCK_ID"));
    long numBytes = Long.valueOf(st.getValue("NUM_BYTES"));
    long generationStamp = Long.valueOf(st.getValue("GENSTAMP"));
    return new Block(blockId, numBytes, generationStamp);
  }

  public static void delegationTokenToXml(ContentHandler contentHandler,
      DelegationTokenIdentifier token) throws SAXException {
    contentHandler.startElement("", "", "DELEGATION_TOKEN_IDENTIFIER", new AttributesImpl());
    XMLUtils.addSaxString(contentHandler, "KIND", token.getKind().toString());
    XMLUtils.addSaxString(contentHandler, "SEQUENCE_NUMBER",
        Integer.valueOf(token.getSequenceNumber()).toString());
    XMLUtils.addSaxString(contentHandler, "OWNER",
        token.getOwner().toString());
    XMLUtils.addSaxString(contentHandler, "RENEWER",
        token.getRenewer().toString());
    XMLUtils.addSaxString(contentHandler, "REALUSER",
        token.getRealUser().toString());
    XMLUtils.addSaxString(contentHandler, "ISSUE_DATE",
        Long.valueOf(token.getIssueDate()).toString());
    XMLUtils.addSaxString(contentHandler, "MAX_DATE",
        Long.valueOf(token.getMaxDate()).toString());
    XMLUtils.addSaxString(contentHandler, "MASTER_KEY_ID",
        Integer.valueOf(token.getMasterKeyId()).toString());
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
    int seqNum = Integer.valueOf(st.getValue("SEQUENCE_NUMBER"));
    String owner = st.getValue("OWNER");
    String renewer = st.getValue("RENEWER");
    String realuser = st.getValue("REALUSER");
    long issueDate = Long.valueOf(st.getValue("ISSUE_DATE"));
    long maxDate = Long.valueOf(st.getValue("MAX_DATE"));
    int masterKeyId = Integer.valueOf(st.getValue("MASTER_KEY_ID"));
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
        Integer.valueOf(key.getKeyId()).toString());
    XMLUtils.addSaxString(contentHandler, "EXPIRY_DATE",
        Long.valueOf(key.getExpiryDate()).toString());
    if (key.getEncodedKey() != null) {
      XMLUtils.addSaxString(contentHandler, "KEY",
          Hex.encodeHexString(key.getEncodedKey()));
    }
    contentHandler.endElement("", "", "DELEGATION_KEY");
  }
  
  public static DelegationKey delegationKeyFromXml(Stanza st)
      throws InvalidXmlException {
    int keyId = Integer.valueOf(st.getValue("KEY_ID"));
    long expiryDate = Long.valueOf(st.getValue("EXPIRY_DATE"));
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
    XMLUtils.addSaxString(contentHandler, "MODE",
        Short.valueOf(perm.getPermission().toShort()).toString());
    contentHandler.endElement("", "", "PERMISSION_STATUS");
  }

  public static PermissionStatus permissionStatusFromXml(Stanza st)
      throws InvalidXmlException {
    String username = st.getValue("USERNAME");
    String groupname = st.getValue("GROUPNAME");
    short mode = Short.valueOf(st.getValue("MODE"));
    return new PermissionStatus(username, groupname, new FsPermission(mode));
  }
}
