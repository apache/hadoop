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
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DeprecatedUTF8;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat.ReferenceMap;
import org.apache.hadoop.hdfs.util.XMLUtils;
import org.apache.hadoop.hdfs.util.XMLUtils.InvalidXmlException;
import org.apache.hadoop.hdfs.util.XMLUtils.Stanza;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import com.google.common.base.Preconditions;

/**
 * Static utility functions for serializing various pieces of data in the correct
 * format for the FSImage file.
 *
 * Some members are currently public for the benefit of the Offline Image Viewer
 * which is located outside of this package. These members should be made
 * package-protected when the OIV is refactored.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSImageSerialization {

  // Static-only class
  private FSImageSerialization() {}
  
  /**
   * In order to reduce allocation, we reuse some static objects. However, the methods
   * in this class should be thread-safe since image-saving is multithreaded, so 
   * we need to keep the static objects in a thread-local.
   */
  static private final ThreadLocal<TLData> TL_DATA =
    new ThreadLocal<TLData>() {
    @Override
    protected TLData initialValue() {
      return new TLData();
    }
  };

  /**
   * Simple container "struct" for threadlocal data.
   */
  static private final class TLData {
    final DeprecatedUTF8 U_STR = new DeprecatedUTF8();
    final ShortWritable U_SHORT = new ShortWritable();
    final IntWritable U_INT = new IntWritable();
    final LongWritable U_LONG = new LongWritable();
    final FsPermission FILE_PERM = new FsPermission((short) 0);
    final BooleanWritable U_BOOLEAN = new BooleanWritable();
  }

  private static void writePermissionStatus(INodeAttributes inode,
      DataOutput out) throws IOException {
    final FsPermission p = TL_DATA.get().FILE_PERM;
    p.fromShort(inode.getFsPermissionShort());
    PermissionStatus.write(out, inode.getUserName(), inode.getGroupName(), p);
  }

  private static void writeBlocks(final Block[] blocks,
      final DataOutput out) throws IOException {
    if (blocks == null) {
      out.writeInt(0);
    } else {
      out.writeInt(blocks.length);
      for (Block blk : blocks) {
        blk.write(out);
      }
    }
  }

  // Helper function that reads in an INodeUnderConstruction
  // from the input stream
  //
  static INodeFile readINodeUnderConstruction(
      DataInput in, FSNamesystem fsNamesys, int imgVersion)
      throws IOException {
    byte[] name = readBytes(in);
    long inodeId = NameNodeLayoutVersion.supports(
        LayoutVersion.Feature.ADD_INODE_ID, imgVersion) ? in.readLong()
        : fsNamesys.dir.allocateNewInodeId();
    short blockReplication = in.readShort();
    long modificationTime = in.readLong();
    long preferredBlockSize = in.readLong();

    int numBlocks = in.readInt();

    final BlockInfoContiguous[] blocksContiguous =
        new BlockInfoContiguous[numBlocks];
    Block blk = new Block();
    int i = 0;
    for (; i < numBlocks - 1; i++) {
      blk.readFields(in);
      blocksContiguous[i] = new BlockInfoContiguous(blk, blockReplication);
    }
    // last block is UNDER_CONSTRUCTION
    if(numBlocks > 0) {
      blk.readFields(in);
      blocksContiguous[i] = new BlockInfoContiguous(blk, blockReplication);
      blocksContiguous[i].convertToBlockUnderConstruction(
          BlockUCState.UNDER_CONSTRUCTION, null);
    }

    PermissionStatus perm = PermissionStatus.read(in);
    String clientName = readString(in);
    String clientMachine = readString(in);

    // We previously stored locations for the last block, now we
    // just record that there are none
    int numLocs = in.readInt();
    assert numLocs == 0 : "Unexpected block locations";

    // Images in the pre-protobuf format will not have the lazyPersist flag,
    // so it is safe to pass false always.
    INodeFile file = new INodeFile(inodeId, name, perm, modificationTime,
        modificationTime, blocksContiguous, blockReplication, preferredBlockSize);
    file.toUnderConstruction(clientName, clientMachine);
    return file;
  }

  // Helper function that writes an INodeUnderConstruction
  // into the output stream
  //
  static void writeINodeUnderConstruction(DataOutputStream out, INodeFile cons,
      String path) throws IOException {
    writeString(path, out);
    out.writeLong(cons.getId());
    out.writeShort(cons.getFileReplication());
    out.writeLong(cons.getModificationTime());
    out.writeLong(cons.getPreferredBlockSize());

    writeBlocks(cons.getBlocks(), out);
    cons.getPermissionStatus().write(out);

    FileUnderConstructionFeature uc = cons.getFileUnderConstructionFeature();
    writeString(uc.getClientName(), out);
    writeString(uc.getClientMachine(), out);

    out.writeInt(0); //  do not store locations of last block
  }

  /**
   * Serialize a {@link INodeFile} node
   * @param file The INodeFile to write
   * @param out The {@link DataOutputStream} where the fields are written
   * @param writeUnderConstruction Whether to write under construction information
   */
  public static void writeINodeFile(INodeFile file, DataOutput out,
      boolean writeUnderConstruction) throws IOException {
    writeLocalName(file, out);
    out.writeLong(file.getId());
    out.writeShort(file.getFileReplication());
    out.writeLong(file.getModificationTime());
    out.writeLong(file.getAccessTime());
    out.writeLong(file.getPreferredBlockSize());

    writeBlocks(file.getBlocks(), out);
    SnapshotFSImageFormat.saveFileDiffList(file, out);

    if (writeUnderConstruction) {
      if (file.isUnderConstruction()) {
        out.writeBoolean(true);
        final FileUnderConstructionFeature uc = file.getFileUnderConstructionFeature();
        writeString(uc.getClientName(), out);
        writeString(uc.getClientMachine(), out);
      } else {
        out.writeBoolean(false);
      }
    }

    writePermissionStatus(file, out);
  }

  /** Serialize an {@link INodeFileAttributes}. */
  public static void writeINodeFileAttributes(INodeFileAttributes file,
      DataOutput out) throws IOException {
    writeLocalName(file, out);
    writePermissionStatus(file, out);
    out.writeLong(file.getModificationTime());
    out.writeLong(file.getAccessTime());

    out.writeShort(file.getFileReplication());
    out.writeLong(file.getPreferredBlockSize());
  }

  private static void writeQuota(QuotaCounts quota, DataOutput out)
      throws IOException {
    out.writeLong(quota.getNameSpace());
    out.writeLong(quota.getStorageSpace());
  }

  /**
   * Serialize a {@link INodeDirectory}
   * @param node The node to write
   * @param out The {@link DataOutput} where the fields are written
   */
  public static void writeINodeDirectory(INodeDirectory node, DataOutput out)
      throws IOException {
    writeLocalName(node, out);
    out.writeLong(node.getId());
    out.writeShort(0);  // replication
    out.writeLong(node.getModificationTime());
    out.writeLong(0);   // access time
    out.writeLong(0);   // preferred block size
    out.writeInt(-1);   // # of blocks

    writeQuota(node.getQuotaCounts(), out);

    if (node.isSnapshottable()) {
      out.writeBoolean(true);
    } else {
      out.writeBoolean(false);
      out.writeBoolean(node.isWithSnapshot());
    }

    writePermissionStatus(node, out);
  }

  /**
   * Serialize a {@link INodeDirectory}
   * @param a The node to write
   * @param out The {@link DataOutput} where the fields are written
   */
  public static void writeINodeDirectoryAttributes(
      INodeDirectoryAttributes a, DataOutput out) throws IOException {
    writeLocalName(a, out);
    writePermissionStatus(a, out);
    out.writeLong(a.getModificationTime());
    writeQuota(a.getQuotaCounts(), out);
  }

  /**
   * Serialize a {@link INodeSymlink} node
   * @param node The node to write
   * @param out The {@link DataOutput} where the fields are written
   */
  private static void writeINodeSymlink(INodeSymlink node, DataOutput out)
      throws IOException {
    writeLocalName(node, out);
    out.writeLong(node.getId());
    out.writeShort(0);  // replication
    out.writeLong(0);   // modification time
    out.writeLong(0);   // access time
    out.writeLong(0);   // preferred block size
    out.writeInt(-2);   // # of blocks

    Text.writeString(out, node.getSymlinkString());
    writePermissionStatus(node, out);
  }

  /** Serialize a {@link INodeReference} node */
  private static void writeINodeReference(INodeReference ref, DataOutput out,
      boolean writeUnderConstruction, ReferenceMap referenceMap
      ) throws IOException {
    writeLocalName(ref, out);
    out.writeLong(ref.getId());
    out.writeShort(0);  // replication
    out.writeLong(0);   // modification time
    out.writeLong(0);   // access time
    out.writeLong(0);   // preferred block size
    out.writeInt(-3);   // # of blocks

    final boolean isWithName = ref instanceof INodeReference.WithName;
    out.writeBoolean(isWithName);

    if (!isWithName) {
      Preconditions.checkState(ref instanceof INodeReference.DstReference);
      // dst snapshot id
      out.writeInt(ref.getDstSnapshotId());
    } else {
      out.writeInt(((INodeReference.WithName) ref).getLastSnapshotId());
    }

    final INodeReference.WithCount withCount
        = (INodeReference.WithCount)ref.getReferredINode();
    referenceMap.writeINodeReferenceWithCount(withCount, out,
        writeUnderConstruction);
  }

  /**
   * Save one inode's attributes to the image.
   */
  public static void saveINode2Image(INode node, DataOutput out,
      boolean writeUnderConstruction, ReferenceMap referenceMap)
      throws IOException {
    if (node.isReference()) {
      writeINodeReference(node.asReference(), out, writeUnderConstruction,
          referenceMap);
    } else if (node.isDirectory()) {
      writeINodeDirectory(node.asDirectory(), out);
    } else if (node.isSymlink()) {
      writeINodeSymlink(node.asSymlink(), out);
    } else if (node.isFile()) {
      writeINodeFile(node.asFile(), out, writeUnderConstruction);
    }
  }

  // This should be reverted to package private once the ImageLoader
  // code is moved into this package. This method should not be called
  // by other code.
  @SuppressWarnings("deprecation")
  public static String readString(DataInput in) throws IOException {
    DeprecatedUTF8 ustr = TL_DATA.get().U_STR;
    ustr.readFields(in);
    return ustr.toStringChecked();
  }

  static String readString_EmptyAsNull(DataInput in) throws IOException {
    final String s = readString(in);
    return s.isEmpty()? null: s;
  }

  @SuppressWarnings("deprecation")
  public static void writeString(String str, DataOutput out) throws IOException {
    DeprecatedUTF8 ustr = TL_DATA.get().U_STR;
    ustr.set(str);
    ustr.write(out);
  }

  
  /** read the long value */
  static long readLong(DataInput in) throws IOException {
    LongWritable uLong = TL_DATA.get().U_LONG;
    uLong.readFields(in);
    return uLong.get();
  }

  /** write the long value */
  static void writeLong(long value, DataOutputStream out) throws IOException {
    LongWritable uLong = TL_DATA.get().U_LONG;
    uLong.set(value);
    uLong.write(out);
  }
  
  /** read the boolean value */
  static boolean readBoolean(DataInput in) throws IOException {
    BooleanWritable uBoolean = TL_DATA.get().U_BOOLEAN;
    uBoolean.readFields(in);
    return uBoolean.get();
  }
  
  /** write the boolean value */
  static void writeBoolean(boolean value, DataOutputStream out) 
      throws IOException {
    BooleanWritable uBoolean = TL_DATA.get().U_BOOLEAN;
    uBoolean.set(value);
    uBoolean.write(out);
  }
  
  /** write the byte value */
  static void writeByte(byte value, DataOutputStream out)
      throws IOException {
    out.write(value);
  }

  /** read the int value */
  static int readInt(DataInput in) throws IOException {
    IntWritable uInt = TL_DATA.get().U_INT;
    uInt.readFields(in);
    return uInt.get();
  }
  
  /** write the int value */
  static void writeInt(int value, DataOutputStream out) throws IOException {
    IntWritable uInt = TL_DATA.get().U_INT;
    uInt.set(value);
    uInt.write(out);
  }

  /** read short value */
  static short readShort(DataInput in) throws IOException {
    ShortWritable uShort = TL_DATA.get().U_SHORT;
    uShort.readFields(in);
    return uShort.get();
  }

  /** write short value */
  static void writeShort(short value, DataOutputStream out) throws IOException {
    ShortWritable uShort = TL_DATA.get().U_SHORT;
    uShort.set(value);
    uShort.write(out);
  }
  
  // Same comments apply for this method as for readString()
  @SuppressWarnings("deprecation")
  public static byte[] readBytes(DataInput in) throws IOException {
    DeprecatedUTF8 ustr = TL_DATA.get().U_STR;
    ustr.readFields(in);
    int len = ustr.getLength();
    byte[] bytes = new byte[len];
    System.arraycopy(ustr.getBytes(), 0, bytes, 0, len);
    return bytes;
  }

  public static byte readByte(DataInput in) throws IOException {
    return in.readByte();
  }

  /**
   * Reading the path from the image and converting it to byte[][] directly
   * this saves us an array copy and conversions to and from String
   * @param in input to read from
   * @return the array each element of which is a byte[] representation 
   *            of a path component
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  public static byte[][] readPathComponents(DataInput in)
      throws IOException {
    DeprecatedUTF8 ustr = TL_DATA.get().U_STR;
    
    ustr.readFields(in);
    return DFSUtil.bytes2byteArray(ustr.getBytes(),
      ustr.getLength(), (byte) Path.SEPARATOR_CHAR);
  }
  
  public static byte[] readLocalName(DataInput in) throws IOException {
    byte[] createdNodeName = new byte[in.readShort()];
    in.readFully(createdNodeName);
    return createdNodeName;
  }

  private static void writeLocalName(INodeAttributes inode, DataOutput out)
      throws IOException {
    final byte[] name = inode.getLocalNameBytes();
    writeBytes(name, out);
  }
  
  public static void writeBytes(byte[] data, DataOutput out)
      throws IOException {
    out.writeShort(data.length);
    out.write(data);
  }

  /**
   * Write an array of blocks as compactly as possible. This uses
   * delta-encoding for the generation stamp and size, following
   * the principle that genstamp increases relatively slowly,
   * and size is equal for all but the last block of a file.
   */
  public static void writeCompactBlockArray(
      Block[] blocks, DataOutputStream out) throws IOException {
    WritableUtils.writeVInt(out, blocks.length);
    Block prev = null;
    for (Block b : blocks) {
      long szDelta = b.getNumBytes() -
          (prev != null ? prev.getNumBytes() : 0);
      long gsDelta = b.getGenerationStamp() -
          (prev != null ? prev.getGenerationStamp() : 0);
      out.writeLong(b.getBlockId()); // blockid is random
      WritableUtils.writeVLong(out, szDelta);
      WritableUtils.writeVLong(out, gsDelta);
      prev = b;
    }
  }
  
  public static Block[] readCompactBlockArray(
      DataInput in, int logVersion) throws IOException {
    int num = WritableUtils.readVInt(in);
    if (num < 0) {
      throw new IOException("Invalid block array length: " + num);
    }
    Block prev = null;
    Block[] ret = new Block[num];
    for (int i = 0; i < num; i++) {
      long id = in.readLong();
      long sz = WritableUtils.readVLong(in) +
          ((prev != null) ? prev.getNumBytes() : 0);
      long gs = WritableUtils.readVLong(in) +
          ((prev != null) ? prev.getGenerationStamp() : 0);
      ret[i] = new Block(id, sz, gs);
      prev = ret[i];
    }
    return ret;
  }

  public static void writeCacheDirectiveInfo(DataOutputStream out,
      CacheDirectiveInfo directive) throws IOException {
    writeLong(directive.getId(), out);
    int flags =
        ((directive.getPath() != null) ? 0x1 : 0) |
        ((directive.getReplication() != null) ? 0x2 : 0) |
        ((directive.getPool() != null) ? 0x4 : 0) |
        ((directive.getExpiration() != null) ? 0x8 : 0);
    out.writeInt(flags);
    if (directive.getPath() != null) {
      writeString(directive.getPath().toUri().getPath(), out);
    }
    if (directive.getReplication() != null) {
      writeShort(directive.getReplication(), out);
    }
    if (directive.getPool() != null) {
      writeString(directive.getPool(), out);
    }
    if (directive.getExpiration() != null) {
      writeLong(directive.getExpiration().getMillis(), out);
    }
  }

  public static CacheDirectiveInfo readCacheDirectiveInfo(DataInput in)
      throws IOException {
    CacheDirectiveInfo.Builder builder =
        new CacheDirectiveInfo.Builder();
    builder.setId(readLong(in));
    int flags = in.readInt();
    if ((flags & 0x1) != 0) {
      builder.setPath(new Path(readString(in)));
    }
    if ((flags & 0x2) != 0) {
      builder.setReplication(readShort(in));
    }
    if ((flags & 0x4) != 0) {
      builder.setPool(readString(in));
    }
    if ((flags & 0x8) != 0) {
      builder.setExpiration(
          CacheDirectiveInfo.Expiration.newAbsolute(readLong(in)));
    }
    if ((flags & ~0xF) != 0) {
      throw new IOException("unknown flags set in " +
          "ModifyCacheDirectiveInfoOp: " + flags);
    }
    return builder.build();
  }

  public static CacheDirectiveInfo readCacheDirectiveInfo(Stanza st)
      throws InvalidXmlException {
    CacheDirectiveInfo.Builder builder =
        new CacheDirectiveInfo.Builder();
    builder.setId(Long.parseLong(st.getValue("ID")));
    String path = st.getValueOrNull("PATH");
    if (path != null) {
      builder.setPath(new Path(path));
    }
    String replicationString = st.getValueOrNull("REPLICATION");
    if (replicationString != null) {
      builder.setReplication(Short.parseShort(replicationString));
    }
    String pool = st.getValueOrNull("POOL");
    if (pool != null) {
      builder.setPool(pool);
    }
    String expiryTime = st.getValueOrNull("EXPIRATION");
    if (expiryTime != null) {
      builder.setExpiration(CacheDirectiveInfo.Expiration.newAbsolute(
          Long.parseLong(expiryTime)));
    }
    return builder.build();
  }

  public static void writeCacheDirectiveInfo(ContentHandler contentHandler,
      CacheDirectiveInfo directive) throws SAXException {
    XMLUtils.addSaxString(contentHandler, "ID",
        Long.toString(directive.getId()));
    if (directive.getPath() != null) {
      XMLUtils.addSaxString(contentHandler, "PATH",
          directive.getPath().toUri().getPath());
    }
    if (directive.getReplication() != null) {
      XMLUtils.addSaxString(contentHandler, "REPLICATION",
          Short.toString(directive.getReplication()));
    }
    if (directive.getPool() != null) {
      XMLUtils.addSaxString(contentHandler, "POOL", directive.getPool());
    }
    if (directive.getExpiration() != null) {
      XMLUtils.addSaxString(contentHandler, "EXPIRATION",
          "" + directive.getExpiration().getMillis());
    }
  }

  public static void writeCachePoolInfo(DataOutputStream out, CachePoolInfo info)
      throws IOException {
    writeString(info.getPoolName(), out);

    final String ownerName = info.getOwnerName();
    final String groupName = info.getGroupName();
    final Long limit = info.getLimit();
    final FsPermission mode = info.getMode();
    final Long maxRelativeExpiry = info.getMaxRelativeExpiryMs();
    final Short defaultReplication = info.getDefaultReplication();

    boolean hasOwner, hasGroup, hasMode, hasLimit,
            hasMaxRelativeExpiry, hasDefaultReplication;
    hasOwner = ownerName != null;
    hasGroup = groupName != null;
    hasMode = mode != null;
    hasLimit = limit != null;
    hasMaxRelativeExpiry = maxRelativeExpiry != null;
    hasDefaultReplication = defaultReplication != null;

    int flags =
        (hasOwner ? 0x1 : 0) |
        (hasGroup ? 0x2 : 0) |
        (hasMode  ? 0x4 : 0) |
        (hasLimit ? 0x8 : 0) |
        (hasMaxRelativeExpiry ? 0x10 : 0) |
        (hasDefaultReplication ? 0x20 : 0);

    writeInt(flags, out);

    if (hasOwner) {
      writeString(ownerName, out);
    }
    if (hasGroup) {
      writeString(groupName, out);
    }
    if (hasMode) {
      mode.write(out);
    }
    if (hasLimit) {
      writeLong(limit, out);
    }
    if (hasMaxRelativeExpiry) {
      writeLong(maxRelativeExpiry, out);
    }
    if (hasDefaultReplication) {
      writeShort(defaultReplication, out);
    }
  }

  public static CachePoolInfo readCachePoolInfo(DataInput in)
      throws IOException {
    String poolName = readString(in);
    CachePoolInfo info = new CachePoolInfo(poolName);
    int flags = readInt(in);
    if ((flags & 0x1) != 0) {
      info.setOwnerName(readString(in));
    }
    if ((flags & 0x2) != 0)  {
      info.setGroupName(readString(in));
    }
    if ((flags & 0x4) != 0) {
      info.setMode(FsPermission.read(in));
    }
    if ((flags & 0x8) != 0) {
      info.setLimit(readLong(in));
    }
    if ((flags & 0x10) != 0) {
      info.setMaxRelativeExpiryMs(readLong(in));
    }
    if ((flags & 0x20) != 0) {
      info.setDefaultReplication(readShort(in));
    }
    if ((flags & ~0x3F) != 0) {
      throw new IOException("Unknown flag in CachePoolInfo: " + flags);
    }
    return info;
  }

  public static void writeCachePoolInfo(ContentHandler contentHandler,
      CachePoolInfo info) throws SAXException {
    XMLUtils.addSaxString(contentHandler, "POOLNAME", info.getPoolName());

    final String ownerName = info.getOwnerName();
    final String groupName = info.getGroupName();
    final Long limit = info.getLimit();
    final FsPermission mode = info.getMode();
    final Long maxRelativeExpiry = info.getMaxRelativeExpiryMs();
    final Short defaultReplication = info.getDefaultReplication();

    if (ownerName != null) {
      XMLUtils.addSaxString(contentHandler, "OWNERNAME", ownerName);
    }
    if (groupName != null) {
      XMLUtils.addSaxString(contentHandler, "GROUPNAME", groupName);
    }
    if (mode != null) {
      FSEditLogOp.fsPermissionToXml(contentHandler, mode);
    }
    if (limit != null) {
      XMLUtils.addSaxString(contentHandler, "LIMIT",
          Long.toString(limit));
    }
    if (maxRelativeExpiry != null) {
      XMLUtils.addSaxString(contentHandler, "MAXRELATIVEEXPIRY",
          Long.toString(maxRelativeExpiry));
    }
    if (defaultReplication != null) {
      XMLUtils.addSaxString(contentHandler, "DEFAULTREPLICATION",
          Short.toString(defaultReplication));
    }
  }

  public static CachePoolInfo readCachePoolInfo(Stanza st)
      throws InvalidXmlException {
    String poolName = st.getValue("POOLNAME");
    CachePoolInfo info = new CachePoolInfo(poolName);
    if (st.hasChildren("OWNERNAME")) {
      info.setOwnerName(st.getValue("OWNERNAME"));
    }
    if (st.hasChildren("GROUPNAME")) {
      info.setGroupName(st.getValue("GROUPNAME"));
    }
    if (st.hasChildren("MODE")) {
      info.setMode(FSEditLogOp.fsPermissionFromXml(st));
    }
    if (st.hasChildren("LIMIT")) {
      info.setLimit(Long.parseLong(st.getValue("LIMIT")));
    }
    if (st.hasChildren("MAXRELATIVEEXPIRY")) {
      info.setMaxRelativeExpiryMs(
          Long.parseLong(st.getValue("MAXRELATIVEEXPIRY")));
    }
    if (st.hasChildren("DEFAULTREPLICATION")) {
      info.setDefaultReplication(Short.parseShort(st
          .getValue("DEFAULTREPLICATION")));
    }
    return info;
  }

  public static void writeErasureCodingPolicy(DataOutputStream out,
      ErasureCodingPolicy ecPolicy) throws IOException {
    writeString(ecPolicy.getSchema().getCodecName(), out);
    writeInt(ecPolicy.getNumDataUnits(), out);
    writeInt(ecPolicy.getNumParityUnits(), out);
    writeInt(ecPolicy.getCellSize(), out);

    Map<String, String> extraOptions = ecPolicy.getSchema().getExtraOptions();
    if (extraOptions == null || extraOptions.isEmpty()) {
      writeInt(0, out);
      return;
    }

    writeInt(extraOptions.size(), out);
    for (Map.Entry<String, String> entry : extraOptions.entrySet()) {
      writeString(entry.getKey(), out);
      writeString(entry.getValue(), out);
    }
  }

  public static ErasureCodingPolicy readErasureCodingPolicy(DataInput in)
      throws IOException {
    String codecName = readString(in);
    int numDataUnits = readInt(in);
    int numParityUnits = readInt(in);
    int cellSize = readInt(in);

    int size = readInt(in);
    Map<String, String> extraOptions = new HashMap<>(size);

    if (size != 0) {
      for (int i = 0; i < size; i++) {
        String key = readString(in);
        String value = readString(in);
        extraOptions.put(key, value);
      }
    }
    ECSchema ecSchema = new ECSchema(codecName, numDataUnits,
        numParityUnits, extraOptions);
    return new ErasureCodingPolicy(ecSchema, cellSize);
  }
}
