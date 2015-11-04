// automatically generated, do not modify
package org.apache.hadoop.hdfs.server.flatbuffer;
import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class IntelINodeFile extends Table {
  public static IntelINodeFile getRootAsIntelINodeFile(ByteBuffer _bb) { return getRootAsIntelINodeFile(_bb, new IntelINodeFile()); }
  public static IntelINodeFile getRootAsIntelINodeFile(ByteBuffer _bb, IntelINodeFile obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public IntelINodeFile __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public long replication() { int o = __offset(4); return o != 0 ? (long)bb.getInt(o + bb_pos) & 0xFFFFFFFFL : 0; }
  public long modificationTime() { int o = __offset(6); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public long accessTime() { int o = __offset(8); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public long preferredBlockSize() { int o = __offset(10); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public long permission() { int o = __offset(12); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public IntelBlockProto blocks(int j) { return blocks(new IntelBlockProto(), j); }
  public IntelBlockProto blocks(IntelBlockProto obj, int j) { int o = __offset(14); return o != 0 ? obj.__init(__indirect(__vector(o) + j * 4), bb) : null; }
  public int blocksLength() { int o = __offset(14); return o != 0 ? __vector_len(o) : 0; }
  public IntelFileUnderConstructionFeature fileUC() { return fileUC(new IntelFileUnderConstructionFeature()); }
  public IntelFileUnderConstructionFeature fileUC(IntelFileUnderConstructionFeature obj) { int o = __offset(16); return o != 0 ? obj.__init(__indirect(o + bb_pos), bb) : null; }
  public IntelAclFeatureProto acl() { return acl(new IntelAclFeatureProto()); }
  public IntelAclFeatureProto acl(IntelAclFeatureProto obj) { int o = __offset(18); return o != 0 ? obj.__init(__indirect(o + bb_pos), bb) : null; }
  public IntelXAttrFeatureProto xAttrs() { return xAttrs(new IntelXAttrFeatureProto()); }
  public IntelXAttrFeatureProto xAttrs(IntelXAttrFeatureProto obj) { int o = __offset(20); return o != 0 ? obj.__init(__indirect(o + bb_pos), bb) : null; }
  public long storagePolicyID() { int o = __offset(22); return o != 0 ? (long)bb.getInt(o + bb_pos) & 0xFFFFFFFFL : 0; }

  public static int createIntelINodeFile(FlatBufferBuilder builder,
      long replication,
      long modificationTime,
      long accessTime,
      long preferredBlockSize,
      long permission,
      int blocks,
      int fileUC,
      int acl,
      int xAttrs,
      long storagePolicyID) {
    builder.startObject(10);
    IntelINodeFile.addPermission(builder, permission);
    IntelINodeFile.addPreferredBlockSize(builder, preferredBlockSize);
    IntelINodeFile.addAccessTime(builder, accessTime);
    IntelINodeFile.addModificationTime(builder, modificationTime);
    IntelINodeFile.addStoragePolicyID(builder, storagePolicyID);
    IntelINodeFile.addXAttrs(builder, xAttrs);
    IntelINodeFile.addAcl(builder, acl);
    IntelINodeFile.addFileUC(builder, fileUC);
    IntelINodeFile.addBlocks(builder, blocks);
    IntelINodeFile.addReplication(builder, replication);
    return IntelINodeFile.endIntelINodeFile(builder);
  }

  public static void startIntelINodeFile(FlatBufferBuilder builder) { builder.startObject(10); }
  public static void addReplication(FlatBufferBuilder builder, long replication) { builder.addInt(0, (int)(replication & 0xFFFFFFFFL), 0); }
  public static void addModificationTime(FlatBufferBuilder builder, long modificationTime) { builder.addLong(1, modificationTime, 0); }
  public static void addAccessTime(FlatBufferBuilder builder, long accessTime) { builder.addLong(2, accessTime, 0); }
  public static void addPreferredBlockSize(FlatBufferBuilder builder, long preferredBlockSize) { builder.addLong(3, preferredBlockSize, 0); }
  public static void addPermission(FlatBufferBuilder builder, long permission) { builder.addLong(4, permission, 0); }
  public static void addBlocks(FlatBufferBuilder builder, int blocksOffset) { builder.addOffset(5, blocksOffset, 0); }
  public static int createBlocksVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startBlocksVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static void addFileUC(FlatBufferBuilder builder, int fileUCOffset) { builder.addOffset(6, fileUCOffset, 0); }
  public static void addAcl(FlatBufferBuilder builder, int aclOffset) { builder.addOffset(7, aclOffset, 0); }
  public static void addXAttrs(FlatBufferBuilder builder, int xAttrsOffset) { builder.addOffset(8, xAttrsOffset, 0); }
  public static void addStoragePolicyID(FlatBufferBuilder builder, long storagePolicyID) { builder.addInt(9, (int)(storagePolicyID & 0xFFFFFFFFL), 0); }
  public static int endIntelINodeFile(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishIntelINodeFileBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }

};

