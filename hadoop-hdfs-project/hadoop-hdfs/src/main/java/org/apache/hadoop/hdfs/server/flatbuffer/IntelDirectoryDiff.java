// automatically generated, do not modify
package org.apache.hadoop.hdfs.server.flatbuffer;
import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class IntelDirectoryDiff extends Table {
  public static IntelDirectoryDiff getRootAsIntelDirectoryDiff(ByteBuffer _bb) { return getRootAsIntelDirectoryDiff(_bb, new IntelDirectoryDiff()); }
  public static IntelDirectoryDiff getRootAsIntelDirectoryDiff(ByteBuffer _bb, IntelDirectoryDiff obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public IntelDirectoryDiff __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public long snapshotId() { int o = __offset(4); return o != 0 ? (long)bb.getInt(o + bb_pos) & 0xFFFFFFFFL : 0; }
  public long childrenSize() { int o = __offset(6); return o != 0 ? (long)bb.getInt(o + bb_pos) & 0xFFFFFFFFL : 0; }
  public boolean isSnapshotRoot() { int o = __offset(8); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public String name() { int o = __offset(10); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer nameAsByteBuffer() { return __vector_as_bytebuffer(10, 1); }
  public IntelINodeDirectory snapshotCopy() { return snapshotCopy(new IntelINodeDirectory()); }
  public IntelINodeDirectory snapshotCopy(IntelINodeDirectory obj) { int o = __offset(12); return o != 0 ? obj.__init(__indirect(o + bb_pos), bb) : null; }
  public long createdListSize() { int o = __offset(14); return o != 0 ? (long)bb.getInt(o + bb_pos) & 0xFFFFFFFFL : 0; }
  public long deletedINode(int j) { int o = __offset(16); return o != 0 ? bb.getLong(__vector(o) + j * 8) : 0; }
  public int deletedINodeLength() { int o = __offset(16); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer deletedINodeAsByteBuffer() { return __vector_as_bytebuffer(16, 8); }
  public long deletedINodeRef(int j) { int o = __offset(18); return o != 0 ? (long)bb.getInt(__vector(o) + j * 4) & 0xFFFFFFFFL : 0; }
  public int deletedINodeRefLength() { int o = __offset(18); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer deletedINodeRefAsByteBuffer() { return __vector_as_bytebuffer(18, 4); }

  public static int createIntelDirectoryDiff(FlatBufferBuilder builder,
      long snapshotId,
      long childrenSize,
      boolean isSnapshotRoot,
      int name,
      int snapshotCopy,
      long createdListSize,
      int deletedINode,
      int deletedINodeRef) {
    builder.startObject(8);
    IntelDirectoryDiff.addDeletedINodeRef(builder, deletedINodeRef);
    IntelDirectoryDiff.addDeletedINode(builder, deletedINode);
    IntelDirectoryDiff.addCreatedListSize(builder, createdListSize);
    IntelDirectoryDiff.addSnapshotCopy(builder, snapshotCopy);
    IntelDirectoryDiff.addName(builder, name);
    IntelDirectoryDiff.addChildrenSize(builder, childrenSize);
    IntelDirectoryDiff.addSnapshotId(builder, snapshotId);
    IntelDirectoryDiff.addIsSnapshotRoot(builder, isSnapshotRoot);
    return IntelDirectoryDiff.endIntelDirectoryDiff(builder);
  }

  public static void startIntelDirectoryDiff(FlatBufferBuilder builder) { builder.startObject(8); }
  public static void addSnapshotId(FlatBufferBuilder builder, long snapshotId) { builder.addInt(0, (int)(snapshotId & 0xFFFFFFFFL), 0); }
  public static void addChildrenSize(FlatBufferBuilder builder, long childrenSize) { builder.addInt(1, (int)(childrenSize & 0xFFFFFFFFL), 0); }
  public static void addIsSnapshotRoot(FlatBufferBuilder builder, boolean isSnapshotRoot) { builder.addBoolean(2, isSnapshotRoot, false); }
  public static void addName(FlatBufferBuilder builder, int nameOffset) { builder.addOffset(3, nameOffset, 0); }
  public static void addSnapshotCopy(FlatBufferBuilder builder, int snapshotCopyOffset) { builder.addOffset(4, snapshotCopyOffset, 0); }
  public static void addCreatedListSize(FlatBufferBuilder builder, long createdListSize) { builder.addInt(5, (int)(createdListSize & 0xFFFFFFFFL), 0); }
  public static void addDeletedINode(FlatBufferBuilder builder, int deletedINodeOffset) { builder.addOffset(6, deletedINodeOffset, 0); }
  public static int createDeletedINodeVector(FlatBufferBuilder builder, long[] data) { builder.startVector(8, data.length, 8); for (int i = data.length - 1; i >= 0; i--) builder.addLong(data[i]); return builder.endVector(); }
  public static void startDeletedINodeVector(FlatBufferBuilder builder, int numElems) { builder.startVector(8, numElems, 8); }
  public static void addDeletedINodeRef(FlatBufferBuilder builder, int deletedINodeRefOffset) { builder.addOffset(7, deletedINodeRefOffset, 0); }
  public static int createDeletedINodeRefVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addInt(data[i]); return builder.endVector(); }
  public static void startDeletedINodeRefVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endIntelDirectoryDiff(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishIntelDirectoryDiffBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }

};

