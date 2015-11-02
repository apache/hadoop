// automatically generated, do not modify
package org.apache.hadoop.hdfs.server.flatbuffer;
import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class IntelFileDiff extends Table {
  public static IntelFileDiff getRootAsIntelFileDiff(ByteBuffer _bb) { return getRootAsIntelFileDiff(_bb, new IntelFileDiff()); }
  public static IntelFileDiff getRootAsIntelFileDiff(ByteBuffer _bb, IntelFileDiff obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public IntelFileDiff __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public long snapshotId() { int o = __offset(4); return o != 0 ? (long)bb.getInt(o + bb_pos) & 0xFFFFFFFFL : 0; }
  public long fileSize() { int o = __offset(6); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public String name() { int o = __offset(8); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer nameAsByteBuffer() { return __vector_as_bytebuffer(8, 1); }
  public IntelINodeFile snapshotCopy() { return snapshotCopy(new IntelINodeFile()); }
  public IntelINodeFile snapshotCopy(IntelINodeFile obj) { int o = __offset(10); return o != 0 ? obj.__init(__indirect(o + bb_pos), bb) : null; }
  public IntelBlockProto blocks(int j) { return blocks(new IntelBlockProto(), j); }
  public IntelBlockProto blocks(IntelBlockProto obj, int j) { int o = __offset(12); return o != 0 ? obj.__init(__indirect(__vector(o) + j * 4), bb) : null; }
  public int blocksLength() { int o = __offset(12); return o != 0 ? __vector_len(o) : 0; }

  public static int createIntelFileDiff(FlatBufferBuilder builder,
      long snapshotId,
      long fileSize,
      int name,
      int snapshotCopy,
      int blocks) {
    builder.startObject(5);
    IntelFileDiff.addFileSize(builder, fileSize);
    IntelFileDiff.addBlocks(builder, blocks);
    IntelFileDiff.addSnapshotCopy(builder, snapshotCopy);
    IntelFileDiff.addName(builder, name);
    IntelFileDiff.addSnapshotId(builder, snapshotId);
    return IntelFileDiff.endIntelFileDiff(builder);
  }

  public static void startIntelFileDiff(FlatBufferBuilder builder) { builder.startObject(5); }
  public static void addSnapshotId(FlatBufferBuilder builder, long snapshotId) { builder.addInt(0, (int)(snapshotId & 0xFFFFFFFFL), 0); }
  public static void addFileSize(FlatBufferBuilder builder, long fileSize) { builder.addLong(1, fileSize, 0); }
  public static void addName(FlatBufferBuilder builder, int nameOffset) { builder.addOffset(2, nameOffset, 0); }
  public static void addSnapshotCopy(FlatBufferBuilder builder, int snapshotCopyOffset) { builder.addOffset(3, snapshotCopyOffset, 0); }
  public static void addBlocks(FlatBufferBuilder builder, int blocksOffset) { builder.addOffset(4, blocksOffset, 0); }
  public static int createBlocksVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startBlocksVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endIntelFileDiff(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishIntelFileDiffBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
};

