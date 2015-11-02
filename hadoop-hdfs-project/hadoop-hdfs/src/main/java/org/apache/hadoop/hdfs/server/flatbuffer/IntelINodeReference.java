// automatically generated, do not modify
package org.apache.hadoop.hdfs.server.flatbuffer;
import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class IntelINodeReference extends Table {
  public static IntelINodeReference getRootAsIntelINodeReference(ByteBuffer _bb) { return getRootAsIntelINodeReference(_bb, new IntelINodeReference()); }
  public static IntelINodeReference getRootAsIntelINodeReference(ByteBuffer _bb, IntelINodeReference obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public IntelINodeReference __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public long referredId() { int o = __offset(4); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public String name() { int o = __offset(6); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer nameAsByteBuffer() { return __vector_as_bytebuffer(6, 1); }
  public long dstSnapshotId() { int o = __offset(8); return o != 0 ? (long)bb.getInt(o + bb_pos) & 0xFFFFFFFFL : 0; }
  public long lastSnapshotId() { int o = __offset(10); return o != 0 ? (long)bb.getInt(o + bb_pos) & 0xFFFFFFFFL : 0; }

  public static int createIntelINodeReference(FlatBufferBuilder builder,
      long referredId,
      int name,
      long dstSnapshotId,
      long lastSnapshotId) {
    builder.startObject(4);
    IntelINodeReference.addReferredId(builder, referredId);
    IntelINodeReference.addLastSnapshotId(builder, lastSnapshotId);
    IntelINodeReference.addDstSnapshotId(builder, dstSnapshotId);
    IntelINodeReference.addName(builder, name);
    return IntelINodeReference.endIntelINodeReference(builder);
  }

  public static void startIntelINodeReference(FlatBufferBuilder builder) { builder.startObject(4); }
  public static void addReferredId(FlatBufferBuilder builder, long referredId) { builder.addLong(0, referredId, 0); }
  public static void addName(FlatBufferBuilder builder, int nameOffset) { builder.addOffset(1, nameOffset, 0); }
  public static void addDstSnapshotId(FlatBufferBuilder builder, long dstSnapshotId) { builder.addInt(2, (int)(dstSnapshotId & 0xFFFFFFFFL), 0); }
  public static void addLastSnapshotId(FlatBufferBuilder builder, long lastSnapshotId) { builder.addInt(3, (int)(lastSnapshotId & 0xFFFFFFFFL), 0); }
  public static int endIntelINodeReference(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishIntelINodeReferenceBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }

};

