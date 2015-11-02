// automatically generated, do not modify
package org.apache.hadoop.hdfs.server.flatbuffer;
import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class IntelSnapshot extends Table {
  public static IntelSnapshot getRootAsIntelSnapshot(ByteBuffer _bb) { return getRootAsIntelSnapshot(_bb, new IntelSnapshot()); }
  public static IntelSnapshot getRootAsIntelSnapshot(ByteBuffer _bb, IntelSnapshot obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public IntelSnapshot __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public long snapshotId() { int o = __offset(4); return o != 0 ? (long)bb.getInt(o + bb_pos) & 0xFFFFFFFFL : 0; }
  public IntelINode root() { return root(new IntelINode()); }
  public IntelINode root(IntelINode obj) { int o = __offset(6); return o != 0 ? obj.__init(__indirect(o + bb_pos), bb) : null; }

  public static int createIntelSnapshot(FlatBufferBuilder builder,
      long snapshotId,
      int root) {
    builder.startObject(2);
    IntelSnapshot.addRoot(builder, root);
    IntelSnapshot.addSnapshotId(builder, snapshotId);
    return IntelSnapshot.endIntelSnapshot(builder);
  }

  public static void startIntelSnapshot(FlatBufferBuilder builder) { builder.startObject(2); }
  public static void addSnapshotId(FlatBufferBuilder builder, long snapshotId) { builder.addInt(0, (int)(snapshotId & 0xFFFFFFFFL), 0); }
  public static void addRoot(FlatBufferBuilder builder, int rootOffset) { builder.addOffset(1, rootOffset, 0); }
  public static int endIntelSnapshot(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishIntelSnapshotBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
};

