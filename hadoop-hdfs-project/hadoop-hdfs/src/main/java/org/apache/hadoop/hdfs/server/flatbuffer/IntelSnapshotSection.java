// automatically generated, do not modify
package org.apache.hadoop.hdfs.server.flatbuffer;
import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class IntelSnapshotSection extends Table {
  public static IntelSnapshotSection getRootAsIntelSnapshotSection(ByteBuffer _bb) { return getRootAsIntelSnapshotSection(_bb, new IntelSnapshotSection()); }
  public static IntelSnapshotSection getRootAsIntelSnapshotSection(ByteBuffer _bb, IntelSnapshotSection obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public IntelSnapshotSection __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public long snapshotCounter() { int o = __offset(4); return o != 0 ? (long)bb.getInt(o + bb_pos) & 0xFFFFFFFFL : 0; }
  public long snapshottableDir(int j) { int o = __offset(6); return o != 0 ? bb.getLong(__vector(o) + j * 8) : 0; }
  public int snapshottableDirLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer snapshottableDirAsByteBuffer() { return __vector_as_bytebuffer(6, 8); }
  public long numSnapshots() { int o = __offset(8); return o != 0 ? (long)bb.getInt(o + bb_pos) & 0xFFFFFFFFL : 0; }

  public static int createIntelSnapshotSection(FlatBufferBuilder builder,
      long snapshotCounter,
      int snapshottableDir,
      long numSnapshots) {
    builder.startObject(3);
    IntelSnapshotSection.addNumSnapshots(builder, numSnapshots);
    IntelSnapshotSection.addSnapshottableDir(builder, snapshottableDir);
    IntelSnapshotSection.addSnapshotCounter(builder, snapshotCounter);
    return IntelSnapshotSection.endIntelSnapshotSection(builder);
  }

  public static void startIntelSnapshotSection(FlatBufferBuilder builder) { builder.startObject(3); }
  public static void addSnapshotCounter(FlatBufferBuilder builder, long snapshotCounter) { builder.addInt(0, (int)(snapshotCounter & 0xFFFFFFFFL), 0); }
  public static void addSnapshottableDir(FlatBufferBuilder builder, int snapshottableDirOffset) { builder.addOffset(1, snapshottableDirOffset, 0); }
  public static int createSnapshottableDirVector(FlatBufferBuilder builder, long[] data) { builder.startVector(8, data.length, 8); for (int i = data.length - 1; i >= 0; i--) builder.addLong(data[i]); return builder.endVector(); }
  public static void startSnapshottableDirVector(FlatBufferBuilder builder, int numElems) { builder.startVector(8, numElems, 8); }
  public static void addNumSnapshots(FlatBufferBuilder builder, long numSnapshots) { builder.addInt(2, (int)(numSnapshots & 0xFFFFFFFFL), 0); }
  public static int endIntelSnapshotSection(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishIntelSnapshotSectionBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }

};

