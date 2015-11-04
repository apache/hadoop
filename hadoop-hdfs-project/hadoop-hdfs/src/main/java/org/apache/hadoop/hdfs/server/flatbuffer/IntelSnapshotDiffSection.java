// automatically generated, do not modify
package org.apache.hadoop.hdfs.server.flatbuffer;
import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class IntelSnapshotDiffSection extends Table {
  public static IntelSnapshotDiffSection getRootAsIntelSnapshotDiffSection(ByteBuffer _bb) { return getRootAsIntelSnapshotDiffSection(_bb, new IntelSnapshotDiffSection()); }
  public static IntelSnapshotDiffSection getRootAsIntelSnapshotDiffSection(ByteBuffer _bb, IntelSnapshotDiffSection obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public IntelSnapshotDiffSection __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }


  public static void startIntelSnapshotDiffSection(FlatBufferBuilder builder) { builder.startObject(0); }
  public static int endIntelSnapshotDiffSection(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishIntelSnapshotDiffSectionBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }

};

