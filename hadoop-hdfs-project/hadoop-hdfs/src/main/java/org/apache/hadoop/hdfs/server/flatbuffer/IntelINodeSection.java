// automatically generated, do not modify
package org.apache.hadoop.hdfs.server.flatbuffer;
import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class IntelINodeSection extends Table {
  public static IntelINodeSection getRootAsIntelINodeSection(ByteBuffer _bb) { return getRootAsIntelINodeSection(_bb, new IntelINodeSection()); }
  public static IntelINodeSection getRootAsIntelINodeSection(ByteBuffer _bb, IntelINodeSection obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public IntelINodeSection __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public long lastInodeId() { int o = __offset(4); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public long numInodes() { int o = __offset(6); return o != 0 ? bb.getLong(o + bb_pos) : 0; }

  public static int createIntelINodeSection(FlatBufferBuilder builder,
      long lastInodeId,
      long numInodes) {
    builder.startObject(2);
    IntelINodeSection.addNumInodes(builder, numInodes);
    IntelINodeSection.addLastInodeId(builder, lastInodeId);
    return IntelINodeSection.endIntelINodeSection(builder);
  }

  public static void startIntelINodeSection(FlatBufferBuilder builder) { builder.startObject(2); }
  public static void addLastInodeId(FlatBufferBuilder builder, long lastInodeId) { builder.addLong(0, lastInodeId, 0); }
  public static void addNumInodes(FlatBufferBuilder builder, long numInodes) { builder.addLong(1, numInodes, 0); }
  public static int endIntelINodeSection(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishIntelINodeSectionBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
};

