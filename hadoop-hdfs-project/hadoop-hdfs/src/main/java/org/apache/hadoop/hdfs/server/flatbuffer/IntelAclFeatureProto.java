// automatically generated, do not modify

package org.apache.hadoop.hdfs.server.flatbuffer;
import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class IntelAclFeatureProto extends Table {
  public static IntelAclFeatureProto getRootAsIntelAclFeatureProto(ByteBuffer _bb) { return getRootAsIntelAclFeatureProto(_bb, new IntelAclFeatureProto()); }
  public static IntelAclFeatureProto getRootAsIntelAclFeatureProto(ByteBuffer _bb, IntelAclFeatureProto obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public IntelAclFeatureProto __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public long entries(int j) { int o = __offset(4); return o != 0 ? (long)bb.getInt(__vector(o) + j * 4) & 0xFFFFFFFFL : 0; }
  public int entriesLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer entriesAsByteBuffer() { return __vector_as_bytebuffer(4, 4); }

  public static int createIntelAclFeatureProto(FlatBufferBuilder builder,
      int entries) {
    builder.startObject(1);
    IntelAclFeatureProto.addEntries(builder, entries);
    return IntelAclFeatureProto.endIntelAclFeatureProto(builder);
  }

  public static void startIntelAclFeatureProto(FlatBufferBuilder builder) { builder.startObject(1); }
  public static void addEntries(FlatBufferBuilder builder, int entriesOffset) { builder.addOffset(0, entriesOffset, 0); }
  public static int createEntriesVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addInt(data[i]); return builder.endVector(); }
  public static void startEntriesVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endIntelAclFeatureProto(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishIntelAclFeatureProtoBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
};

