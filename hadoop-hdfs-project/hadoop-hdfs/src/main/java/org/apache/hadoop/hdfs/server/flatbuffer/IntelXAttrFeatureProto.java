// automatically generated, do not modify
package org.apache.hadoop.hdfs.server.flatbuffer;
import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class IntelXAttrFeatureProto extends Table {
  public static IntelXAttrFeatureProto getRootAsIntelXAttrFeatureProto(ByteBuffer _bb) { return getRootAsIntelXAttrFeatureProto(_bb, new IntelXAttrFeatureProto()); }
  public static IntelXAttrFeatureProto getRootAsIntelXAttrFeatureProto(ByteBuffer _bb, IntelXAttrFeatureProto obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public IntelXAttrFeatureProto __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public IntelXAttrCompactProto xAttrs(int j) { return xAttrs(new IntelXAttrCompactProto(), j); }
  public IntelXAttrCompactProto xAttrs(IntelXAttrCompactProto obj, int j) { int o = __offset(4); return o != 0 ? obj.__init(__indirect(__vector(o) + j * 4), bb) : null; }
  public int xAttrsLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }

  public static int createIntelXAttrFeatureProto(FlatBufferBuilder builder,
      int xAttrs) {
    builder.startObject(1);
    IntelXAttrFeatureProto.addXAttrs(builder, xAttrs);
    return IntelXAttrFeatureProto.endIntelXAttrFeatureProto(builder);
  }

  public static void startIntelXAttrFeatureProto(FlatBufferBuilder builder) { builder.startObject(1); }
  public static void addXAttrs(FlatBufferBuilder builder, int xAttrsOffset) { builder.addOffset(0, xAttrsOffset, 0); }
  public static int createXAttrsVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startXAttrsVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endIntelXAttrFeatureProto(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishIntelXAttrFeatureProtoBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }

};

