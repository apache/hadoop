// automatically generated, do not modify
package org.apache.hadoop.hdfs.server.flatbuffer;
import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class IntelQuotaByStorageTypeFeatureProto extends Table {
  public static IntelQuotaByStorageTypeFeatureProto getRootAsIntelQuotaByStorageTypeFeatureProto(ByteBuffer _bb) { return getRootAsIntelQuotaByStorageTypeFeatureProto(_bb, new IntelQuotaByStorageTypeFeatureProto()); }
  public static IntelQuotaByStorageTypeFeatureProto getRootAsIntelQuotaByStorageTypeFeatureProto(ByteBuffer _bb, IntelQuotaByStorageTypeFeatureProto obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public IntelQuotaByStorageTypeFeatureProto __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public IntelQuotaByStorageTypeEntryProto quotas(int j) { return quotas(new IntelQuotaByStorageTypeEntryProto(), j); }
  public IntelQuotaByStorageTypeEntryProto quotas(IntelQuotaByStorageTypeEntryProto obj, int j) { int o = __offset(4); return o != 0 ? obj.__init(__indirect(__vector(o) + j * 4), bb) : null; }
  public int quotasLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }

  public static int createIntelQuotaByStorageTypeFeatureProto(FlatBufferBuilder builder,
      int quotas) {
    builder.startObject(1);
    IntelQuotaByStorageTypeFeatureProto.addQuotas(builder, quotas);
    return IntelQuotaByStorageTypeFeatureProto.endIntelQuotaByStorageTypeFeatureProto(builder);
  }

  public static void startIntelQuotaByStorageTypeFeatureProto(FlatBufferBuilder builder) { builder.startObject(1); }
  public static void addQuotas(FlatBufferBuilder builder, int quotasOffset) { builder.addOffset(0, quotasOffset, 0); }
  public static int createQuotasVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startQuotasVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endIntelQuotaByStorageTypeFeatureProto(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishIntelQuotaByStorageTypeFeatureProtoBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }

};

