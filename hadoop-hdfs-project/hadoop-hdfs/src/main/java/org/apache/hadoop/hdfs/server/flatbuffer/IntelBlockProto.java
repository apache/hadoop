// automatically generated, do not modify
package org.apache.hadoop.hdfs.server.flatbuffer;
import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class IntelBlockProto extends Table {
  public static IntelBlockProto getRootAsIntelBlockProto(ByteBuffer _bb) { return getRootAsIntelBlockProto(_bb, new IntelBlockProto()); }
  public static IntelBlockProto getRootAsIntelBlockProto(ByteBuffer _bb, IntelBlockProto obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public IntelBlockProto __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public long blockId() { int o = __offset(4); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public long genStamp() { int o = __offset(6); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public long numBytes() { int o = __offset(8); return o != 0 ? bb.getLong(o + bb_pos) : 0; }

  public static int createIntelBlockProto(FlatBufferBuilder builder,
      long blockId,
      long genStamp,
      long numBytes) {
    builder.startObject(3);
    IntelBlockProto.addNumBytes(builder, numBytes);
    IntelBlockProto.addGenStamp(builder, genStamp);
    IntelBlockProto.addBlockId(builder, blockId);
    return IntelBlockProto.endIntelBlockProto(builder);
  }

  public static void startIntelBlockProto(FlatBufferBuilder builder) { builder.startObject(3); }
  public static void addBlockId(FlatBufferBuilder builder, long blockId) { builder.addLong(0, blockId, 0); }
  public static void addGenStamp(FlatBufferBuilder builder, long genStamp) { builder.addLong(1, genStamp, 0); }
  public static void addNumBytes(FlatBufferBuilder builder, long numBytes) { builder.addLong(2, numBytes, 0); }
  public static int endIntelBlockProto(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishIntelBlockProtoBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }

};

