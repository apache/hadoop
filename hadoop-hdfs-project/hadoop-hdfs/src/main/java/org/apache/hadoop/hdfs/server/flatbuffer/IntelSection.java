// automatically generated, do not modify
package org.apache.hadoop.hdfs.server.flatbuffer;
import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class IntelSection extends Table {
  public static IntelSection getRootAsIntelSection(ByteBuffer _bb) { return getRootAsIntelSection(_bb, new IntelSection()); }
  public static IntelSection getRootAsIntelSection(ByteBuffer _bb, IntelSection obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public IntelSection __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public String name() { int o = __offset(4); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer nameAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public long length() { int o = __offset(6); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public long offset() { int o = __offset(8); return o != 0 ? bb.getLong(o + bb_pos) : 0; }

  public static int createIntelSection(FlatBufferBuilder builder,
      int name,
      long length,
      long offset) {
    builder.startObject(3);
    IntelSection.addOffset(builder, offset);
    IntelSection.addLength(builder, length);
    IntelSection.addName(builder, name);
    return IntelSection.endIntelSection(builder);
  }

  public static void startIntelSection(FlatBufferBuilder builder) { builder.startObject(3); }
  public static void addName(FlatBufferBuilder builder, int nameOffset) { builder.addOffset(0, nameOffset, 0); }
  public static void addLength(FlatBufferBuilder builder, long length) { builder.addLong(1, length, 0); }
  public static void addOffset(FlatBufferBuilder builder, long offset) { builder.addLong(2, offset, 0); }
  public static int endIntelSection(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishIntelSectionBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }

};

