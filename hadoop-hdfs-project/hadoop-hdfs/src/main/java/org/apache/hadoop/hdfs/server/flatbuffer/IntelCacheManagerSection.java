// automatically generated, do not modify
package org.apache.hadoop.hdfs.server.flatbuffer;
import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class IntelCacheManagerSection extends Table {
  public static IntelCacheManagerSection getRootAsIntelCacheManagerSection(ByteBuffer _bb) { return getRootAsIntelCacheManagerSection(_bb, new IntelCacheManagerSection()); }
  public static IntelCacheManagerSection getRootAsIntelCacheManagerSection(ByteBuffer _bb, IntelCacheManagerSection obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public IntelCacheManagerSection __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public long nextDirectiveId() { int o = __offset(4); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public long numPools() { int o = __offset(6); return o != 0 ? (long)bb.getInt(o + bb_pos) & 0xFFFFFFFFL : 0; }
  public long numDirectives() { int o = __offset(8); return o != 0 ? (long)bb.getInt(o + bb_pos) & 0xFFFFFFFFL : 0; }

  public static int createIntelCacheManagerSection(FlatBufferBuilder builder,
      long nextDirectiveId,
      long numPools,
      long numDirectives) {
    builder.startObject(3);
    IntelCacheManagerSection.addNextDirectiveId(builder, nextDirectiveId);
    IntelCacheManagerSection.addNumDirectives(builder, numDirectives);
    IntelCacheManagerSection.addNumPools(builder, numPools);
    return IntelCacheManagerSection.endIntelCacheManagerSection(builder);
  }

  public static void startIntelCacheManagerSection(FlatBufferBuilder builder) { builder.startObject(3); }
  public static void addNextDirectiveId(FlatBufferBuilder builder, long nextDirectiveId) { builder.addLong(0, nextDirectiveId, 0); }
  public static void addNumPools(FlatBufferBuilder builder, long numPools) { builder.addInt(1, (int)(numPools & 0xFFFFFFFFL), 0); }
  public static void addNumDirectives(FlatBufferBuilder builder, long numDirectives) { builder.addInt(2, (int)(numDirectives & 0xFFFFFFFFL), 0); }
  public static int endIntelCacheManagerSection(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishIntelCacheManagerSectionBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }

};

