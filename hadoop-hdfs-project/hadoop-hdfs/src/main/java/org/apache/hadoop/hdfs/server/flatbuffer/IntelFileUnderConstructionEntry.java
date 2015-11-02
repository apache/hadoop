// automatically generated, do not modify
package org.apache.hadoop.hdfs.server.flatbuffer;
import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class IntelFileUnderConstructionEntry extends Table {
  public static IntelFileUnderConstructionEntry getRootAsIntelFileUnderConstructionEntry(ByteBuffer _bb) { return getRootAsIntelFileUnderConstructionEntry(_bb, new IntelFileUnderConstructionEntry()); }
  public static IntelFileUnderConstructionEntry getRootAsIntelFileUnderConstructionEntry(ByteBuffer _bb, IntelFileUnderConstructionEntry obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public IntelFileUnderConstructionEntry __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public long inodeId() { int o = __offset(4); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public String fullPath() { int o = __offset(6); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer fullPathAsByteBuffer() { return __vector_as_bytebuffer(6, 1); }

  public static int createIntelFileUnderConstructionEntry(FlatBufferBuilder builder,
      long inodeId,
      int fullPath) {
    builder.startObject(2);
    IntelFileUnderConstructionEntry.addInodeId(builder, inodeId);
    IntelFileUnderConstructionEntry.addFullPath(builder, fullPath);
    return IntelFileUnderConstructionEntry.endIntelFileUnderConstructionEntry(builder);
  }

  public static void startIntelFileUnderConstructionEntry(FlatBufferBuilder builder) { builder.startObject(2); }
  public static void addInodeId(FlatBufferBuilder builder, long inodeId) { builder.addLong(0, inodeId, 0); }
  public static void addFullPath(FlatBufferBuilder builder, int fullPathOffset) { builder.addOffset(1, fullPathOffset, 0); }
  public static int endIntelFileUnderConstructionEntry(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishIntelFileUnderConstructionEntryBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }

};

