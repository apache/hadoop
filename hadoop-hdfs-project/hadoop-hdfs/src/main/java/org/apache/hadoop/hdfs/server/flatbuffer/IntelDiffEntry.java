// automatically generated, do not modify
package org.apache.hadoop.hdfs.server.flatbuffer;
import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class IntelDiffEntry extends Table {
  public static IntelDiffEntry getRootAsIntelDiffEntry(ByteBuffer _bb) { return getRootAsIntelDiffEntry(_bb, new IntelDiffEntry()); }
  public static IntelDiffEntry getRootAsIntelDiffEntry(ByteBuffer _bb, IntelDiffEntry obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public IntelDiffEntry __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public int type() { int o = __offset(4); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public long inodeId() { int o = __offset(6); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public long numOfDiff() { int o = __offset(8); return o != 0 ? (long)bb.getInt(o + bb_pos) & 0xFFFFFFFFL : 0; }

  public static int createIntelDiffEntry(FlatBufferBuilder builder,
      int type,
      long inodeId,
      long numOfDiff) {
    builder.startObject(3);
    IntelDiffEntry.addInodeId(builder, inodeId);
    IntelDiffEntry.addNumOfDiff(builder, numOfDiff);
    IntelDiffEntry.addType(builder, type);
    return IntelDiffEntry.endIntelDiffEntry(builder);
  }

  public static void startIntelDiffEntry(FlatBufferBuilder builder) { builder.startObject(3); }
  public static void addType(FlatBufferBuilder builder, int type) { builder.addInt(0, type, 0); }
  public static void addInodeId(FlatBufferBuilder builder, long inodeId) { builder.addLong(1, inodeId, 0); }
  public static void addNumOfDiff(FlatBufferBuilder builder, long numOfDiff) { builder.addInt(2, (int)(numOfDiff & 0xFFFFFFFFL), 0); }
  public static int endIntelDiffEntry(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishIntelDiffEntryBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
};

