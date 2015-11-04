// automatically generated, do not modify
package org.apache.hadoop.hdfs.server.flatbuffer;
import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class IntelDirEntry extends Table {
  public static IntelDirEntry getRootAsIntelDirEntry(ByteBuffer _bb) { return getRootAsIntelDirEntry(_bb, new IntelDirEntry()); }
  public static IntelDirEntry getRootAsIntelDirEntry(ByteBuffer _bb, IntelDirEntry obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public IntelDirEntry __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public long parent() { int o = __offset(4); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public long children(int j) { int o = __offset(6); return o != 0 ? bb.getLong(__vector(o) + j * 8) : 0; }
  public int childrenLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer childrenAsByteBuffer() { return __vector_as_bytebuffer(6, 8); }
  public long refChildren(int j) { int o = __offset(8); return o != 0 ? (long)bb.getInt(__vector(o) + j * 4) & 0xFFFFFFFFL : 0; }
  public int refChildrenLength() { int o = __offset(8); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer refChildrenAsByteBuffer() { return __vector_as_bytebuffer(8, 4); }

  public static int createIntelDirEntry(FlatBufferBuilder builder,
      long parent,
      int children,
      int refChildren) {
    builder.startObject(3);
    IntelDirEntry.addParent(builder, parent);
    IntelDirEntry.addRefChildren(builder, refChildren);
    IntelDirEntry.addChildren(builder, children);
    return IntelDirEntry.endIntelDirEntry(builder);
  }

  public static void startIntelDirEntry(FlatBufferBuilder builder) { builder.startObject(3); }
  public static void addParent(FlatBufferBuilder builder, long parent) { builder.addLong(0, parent, 0); }
  public static void addChildren(FlatBufferBuilder builder, int childrenOffset) { builder.addOffset(1, childrenOffset, 0); }
  public static int createChildrenVector(FlatBufferBuilder builder, long[] data) { builder.startVector(8, data.length, 8); for (int i = data.length - 1; i >= 0; i--) builder.addLong(data[i]); return builder.endVector(); }
  public static void startChildrenVector(FlatBufferBuilder builder, int numElems) { builder.startVector(8, numElems, 8); }
  public static void addRefChildren(FlatBufferBuilder builder, int refChildrenOffset) { builder.addOffset(2, refChildrenOffset, 0); }
  public static int createRefChildrenVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addInt(data[i]); return builder.endVector(); }
  public static void startRefChildrenVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endIntelDirEntry(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishIntelDirEntryBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }

};

