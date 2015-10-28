// automatically generated, do not modify
package org.apache.hadoop.hdfs.server.flatbuffer;
import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class IntelStringTableSection extends Table {
  public static IntelStringTableSection getRootAsIntelStringTableSection(ByteBuffer _bb) { return getRootAsIntelStringTableSection(_bb, new IntelStringTableSection()); }
  public static IntelStringTableSection getRootAsIntelStringTableSection(ByteBuffer _bb, IntelStringTableSection obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public IntelStringTableSection __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public long numEntry() { int o = __offset(4); return o != 0 ? (long)bb.getInt(o + bb_pos) & 0xFFFFFFFFL : 0; }

  public static int createIntelStringTableSection(FlatBufferBuilder builder,
      long numEntry) {
    builder.startObject(1);
    IntelStringTableSection.addNumEntry(builder, numEntry);
    return IntelStringTableSection.endIntelStringTableSection(builder);
  }

  public static void startIntelStringTableSection(FlatBufferBuilder builder) { builder.startObject(1); }
  public static void addNumEntry(FlatBufferBuilder builder, long numEntry) { builder.addInt(0, (int)(numEntry & 0xFFFFFFFFL), 0); }
  public static int endIntelStringTableSection(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishIntelStringTableSectionBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
};

