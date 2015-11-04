// automatically generated, do not modify
package org.apache.hadoop.hdfs.server.flatbuffer;
import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class IntelINodeDirectorySection extends Table {
  public static IntelINodeDirectorySection getRootAsIntelINodeDirectorySection(ByteBuffer _bb) { return getRootAsIntelINodeDirectorySection(_bb, new IntelINodeDirectorySection()); }
  public static IntelINodeDirectorySection getRootAsIntelINodeDirectorySection(ByteBuffer _bb, IntelINodeDirectorySection obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public IntelINodeDirectorySection __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }


  public static void startIntelINodeDirectorySection(FlatBufferBuilder builder) { builder.startObject(0); }
  public static int endIntelINodeDirectorySection(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishIntelINodeDirectorySectionBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }

};

