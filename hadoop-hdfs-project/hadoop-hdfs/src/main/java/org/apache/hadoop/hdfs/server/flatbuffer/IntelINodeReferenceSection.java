// automatically generated, do not modify
package org.apache.hadoop.hdfs.server.flatbuffer;
import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class IntelINodeReferenceSection extends Table {
  public static IntelINodeReferenceSection getRootAsIntelINodeReferenceSection(ByteBuffer _bb) { return getRootAsIntelINodeReferenceSection(_bb, new IntelINodeReferenceSection()); }
  public static IntelINodeReferenceSection getRootAsIntelINodeReferenceSection(ByteBuffer _bb, IntelINodeReferenceSection obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public IntelINodeReferenceSection __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }


  public static void startIntelINodeReferenceSection(FlatBufferBuilder builder) { builder.startObject(0); }
  public static int endIntelINodeReferenceSection(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishIntelINodeReferenceSectionBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }

};

