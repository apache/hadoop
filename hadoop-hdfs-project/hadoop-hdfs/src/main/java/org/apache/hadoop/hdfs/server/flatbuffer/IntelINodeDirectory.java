// automatically generated, do not modify
package org.apache.hadoop.hdfs.server.flatbuffer;
import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class IntelINodeDirectory extends Table {
  public static IntelINodeDirectory getRootAsIntelINodeDirectory(ByteBuffer _bb) { return getRootAsIntelINodeDirectory(_bb, new IntelINodeDirectory()); }
  public static IntelINodeDirectory getRootAsIntelINodeDirectory(ByteBuffer _bb, IntelINodeDirectory obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public IntelINodeDirectory __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public long modificationTime() { int o = __offset(4); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public long nsQuota() { int o = __offset(6); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public long dsQuota() { int o = __offset(8); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public long permission() { int o = __offset(10); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public IntelAclFeatureProto acl() { return acl(new IntelAclFeatureProto()); }
  public IntelAclFeatureProto acl(IntelAclFeatureProto obj) { int o = __offset(12); return o != 0 ? obj.__init(__indirect(o + bb_pos), bb) : null; }
  public IntelXAttrFeatureProto xAttrs() { return xAttrs(new IntelXAttrFeatureProto()); }
  public IntelXAttrFeatureProto xAttrs(IntelXAttrFeatureProto obj) { int o = __offset(14); return o != 0 ? obj.__init(__indirect(o + bb_pos), bb) : null; }
  public IntelQuotaByStorageTypeFeatureProto typeQuotas() { return typeQuotas(new IntelQuotaByStorageTypeFeatureProto()); }
  public IntelQuotaByStorageTypeFeatureProto typeQuotas(IntelQuotaByStorageTypeFeatureProto obj) { int o = __offset(16); return o != 0 ? obj.__init(__indirect(o + bb_pos), bb) : null; }

  public static int createIntelINodeDirectory(FlatBufferBuilder builder,
      long modificationTime,
      long nsQuota,
      long dsQuota,
      long permission,
      int acl,
      int xAttrs,
      int typeQuotas) {
    builder.startObject(7);
    IntelINodeDirectory.addPermission(builder, permission);
    IntelINodeDirectory.addDsQuota(builder, dsQuota);
    IntelINodeDirectory.addNsQuota(builder, nsQuota);
    IntelINodeDirectory.addModificationTime(builder, modificationTime);
    IntelINodeDirectory.addTypeQuotas(builder, typeQuotas);
    IntelINodeDirectory.addXAttrs(builder, xAttrs);
    IntelINodeDirectory.addAcl(builder, acl);
    return IntelINodeDirectory.endIntelINodeDirectory(builder);
  }

  public static void startIntelINodeDirectory(FlatBufferBuilder builder) { builder.startObject(7); }
  public static void addModificationTime(FlatBufferBuilder builder, long modificationTime) { builder.addLong(0, modificationTime, 0); }
  public static void addNsQuota(FlatBufferBuilder builder, long nsQuota) { builder.addLong(1, nsQuota, 0); }
  public static void addDsQuota(FlatBufferBuilder builder, long dsQuota) { builder.addLong(2, dsQuota, 0); }
  public static void addPermission(FlatBufferBuilder builder, long permission) { builder.addLong(3, permission, 0); }
  public static void addAcl(FlatBufferBuilder builder, int aclOffset) { builder.addOffset(4, aclOffset, 0); }
  public static void addXAttrs(FlatBufferBuilder builder, int xAttrsOffset) { builder.addOffset(5, xAttrsOffset, 0); }
  public static void addTypeQuotas(FlatBufferBuilder builder, int typeQuotasOffset) { builder.addOffset(6, typeQuotasOffset, 0); }
  public static int endIntelINodeDirectory(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishIntelINodeDirectoryBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }

};

