package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.DBRecordProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.DBRecordProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.DBRecord;

public class DBRecordPBImpl extends DBRecord {

  DBRecordProto proto = DBRecordProto.getDefaultInstance();
  DBRecordProto.Builder builder = null;
  boolean viaProto = false;

  public DBRecordPBImpl() {
    builder = DBRecordProto.newBuilder();
  }

  public DBRecordPBImpl(DBRecordProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public DBRecordProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  @Override
  public void setKey(String key) {
    maybeInitBuilder();
    if (key != null) {
      builder.setKey(key);
    } else {
      builder.clearKey();
    }
  }

  @Override
  public void setValue(String value) {
    maybeInitBuilder();;
    if (value != null) {
      builder.setValue(value);
    } else {
      builder.clearValue();
    }
  }

  @Override
  public String getKey() {
    DBRecordProtoOrBuilder p = viaProto ? proto : builder;
    return p.hasKey() ? p.getKey() : null;
  }

  @Override
  public String getValue() {
    DBRecordProtoOrBuilder p = viaProto ? proto : builder;
    return p.hasValue() ? p.getValue() : null;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = DBRecordProto.newBuilder(proto);
    }
    viaProto = false;
  }
}
