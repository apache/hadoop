package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.yarn.server.api.protocolrecords.DatabaseAccessRequest;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.DatabaseAccessRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.DatabaseAccessRequestProtoOrBuilder;

public class DatabaseAccessRequestPBImpl extends DatabaseAccessRequest {

  DatabaseAccessRequestProto proto = DatabaseAccessRequestProto.getDefaultInstance();
  DatabaseAccessRequestProto.Builder builder = null;
  boolean viaProto = false;

  public DatabaseAccessRequestPBImpl() {
    builder = DatabaseAccessRequestProto.newBuilder();
  }

  public DatabaseAccessRequestPBImpl(DatabaseAccessRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public DatabaseAccessRequestProto getProto() {
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
  public void setOperation(String operation) {
    maybeInitBuilder();
    if (operation != null) {
      builder.setOperation(operation);
    } else {
      builder.clearOperation();
    }
  }

  @Override
  public void setDatabase(String database) {
    maybeInitBuilder();
    if (database != null) {
      builder.setDatabase(database);
    } else {
      builder.clearDatabase();
    }
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
    maybeInitBuilder();
    if (value != null) {
      builder.setValue(value);
    } else {
      builder.clearValue();
    }
  }

  @Override
  public String getOperation() {
    DatabaseAccessRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.hasOperation() ? p.getOperation() : null;
  }

  @Override
  public String getDatabase() {
    DatabaseAccessRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.hasDatabase() ? p.getDatabase() : null;
  }

  @Override
  public String getKey() {
    DatabaseAccessRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.hasKey() ? p.getKey() : null;
  }

  @Override
  public String getValue() {
    DatabaseAccessRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.hasValue() ? p.getValue() : null;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = DatabaseAccessRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
}
