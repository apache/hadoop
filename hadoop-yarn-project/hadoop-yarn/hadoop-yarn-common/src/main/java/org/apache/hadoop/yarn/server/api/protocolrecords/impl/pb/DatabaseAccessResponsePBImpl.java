package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos;
import org.apache.hadoop.yarn.server.api.protocolrecords.DBRecord;
import org.apache.hadoop.yarn.server.api.protocolrecords.DatabaseAccessResponse;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.DatabaseAccessResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.DatabaseAccessResponseProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.DBRecordProto;

public class DatabaseAccessResponsePBImpl extends DatabaseAccessResponse {

  DatabaseAccessResponseProto proto = DatabaseAccessResponseProto.getDefaultInstance();
  DatabaseAccessResponseProto.Builder builder = null;
  boolean viaProto = false;

  List<DBRecord> dbRecords;

  public DatabaseAccessResponsePBImpl() {
    builder = DatabaseAccessResponseProto.newBuilder();
  }

  public DatabaseAccessResponsePBImpl(DatabaseAccessResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public DatabaseAccessResponseProto getProto() {
    mergeLocalToProto();
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

  private void initDBRecords() {
    if (this.dbRecords != null) {
      return;
    }
    DatabaseAccessResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<YarnServerResourceManagerServiceProtos.DBRecordProto> dbRecordPBS = p.getRecordsList();
    this.dbRecords = new ArrayList<>();
    for (YarnServerResourceManagerServiceProtos.DBRecordProto record : dbRecordPBS) {
      this.dbRecords.add(new DBRecordPBImpl(record));
    }
  }

  @Override
  public void setRecords(List<DBRecord> records) {
    initDBRecords();
    dbRecords.clear();
    dbRecords.addAll(records);
  }

  @Override
  public List<DBRecord> getRecords() {
    initDBRecords();
    return this.dbRecords;
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (this.dbRecords != null) {
      addDbRecordsToProto();
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = DatabaseAccessResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void addDbRecordsToProto() {
    maybeInitBuilder();
    builder.clearRecords();
    if (dbRecords == null) {
      return;
    }
    Iterable<DBRecordProto> iterable = new Iterable<DBRecordProto>() {
      @Override
      public Iterator<DBRecordProto> iterator() {
        return new Iterator<DBRecordProto>() {

          Iterator<DBRecord> iter = dbRecords.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public DBRecordProto next() {
            DBRecord record = iter.next();
            return ((DBRecordPBImpl)record).getProto();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
    builder.addAllRecords(iterable);
  }

}
