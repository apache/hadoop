/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.DBRecordProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.DatabaseAccessResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.DatabaseAccessResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.DBRecord;
import org.apache.hadoop.yarn.server.api.protocolrecords.DatabaseAccessResponse;

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
    if (this.dbRecords != null) {
      dbRecords.clear();
    } else {
      dbRecords = new ArrayList<>();
    }
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
