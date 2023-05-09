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
