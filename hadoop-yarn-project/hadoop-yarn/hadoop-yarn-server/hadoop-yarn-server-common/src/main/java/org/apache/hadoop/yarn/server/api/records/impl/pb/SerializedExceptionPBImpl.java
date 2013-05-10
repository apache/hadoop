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

package org.apache.hadoop.yarn.server.api.records.impl.pb;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.hadoop.yarn.proto.YarnProtos.SerializedExceptionProto;
import org.apache.hadoop.yarn.proto.YarnProtos.SerializedExceptionProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.records.SerializedException;

public class SerializedExceptionPBImpl extends SerializedException {

  SerializedExceptionProto proto = SerializedExceptionProto
      .getDefaultInstance();
  SerializedExceptionProto.Builder builder = null;
  boolean viaProto = false;

  public SerializedExceptionPBImpl() {
  }

  public SerializedExceptionPBImpl(SerializedExceptionProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  private SerializedExceptionPBImpl(Throwable t) {
    init(t);
  }

  public void init(String message) {
    maybeInitBuilder();
    builder.setMessage(message);
  }

  public void init(Throwable t) {
    maybeInitBuilder();
    if (t == null) {
      return;
    }

    if (t.getCause() == null) {
    } else {
      builder.setCause(new SerializedExceptionPBImpl(t.getCause()).getProto());
      builder.setClassName(t.getClass().getCanonicalName());
    }
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    t.printStackTrace(pw);
    pw.close();
    if (sw.toString() != null)
      builder.setTrace(sw.toString());
    if (t.getMessage() != null)
      builder.setMessage(t.getMessage());
  }

  public void init(String message, Throwable t) {
    init(t);
    if (message != null)
      builder.setMessage(message);
  }

  @Override
  public String getMessage() {
    SerializedExceptionProtoOrBuilder p = viaProto ? proto : builder;
    return p.getMessage();
  }

  @Override
  public String getRemoteTrace() {
    SerializedExceptionProtoOrBuilder p = viaProto ? proto : builder;
    return p.getTrace();
  }

  @Override
  public SerializedException getCause() {
    SerializedExceptionProtoOrBuilder p = viaProto ? proto : builder;
    if (p.hasCause()) {
      return new SerializedExceptionPBImpl(p.getCause());
    } else {
      return null;
    }
  }

  public SerializedExceptionProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SerializedExceptionProto.newBuilder(proto);
    }
    viaProto = false;
  }
}