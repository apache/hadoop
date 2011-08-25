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

package org.apache.hadoop.yarn.exceptions.impl.pb;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.proto.YarnProtos.YarnRemoteExceptionProto;
import org.apache.hadoop.yarn.proto.YarnProtos.YarnRemoteExceptionProtoOrBuilder;

public class YarnRemoteExceptionPBImpl extends YarnRemoteException {

  private static final long serialVersionUID = 1L;

  YarnRemoteExceptionProto proto = YarnRemoteExceptionProto.getDefaultInstance();
  YarnRemoteExceptionProto.Builder builder = null;
  boolean viaProto = false;

  public YarnRemoteExceptionPBImpl() {
  }

  public YarnRemoteExceptionPBImpl(YarnRemoteExceptionProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public YarnRemoteExceptionPBImpl(String message) {
    super(message);
    maybeInitBuilder();
    builder.setMessage(super.getMessage());
  }

  public YarnRemoteExceptionPBImpl(Throwable t) {
    super(t);
    maybeInitBuilder();

    if (t.getCause() == null) { 
    } else {
      builder.setCause(new YarnRemoteExceptionPBImpl(t.getCause()).getProto());
      builder.setClassName(t.getClass().getName());
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
  
  public YarnRemoteExceptionPBImpl(String message, Throwable t) {
    this(t);
    if (message != null) 
      builder.setMessage(message);
  }
  @Override
  public String getMessage() {
    YarnRemoteExceptionProtoOrBuilder p = viaProto ? proto : builder;
    return p.getMessage();
  }
  
  @Override
  public String getRemoteTrace() {
    YarnRemoteExceptionProtoOrBuilder p = viaProto ? proto : builder;
    return p.getTrace();
  }

  @Override
  public YarnRemoteException getCause() {
    YarnRemoteExceptionProtoOrBuilder p = viaProto ? proto : builder;
    if (p.hasCause()) {
      return new YarnRemoteExceptionPBImpl(p.getCause());
    } else {
      return null;
    }
  }

  public YarnRemoteExceptionProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = YarnRemoteExceptionProto.newBuilder(proto);
    }
    viaProto = false;
  }
}