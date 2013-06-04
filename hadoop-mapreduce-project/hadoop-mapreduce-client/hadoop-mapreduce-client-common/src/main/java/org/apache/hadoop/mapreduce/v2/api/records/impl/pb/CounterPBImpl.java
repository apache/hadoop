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

package org.apache.hadoop.mapreduce.v2.api.records.impl.pb;


import org.apache.hadoop.mapreduce.v2.api.records.Counter;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.CounterProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.CounterProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;


    
public class CounterPBImpl extends ProtoBase<CounterProto> implements Counter {
  CounterProto proto = CounterProto.getDefaultInstance();
  CounterProto.Builder builder = null;
  boolean viaProto = false;
  
  public CounterPBImpl() {
    builder = CounterProto.newBuilder();
  }

  public CounterPBImpl(CounterProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public CounterProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = CounterProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public String getName() {
    CounterProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasName()) {
      return null;
    }
    return (p.getName());
  }

  @Override
  public void setName(String name) {
    maybeInitBuilder();
    if (name == null) {
      builder.clearName();
      return;
    }
    builder.setName((name));
  }
  @Override
  public long getValue() {
    CounterProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getValue());
  }

  @Override
  public void setValue(long value) {
    maybeInitBuilder();
    builder.setValue((value));
  }
  @Override
  public String getDisplayName() {
    CounterProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDisplayName()) {
      return null;
    }
    return (p.getDisplayName());
  }

  @Override
  public void setDisplayName(String displayName) {
    maybeInitBuilder();
    if (displayName == null) {
      builder.clearDisplayName();
      return;
    }
    builder.setDisplayName((displayName));
  }



}  
