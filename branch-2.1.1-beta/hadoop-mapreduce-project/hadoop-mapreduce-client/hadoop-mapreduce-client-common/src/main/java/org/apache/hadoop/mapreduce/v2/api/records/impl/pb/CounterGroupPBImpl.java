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


import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.v2.api.records.Counter;
import org.apache.hadoop.mapreduce.v2.api.records.CounterGroup;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.CounterGroupProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.CounterGroupProtoOrBuilder;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.CounterProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.StringCounterMapProto;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;


    
public class CounterGroupPBImpl extends ProtoBase<CounterGroupProto> implements CounterGroup {
  CounterGroupProto proto = CounterGroupProto.getDefaultInstance();
  CounterGroupProto.Builder builder = null;
  boolean viaProto = false;
  
  private Map<String, Counter> counters = null;
  
  
  public CounterGroupPBImpl() {
    builder = CounterGroupProto.newBuilder();
  }

  public CounterGroupPBImpl(CounterGroupProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public CounterGroupProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.counters != null) {
      addContersToProto();
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = CounterGroupProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public String getName() {
    CounterGroupProtoOrBuilder p = viaProto ? proto : builder;
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
  public String getDisplayName() {
    CounterGroupProtoOrBuilder p = viaProto ? proto : builder;
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
  @Override
  public Map<String, Counter> getAllCounters() {
    initCounters();
    return this.counters;
  }
  @Override
  public Counter getCounter(String key) {
    initCounters();
    return this.counters.get(key);
  }
  
  private void initCounters() {
    if (this.counters != null) {
      return;
    }
    CounterGroupProtoOrBuilder p = viaProto ? proto : builder;
    List<StringCounterMapProto> list = p.getCountersList();
    this.counters = new HashMap<String, Counter>();

    for (StringCounterMapProto c : list) {
      this.counters.put(c.getKey(), convertFromProtoFormat(c.getValue()));
    }
  }
  
  @Override
  public void addAllCounters(final Map<String, Counter> counters) {
    if (counters == null)
      return;
    initCounters();
    this.counters.putAll(counters);
  }
  
  private void addContersToProto() {
    maybeInitBuilder();
    builder.clearCounters();
    if (counters == null)
      return;
    Iterable<StringCounterMapProto> iterable = new Iterable<StringCounterMapProto>() {
      
      @Override
      public Iterator<StringCounterMapProto> iterator() {
        return new Iterator<StringCounterMapProto>() {
          
          Iterator<String> keyIter = counters.keySet().iterator();
          
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
          
          @Override
          public StringCounterMapProto next() {
            String key = keyIter.next();
            return StringCounterMapProto.newBuilder().setKey(key).setValue(convertToProtoFormat(counters.get(key))).build();
          }
          
          @Override
          public boolean hasNext() {
            return keyIter.hasNext();
          }
        };
      }
    };
    builder.addAllCounters(iterable);
  }
  @Override
  public void setCounter(String key, Counter val) {
    initCounters();
    this.counters.put(key, val);
  }
  @Override
  public void removeCounter(String key) {
    initCounters();
    this.counters.remove(key);
  }
  @Override
  public void clearCounters() {
    initCounters();
    this.counters.clear();
  }

  private CounterPBImpl convertFromProtoFormat(CounterProto p) {
    return new CounterPBImpl(p);
  }

  private CounterProto convertToProtoFormat(Counter t) {
    return ((CounterPBImpl)t).getProto();
  }
}  
