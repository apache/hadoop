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
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.CounterGroupProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.CountersProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.CountersProtoOrBuilder;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.StringCounterGroupMapProto;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;


    
public class CountersPBImpl extends ProtoBase<CountersProto> implements Counters {
  CountersProto proto = CountersProto.getDefaultInstance();
  CountersProto.Builder builder = null;
  boolean viaProto = false;

  private Map<String, CounterGroup> counterGroups = null;

  
  public CountersPBImpl() {
    builder = CountersProto.newBuilder();
  }

  public CountersPBImpl(CountersProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public CountersProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.counterGroups != null) {
      addCounterGroupsToProto();
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
      builder = CountersProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public Map<String, CounterGroup> getAllCounterGroups() {
    initCounterGroups();
    return this.counterGroups;
  }
  @Override
  public CounterGroup getCounterGroup(String key) {
    initCounterGroups();
    return this.counterGroups.get(key);
  }
  @Override
  public Counter getCounter(Enum<?> key) {
    CounterGroup group = getCounterGroup(key.getDeclaringClass().getName());
    return group == null ? null : group.getCounter(key.name());
  }

  @Override
  public void incrCounter(Enum<?> key, long amount) {
    String groupName = key.getDeclaringClass().getName();
    if (getCounterGroup(groupName) == null) {
      CounterGroup cGrp = new CounterGroupPBImpl();
      cGrp.setName(groupName);
      cGrp.setDisplayName(groupName);
      setCounterGroup(groupName, cGrp);
    }
    if (getCounterGroup(groupName).getCounter(key.name()) == null) {
      Counter c = new CounterPBImpl();
      c.setName(key.name());
      c.setDisplayName(key.name());
      c.setValue(0l);
      getCounterGroup(groupName).setCounter(key.name(), c);
    }
    Counter counter = getCounterGroup(groupName).getCounter(key.name());
    counter.setValue(counter.getValue() + amount);
  }
 
  private void initCounterGroups() {
    if (this.counterGroups != null) {
      return;
    }
    CountersProtoOrBuilder p = viaProto ? proto : builder;
    List<StringCounterGroupMapProto> list = p.getCounterGroupsList();
    this.counterGroups = new HashMap<String, CounterGroup>();

    for (StringCounterGroupMapProto c : list) {
      this.counterGroups.put(c.getKey(), convertFromProtoFormat(c.getValue()));
    }
  }
  
  @Override
  public void addAllCounterGroups(final Map<String, CounterGroup> counterGroups) {
    if (counterGroups == null)
      return;
    initCounterGroups();
    this.counterGroups.putAll(counterGroups);
  }
  
  private void addCounterGroupsToProto() {
    maybeInitBuilder();
    builder.clearCounterGroups();
    if (counterGroups == null)
      return;
    Iterable<StringCounterGroupMapProto> iterable = new Iterable<StringCounterGroupMapProto>() {
      
      @Override
      public Iterator<StringCounterGroupMapProto> iterator() {
        return new Iterator<StringCounterGroupMapProto>() {
          
          Iterator<String> keyIter = counterGroups.keySet().iterator();
          
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
          
          @Override
          public StringCounterGroupMapProto next() {
            String key = keyIter.next();
            return StringCounterGroupMapProto.newBuilder().setKey(key).setValue(convertToProtoFormat(counterGroups.get(key))).build();
          }
          
          @Override
          public boolean hasNext() {
            return keyIter.hasNext();
          }
        };
      }
    };
    builder.addAllCounterGroups(iterable);
  }
  @Override
  public void setCounterGroup(String key, CounterGroup val) {
    initCounterGroups();
    this.counterGroups.put(key, val);
  }
  @Override
  public void removeCounterGroup(String key) {
    initCounterGroups();
    this.counterGroups.remove(key);
  }
  @Override
  public void clearCounterGroups() {
    initCounterGroups();
    this.counterGroups.clear();
  }

  private CounterGroupPBImpl convertFromProtoFormat(CounterGroupProto p) {
    return new CounterGroupPBImpl(p);
  }

  private CounterGroupProto convertToProtoFormat(CounterGroup t) {
    return ((CounterGroupPBImpl)t).getProto();
  }
}  
