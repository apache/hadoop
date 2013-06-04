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

package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptCompletionEventsResponse;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskAttemptCompletionEventPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptCompletionEventProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskAttemptCompletionEventsResponseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskAttemptCompletionEventsResponseProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;


    
public class GetTaskAttemptCompletionEventsResponsePBImpl extends ProtoBase<GetTaskAttemptCompletionEventsResponseProto> implements GetTaskAttemptCompletionEventsResponse {
  GetTaskAttemptCompletionEventsResponseProto proto = GetTaskAttemptCompletionEventsResponseProto.getDefaultInstance();
  GetTaskAttemptCompletionEventsResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private List<TaskAttemptCompletionEvent> completionEvents = null;
  
  
  public GetTaskAttemptCompletionEventsResponsePBImpl() {
    builder = GetTaskAttemptCompletionEventsResponseProto.newBuilder();
  }

  public GetTaskAttemptCompletionEventsResponsePBImpl(GetTaskAttemptCompletionEventsResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public GetTaskAttemptCompletionEventsResponseProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.completionEvents != null) {
      addCompletionEventsToProto();
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
      builder = GetTaskAttemptCompletionEventsResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public List<TaskAttemptCompletionEvent> getCompletionEventList() {
    initCompletionEvents();
    return this.completionEvents;
  }
  @Override
  public TaskAttemptCompletionEvent getCompletionEvent(int index) {
    initCompletionEvents();
    return this.completionEvents.get(index);
  }
  @Override
  public int getCompletionEventCount() {
    initCompletionEvents();
    return this.completionEvents.size();
  }
  
  private void initCompletionEvents() {
    if (this.completionEvents != null) {
      return;
    }
    GetTaskAttemptCompletionEventsResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<TaskAttemptCompletionEventProto> list = p.getCompletionEventsList();
    this.completionEvents = new ArrayList<TaskAttemptCompletionEvent>();

    for (TaskAttemptCompletionEventProto c : list) {
      this.completionEvents.add(convertFromProtoFormat(c));
    }
  }
  
  @Override
  public void addAllCompletionEvents(final List<TaskAttemptCompletionEvent> completionEvents) {
    if (completionEvents == null)
      return;
    initCompletionEvents();
    this.completionEvents.addAll(completionEvents);
  }
  
  private void addCompletionEventsToProto() {
    maybeInitBuilder();
    builder.clearCompletionEvents();
    if (completionEvents == null)
      return;
    Iterable<TaskAttemptCompletionEventProto> iterable = new Iterable<TaskAttemptCompletionEventProto>() {
      @Override
      public Iterator<TaskAttemptCompletionEventProto> iterator() {
        return new Iterator<TaskAttemptCompletionEventProto>() {

          Iterator<TaskAttemptCompletionEvent> iter = completionEvents.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public TaskAttemptCompletionEventProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllCompletionEvents(iterable);
  }
  @Override
  public void addCompletionEvent(TaskAttemptCompletionEvent completionEvents) {
    initCompletionEvents();
    this.completionEvents.add(completionEvents);
  }
  @Override
  public void removeCompletionEvent(int index) {
    initCompletionEvents();
    this.completionEvents.remove(index);
  }
  @Override
  public void clearCompletionEvents() {
    initCompletionEvents();
    this.completionEvents.clear();
  }

  private TaskAttemptCompletionEventPBImpl convertFromProtoFormat(TaskAttemptCompletionEventProto p) {
    return new TaskAttemptCompletionEventPBImpl(p);
  }

  private TaskAttemptCompletionEventProto convertToProtoFormat(TaskAttemptCompletionEvent t) {
    return ((TaskAttemptCompletionEventPBImpl)t).getProto();
  }



}  
