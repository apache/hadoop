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

import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportsResponse;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskReportPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskReportProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskReportsResponseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskReportsResponseProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;


    
public class GetTaskReportsResponsePBImpl extends ProtoBase<GetTaskReportsResponseProto> implements GetTaskReportsResponse {
  GetTaskReportsResponseProto proto = GetTaskReportsResponseProto.getDefaultInstance();
  GetTaskReportsResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private List<TaskReport> taskReports = null;
  
  
  public GetTaskReportsResponsePBImpl() {
    builder = GetTaskReportsResponseProto.newBuilder();
  }

  public GetTaskReportsResponsePBImpl(GetTaskReportsResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public GetTaskReportsResponseProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.taskReports != null) {
      addTaskReportsToProto();
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
      builder = GetTaskReportsResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public List<TaskReport> getTaskReportList() {
    initTaskReports();
    return this.taskReports;
  }
  @Override
  public TaskReport getTaskReport(int index) {
    initTaskReports();
    return this.taskReports.get(index);
  }
  @Override
  public int getTaskReportCount() {
    initTaskReports();
    return this.taskReports.size();
  }
  
  private void initTaskReports() {
    if (this.taskReports != null) {
      return;
    }
    GetTaskReportsResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<TaskReportProto> list = p.getTaskReportsList();
    this.taskReports = new ArrayList<TaskReport>();

    for (TaskReportProto c : list) {
      this.taskReports.add(convertFromProtoFormat(c));
    }
  }
  
  @Override
  public void addAllTaskReports(final List<TaskReport> taskReports) {
    if (taskReports == null)
      return;
    initTaskReports();
    this.taskReports.addAll(taskReports);
  }
  
  private void addTaskReportsToProto() {
    maybeInitBuilder();
    builder.clearTaskReports();
    if (taskReports == null)
      return;
    Iterable<TaskReportProto> iterable = new Iterable<TaskReportProto>() {
      @Override
      public Iterator<TaskReportProto> iterator() {
        return new Iterator<TaskReportProto>() {

          Iterator<TaskReport> iter = taskReports.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public TaskReportProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllTaskReports(iterable);
  }
  @Override
  public void addTaskReport(TaskReport taskReports) {
    initTaskReports();
    this.taskReports.add(taskReports);
  }
  @Override
  public void removeTaskReport(int index) {
    initTaskReports();
    this.taskReports.remove(index);
  }
  @Override
  public void clearTaskReports() {
    initTaskReports();
    this.taskReports.clear();
  }

  private TaskReportPBImpl convertFromProtoFormat(TaskReportProto p) {
    return new TaskReportPBImpl(p);
  }

  private TaskReportProto convertToProtoFormat(TaskReport t) {
    return ((TaskReportPBImpl)t).getProto();
  }



}  
