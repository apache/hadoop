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


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.FailTaskAttemptResponse;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.FailTaskAttemptResponseProto;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;


    
public class FailTaskAttemptResponsePBImpl extends ProtoBase<FailTaskAttemptResponseProto> implements FailTaskAttemptResponse {
  FailTaskAttemptResponseProto proto = FailTaskAttemptResponseProto.getDefaultInstance();
  FailTaskAttemptResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public FailTaskAttemptResponsePBImpl() {
    builder = FailTaskAttemptResponseProto.newBuilder();
  }

  public FailTaskAttemptResponsePBImpl(FailTaskAttemptResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public FailTaskAttemptResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = FailTaskAttemptResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

}  
