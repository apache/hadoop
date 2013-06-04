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


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskAttemptResponse;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillTaskAttemptResponseProto;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;


    
public class KillTaskAttemptResponsePBImpl extends ProtoBase<KillTaskAttemptResponseProto> implements KillTaskAttemptResponse {
  KillTaskAttemptResponseProto proto = KillTaskAttemptResponseProto.getDefaultInstance();
  KillTaskAttemptResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public KillTaskAttemptResponsePBImpl() {
    builder = KillTaskAttemptResponseProto.newBuilder();
  }

  public KillTaskAttemptResponsePBImpl(KillTaskAttemptResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public KillTaskAttemptResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = KillTaskAttemptResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  



}  
