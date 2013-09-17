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


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillJobResponse;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillJobResponseProto;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;


    
public class KillJobResponsePBImpl extends ProtoBase<KillJobResponseProto> implements KillJobResponse {
  KillJobResponseProto proto = KillJobResponseProto.getDefaultInstance();
  KillJobResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public KillJobResponsePBImpl() {
    builder = KillJobResponseProto.newBuilder();
  }

  public KillJobResponsePBImpl(KillJobResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public KillJobResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = KillJobResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  



}  
