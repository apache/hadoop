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

package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProtoOrBuilder;

@Private
@Unstable
public class PriorityPBImpl extends Priority {
  PriorityProto proto = PriorityProto.getDefaultInstance();
  PriorityProto.Builder builder = null;
  boolean viaProto = false;
  
  public PriorityPBImpl() {
    builder = PriorityProto.newBuilder();
  }

  public PriorityPBImpl(PriorityProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public PriorityProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = PriorityProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public int getPriority() {
    PriorityProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getPriority());
  }

  @Override
  public void setPriority(int priority) {
    maybeInitBuilder();
    builder.setPriority((priority));
  }
  
  @Override
  public String toString() {
    return Integer.valueOf(getPriority()).toString();
  }

}  
