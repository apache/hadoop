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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceOptionProto;

import com.google.common.base.Preconditions;

public class ResourceOptionPBImpl extends ResourceOption {

  ResourceOptionProto proto = null;
  ResourceOptionProto.Builder builder = null;
  private Resource resource = null;

  public ResourceOptionPBImpl() {
    builder = ResourceOptionProto.newBuilder();
  }

  public ResourceOptionPBImpl(ResourceOptionProto proto) {
    this.proto = proto;
    this.resource = convertFromProtoFormat(proto.getResource());
  }
  
  public ResourceOptionProto getProto() {
    return proto;
  }
  
  @Override
  public Resource getResource() {
    return this.resource;
  }

  @Override
  protected void setResource(Resource resource) {
    if (resource != null) {
      Preconditions.checkNotNull(builder);
      builder.setResource(convertToProtoFormat(resource));
    }
    this.resource = resource;
  }

  @Override
  public int getOverCommitTimeout() {
    Preconditions.checkNotNull(proto);
    return proto.getOverCommitTimeout();
  }

  @Override
  protected void setOverCommitTimeout(int overCommitTimeout) {
    Preconditions.checkNotNull(builder);
    builder.setOverCommitTimeout(overCommitTimeout);
  }
  
  private ResourceProto convertToProtoFormat(
      Resource resource) {
    return ((ResourcePBImpl)resource).getProto();
  }
  
  private ResourcePBImpl convertFromProtoFormat(
      ResourceProto p) {
    return new ResourcePBImpl(p);
  }
  
  @Override
  protected void build() {
    proto = builder.build();
    builder = null;
  }

}
