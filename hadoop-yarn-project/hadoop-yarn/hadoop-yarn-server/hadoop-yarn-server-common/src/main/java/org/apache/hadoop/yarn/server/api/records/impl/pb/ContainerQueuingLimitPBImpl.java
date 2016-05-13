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

package org.apache.hadoop.yarn.server.api.records.impl.pb;

import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.ContainerQueuingLimitProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.ContainerQueuingLimitProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;

/**
 * Implementation of ContainerQueuingLimit interface.
 */
public class ContainerQueuingLimitPBImpl extends ContainerQueuingLimit {

  private ContainerQueuingLimitProto proto =
      ContainerQueuingLimitProto.getDefaultInstance();
  private ContainerQueuingLimitProto.Builder builder = null;
  private boolean viaProto = false;

  public ContainerQueuingLimitPBImpl() {
    builder = ContainerQueuingLimitProto.newBuilder();
  }

  public ContainerQueuingLimitPBImpl(ContainerQueuingLimitProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  public ContainerQueuingLimitProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return  proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ContainerQueuingLimitProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public int getMaxQueueWaitTimeInMs() {
    ContainerQueuingLimitProtoOrBuilder p = viaProto ? proto : builder;
    return p.getMaxQueueWaitTimeInMs();
  }

  @Override
  public void setMaxQueueWaitTimeInMs(int waitTime) {
    maybeInitBuilder();
    builder.setMaxQueueWaitTimeInMs(waitTime);
  }

  @Override
  public int getMaxQueueLength() {
    ContainerQueuingLimitProtoOrBuilder p = viaProto ? proto : builder;
    return p.getMaxQueueLength();
  }

  @Override
  public void setMaxQueueLength(int queueLength) {
    maybeInitBuilder();
    builder.setMaxQueueLength(queueLength);
  }
}
