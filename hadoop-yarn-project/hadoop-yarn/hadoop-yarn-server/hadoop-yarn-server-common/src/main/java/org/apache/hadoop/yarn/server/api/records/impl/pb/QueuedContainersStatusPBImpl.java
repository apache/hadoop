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

import org.apache.hadoop.yarn.proto.YarnServerCommonProtos;
import org.apache.hadoop.yarn.server.api.records.QueuedContainersStatus;

/**
 * Protocol Buffer implementation of QueuedContainersStatus.
 */
public class QueuedContainersStatusPBImpl extends QueuedContainersStatus {

  private YarnServerCommonProtos.QueuedContainersStatusProto proto =
      YarnServerCommonProtos.QueuedContainersStatusProto.getDefaultInstance();
  private YarnServerCommonProtos.QueuedContainersStatusProto.Builder builder =
      null;
  private boolean viaProto = false;

  public QueuedContainersStatusPBImpl() {
    builder = YarnServerCommonProtos.QueuedContainersStatusProto.newBuilder();
  }

  public QueuedContainersStatusPBImpl(YarnServerCommonProtos
      .QueuedContainersStatusProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public YarnServerCommonProtos.QueuedContainersStatusProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder =
          YarnServerCommonProtos.QueuedContainersStatusProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public int getEstimatedQueueWaitTime() {
    YarnServerCommonProtos.QueuedContainersStatusProtoOrBuilder p =
        viaProto ? proto : builder;
    return p.getEstimatedQueueWaitTime();
  }

  @Override
  public void setEstimatedQueueWaitTime(int queueWaitTime) {
    maybeInitBuilder();
    builder.setEstimatedQueueWaitTime(queueWaitTime);
  }

  @Override
  public int getWaitQueueLength() {
    YarnServerCommonProtos.QueuedContainersStatusProtoOrBuilder p =
        viaProto ? proto : builder;
    return p.getWaitQueueLength();
  }

  @Override
  public void setWaitQueueLength(int waitQueueLength) {
    maybeInitBuilder();
    builder.setWaitQueueLength(waitQueueLength);
  }
}
