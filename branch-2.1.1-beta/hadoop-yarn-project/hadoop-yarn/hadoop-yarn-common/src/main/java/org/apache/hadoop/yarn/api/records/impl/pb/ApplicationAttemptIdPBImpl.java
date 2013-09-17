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
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;

import com.google.common.base.Preconditions;

@Private
@Unstable
public class ApplicationAttemptIdPBImpl extends ApplicationAttemptId {
  ApplicationAttemptIdProto proto = null;
  ApplicationAttemptIdProto.Builder builder = null;
  private ApplicationId applicationId = null;

  public ApplicationAttemptIdPBImpl() {
    builder = ApplicationAttemptIdProto.newBuilder();
  }

  public ApplicationAttemptIdPBImpl(ApplicationAttemptIdProto proto) {
    this.proto = proto;
    this.applicationId = convertFromProtoFormat(proto.getApplicationId());
  }
  
  public ApplicationAttemptIdProto getProto() {
    return proto;
  }

  @Override
  public int getAttemptId() {
    Preconditions.checkNotNull(proto);
    return proto.getAttemptId();
  }

  @Override
  protected void setAttemptId(int attemptId) {
    Preconditions.checkNotNull(builder);
    builder.setAttemptId(attemptId);
  }

  @Override
  public ApplicationId getApplicationId() {
    return this.applicationId;
  }

  @Override
  public void setApplicationId(ApplicationId appId) {
    if (appId != null) {
      Preconditions.checkNotNull(builder);
      builder.setApplicationId(convertToProtoFormat(appId));
    }
    this.applicationId = appId;
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl)t).getProto();
  }

  @Override
  protected void build() {
    proto = builder.build();
    builder = null;
  }
}  
