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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;

import com.google.common.base.Preconditions;

@Private
@Unstable
public class ApplicationIdPBImpl extends ApplicationId {
  ApplicationIdProto proto = null;
  ApplicationIdProto.Builder builder = null;

  public ApplicationIdPBImpl() {
    builder = ApplicationIdProto.newBuilder();
  }

  public ApplicationIdPBImpl(ApplicationIdProto proto) {
    this.proto = proto;
  }

  public ApplicationIdProto getProto() {
    return proto;
  }

  @Override
  public int getId() {
    Preconditions.checkNotNull(proto);
    return proto.getId();
  }

  @Override
  protected void setId(int id) {
    Preconditions.checkNotNull(builder);
    builder.setId(id);
  }
  @Override
  public long getClusterTimestamp() {
    Preconditions.checkNotNull(proto);
    return proto.getClusterTimestamp();
  }

  @Override
  protected void setClusterTimestamp(long clusterTimestamp) {
    Preconditions.checkNotNull(builder);
    builder.setClusterTimestamp((clusterTimestamp));
  }

  @Override
  protected void build() {
    proto = builder.build();
    builder = null;
  }
}