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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords
    .RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

@Public
@Unstable
public abstract class DistSchedRegisterResponse {

  @Public
  @Unstable
  public static DistSchedRegisterResponse newInstance
      (RegisterApplicationMasterResponse regAMResp) {
    DistSchedRegisterResponse response =
        Records.newRecord(DistSchedRegisterResponse.class);
    response.setRegisterResponse(regAMResp);
    return response;
  }

  @Public
  @Unstable
  public abstract void setRegisterResponse(
      RegisterApplicationMasterResponse resp);

  @Public
  @Unstable
  public abstract RegisterApplicationMasterResponse getRegisterResponse();

  @Public
  @Unstable
  public abstract void setMinAllocatableCapabilty(Resource minResource);

  @Public
  @Unstable
  public abstract Resource getMinAllocatableCapabilty();

  @Public
  @Unstable
  public abstract void setMaxAllocatableCapabilty(Resource maxResource);

  @Public
  @Unstable
  public abstract Resource getMaxAllocatableCapabilty();

  @Public
  @Unstable
  public abstract void setIncrAllocatableCapabilty(Resource maxResource);

  @Public
  @Unstable
  public abstract Resource getIncrAllocatableCapabilty();

  @Public
  @Unstable
  public abstract void setContainerTokenExpiryInterval(int interval);

  @Public
  @Unstable
  public abstract int getContainerTokenExpiryInterval();

  @Public
  @Unstable
  public abstract void setContainerIdStart(long containerIdStart);

  @Public
  @Unstable
  public abstract long getContainerIdStart();

  @Public
  @Unstable
  public abstract void setNodesForScheduling(List<NodeId> nodesForScheduling);

  @Public
  @Unstable
  public abstract List<NodeId> getNodesForScheduling();

}
