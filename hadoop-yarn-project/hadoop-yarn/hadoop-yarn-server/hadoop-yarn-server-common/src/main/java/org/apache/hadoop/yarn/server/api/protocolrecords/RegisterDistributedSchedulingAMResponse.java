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
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

/**
 * This is the response to registering an Application Master when distributed
 * scheduling is enabled. Apart from the
 * {@link RegisterApplicationMasterResponse}, it includes various parameters
 * to be used during distributed scheduling, such as the min and max resources
 * that can be requested by containers.
 */
@Public
@Unstable
public abstract class RegisterDistributedSchedulingAMResponse {

  @Public
  @Unstable
  public static RegisterDistributedSchedulingAMResponse newInstance
      (RegisterApplicationMasterResponse regAMResp) {
    RegisterDistributedSchedulingAMResponse response =
        Records.newRecord(RegisterDistributedSchedulingAMResponse.class);
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
  public abstract void setMinContainerResource(Resource minResource);

  @Public
  @Unstable
  public abstract Resource getMinContainerResource();

  @Public
  @Unstable
  public abstract void setMaxContainerResource(Resource maxResource);

  @Public
  @Unstable
  public abstract Resource getMaxContainerResource();

  @Public
  @Unstable
  public abstract void setIncrContainerResource(Resource maxResource);

  @Public
  @Unstable
  public abstract Resource getIncrContainerResource();

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
  public abstract void setNodesForScheduling(
      List<RemoteNode> nodesForScheduling);

  @Public
  @Unstable
  public abstract List<RemoteNode> getNodesForScheduling();

}
