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
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

/**
 * This is the response of the Resource Manager to the
 * {@link DistributedSchedulingAllocateRequest}, when distributed scheduling is
 * enabled. It includes the {@link AllocateResponse} for the GUARANTEED
 * containers allocated by the Resource Manager. Moreover, it includes a list
 * with the nodes that can be used by the Distributed Scheduler when allocating
 * containers.
 */
@Public
@Unstable
public abstract class DistributedSchedulingAllocateResponse {

  @Public
  @Unstable
  public static DistributedSchedulingAllocateResponse newInstance(
      AllocateResponse allResp) {
    DistributedSchedulingAllocateResponse response =
        Records.newRecord(DistributedSchedulingAllocateResponse.class);
    response.setAllocateResponse(allResp);
    return  response;
  }

  @Public
  @Unstable
  public abstract void setAllocateResponse(AllocateResponse response);

  @Public
  @Unstable
  public abstract AllocateResponse getAllocateResponse();

  @Public
  @Unstable
  public abstract void setNodesForScheduling(
      List<RemoteNode> nodesForScheduling);

  @Public
  @Unstable
  public abstract List<RemoteNode> getNodesForScheduling();
}
