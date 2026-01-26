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
import org.apache.hadoop.yarn.util.Records;

@Public
@Unstable
public abstract class DeregisterSubClusters {

  /**
   * Initialize DeregisterSubClusters.
   *
   * @param subClusterId subCluster Id.
   * @param deregisterState deregister state,
   *      SUCCESS means deregister is successful, Failed means deregister was unsuccessful.
   * @param lastHeartBeatTime last heartbeat time.
   * @param info offline information.
   * @param subClusterState subCluster State.
   * @return DeregisterSubClusters.
   */
  public static DeregisterSubClusters newInstance(String subClusterId,
      String deregisterState, String lastHeartBeatTime, String info,
      String subClusterState) {
    DeregisterSubClusters deregisterSubClusters =
        Records.newRecord(DeregisterSubClusters.class);
    deregisterSubClusters.setSubClusterId(subClusterId);
    deregisterSubClusters.setDeregisterState(deregisterState);
    deregisterSubClusters.setLastHeartBeatTime(lastHeartBeatTime);
    deregisterSubClusters.setInformation(info);
    deregisterSubClusters.setSubClusterState(subClusterState);
    return deregisterSubClusters;
  }

  @Public
  @Unstable
  public abstract String getSubClusterId();

  @Public
  @Unstable
  public abstract void setSubClusterId(String subClusterId);

  @Public
  @Unstable
  public abstract String getDeregisterState();

  @Public
  @Unstable
  public abstract void setDeregisterState(String deregisterState);

  @Public
  @Unstable
  public abstract String getLastHeartBeatTime();

  @Public
  @Unstable
  public abstract void setLastHeartBeatTime(String lastHeartBeatTime);

  @Public
  @Unstable
  public abstract String getInformation();

  @Public
  @Unstable
  public abstract void setInformation(String info);

  @Public
  @Unstable
  public abstract String getSubClusterState();

  @Public
  @Unstable
  public abstract void setSubClusterState(String subClusterState);
}
