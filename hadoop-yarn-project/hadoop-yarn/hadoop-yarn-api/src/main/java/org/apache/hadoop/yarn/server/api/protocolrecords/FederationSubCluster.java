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
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * Federation SubClusters.
 */
@Private
@Unstable
public abstract class FederationSubCluster {

  @Private
  @Unstable
  public static FederationSubCluster newInstance(String subClusterId, String state,
      String lastHeartBeatTime) {
    FederationSubCluster subCluster = Records.newRecord(FederationSubCluster.class);
    subCluster.setSubClusterId(subClusterId);
    subCluster.setSubClusterState(state);
    subCluster.setLastHeartBeatTime(lastHeartBeatTime);
    return subCluster;
  }

  @Public
  @Unstable
  public abstract String getSubClusterId();

  @Private
  @Unstable
  public abstract void setSubClusterId(String subClusterId);

  @Public
  @Unstable
  public abstract String getSubClusterState();

  @Public
  @Unstable
  public abstract void setSubClusterState(String state);

  @Public
  @Unstable
  public abstract String getLastHeartBeatTime();

  @Public
  @Unstable
  public abstract void setLastHeartBeatTime(String lastHeartBeatTime);
}
