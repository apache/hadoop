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

package org.apache.hadoop.yarn.server.resourcemanager.recovery.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.util.Records;

/**
 * Contains all the state data that needs to be stored persistently 
 * for {@link AMRMTokenSecretManager}
 */
@Public
@Unstable
public abstract class AMRMTokenSecretManagerState {
  public static AMRMTokenSecretManagerState newInstance(
      MasterKey currentMasterKey, MasterKey nextMasterKey) {
    AMRMTokenSecretManagerState data =
        Records.newRecord(AMRMTokenSecretManagerState.class);
    data.setCurrentMasterKey(currentMasterKey);
    data.setNextMasterKey(nextMasterKey);
    return data;
  }

  public static AMRMTokenSecretManagerState newInstance(
      AMRMTokenSecretManagerState state) {
    AMRMTokenSecretManagerState data =
        Records.newRecord(AMRMTokenSecretManagerState.class);
    data.setCurrentMasterKey(state.getCurrentMasterKey());
    data.setNextMasterKey(state.getNextMasterKey());
    return data;
  }

  /**
   * {@link AMRMTokenSecretManager} current Master key
   */
  @Public
  @Unstable
  public abstract MasterKey getCurrentMasterKey();

  @Public
  @Unstable
  public abstract void setCurrentMasterKey(MasterKey currentMasterKey);

  /**
   * {@link AMRMTokenSecretManager} next Master key
   */
  @Public
  @Unstable
  public abstract MasterKey getNextMasterKey();

  @Public
  @Unstable
  public abstract void setNextMasterKey(MasterKey nextMasterKey);

  public abstract AMRMTokenSecretManagerStateProto getProto();
}
