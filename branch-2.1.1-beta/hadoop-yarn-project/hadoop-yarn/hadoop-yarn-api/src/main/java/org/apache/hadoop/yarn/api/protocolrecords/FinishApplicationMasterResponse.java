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

package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>
 * The response sent by the <code>ResourceManager</code> to a
 * <code>ApplicationMaster</code> on it's completion.
 * </p>
 * 
 * <p>
 * The response, includes:
 * <ul>
 * <li>A flag which indicates that the application has successfully unregistered
 * with the RM and the application can safely stop.</li>
 * </ul>
 * </p>
 * Note: The flag indicates whether the application has successfully
 * unregistered and is safe to stop. The application may stop after the flag is
 * true. If the application stops before the flag is true then the RM may retry
 * the application .
 * 
 * @see ApplicationMasterProtocol#finishApplicationMaster(FinishApplicationMasterRequest)
 */
@Public
@Stable
public abstract class FinishApplicationMasterResponse {

  @Private
  @Unstable
  public static FinishApplicationMasterResponse newInstance(
      boolean isRemovedFromRMStateStore) {
    FinishApplicationMasterResponse response =
        Records.newRecord(FinishApplicationMasterResponse.class);
    response.setIsUnregistered(isRemovedFromRMStateStore);
    return response;
  }

  /**
   * Get the flag which indicates that the application has successfully
   * unregistered with the RM and the application can safely stop.
   */
  @Public
  @Stable
  public abstract boolean getIsUnregistered();

  /**
   * Set the flag which indicates that the application has successfully
   * unregistered with the RM and the application can safely stop.
   */
  @Private
  @Unstable
  public abstract void setIsUnregistered(boolean isUnregistered);
}
