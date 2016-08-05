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
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.util.Records;

/**
 * The response sent by the <code>ResourceManager</code> to the client aborting
 * a submitted application.
 * <p>
 * The response, includes:
 * <ul>
 *   <li>
 *     A flag which indicates that the process of killing the application is
 *     completed or not.
 *   </li>
 * </ul>
 * Note: user is recommended to wait until this flag becomes true, otherwise if
 * the <code>ResourceManager</code> crashes before the process of killing the
 * application is completed, the <code>ResourceManager</code> may retry this
 * application on recovery.
 * 
 * @see ApplicationClientProtocol#forceKillApplication(KillApplicationRequest)
 */
@Public
@Stable
public abstract class KillApplicationResponse {
  @Private
  @Unstable
  public static KillApplicationResponse newInstance(boolean isKillCompleted) {
    KillApplicationResponse response =
        Records.newRecord(KillApplicationResponse.class);
    response.setIsKillCompleted(isKillCompleted);
    return response;
  }

  /**
   * Get the flag which indicates that the process of killing application is completed or not.
   * @return true if the process of killing application has completed,
   *         false otherwise
   */
  @Public
  @Stable
  public abstract boolean getIsKillCompleted();

  /**
   * Set the flag which indicates that the process of killing application is completed or not.
   */
  @Private
  @Unstable
  public abstract void setIsKillCompleted(boolean isKillCompleted);
}
