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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;

/**
 * Command sent by the Resource Manager to the Application Master in the 
 * AllocateResponse 
 * @see AllocateResponse
 */
@Public
@Unstable
public enum AMCommand {

  /**
   * @deprecated Sent by Resource Manager when it is out of sync with the AM and
   *             wants the AM get back in sync.
   * 
   *             Note: Instead of sending this command,
   *             {@link ApplicationMasterNotRegisteredException} will be thrown
   *             when ApplicationMaster is out of sync with ResourceManager and
   *             ApplicationMaster is expected to re-register with RM by calling
   *             {@link ApplicationMasterProtocol#registerApplicationMaster(RegisterApplicationMasterRequest)}
   */
  AM_RESYNC,

  /**
   * @deprecated Sent by Resource Manager when it wants the AM to shutdown.
   *             Note: This command was earlier sent by ResourceManager to
   *             instruct AM to shutdown if RM had restarted. Now
   *             {@link ApplicationAttemptNotFoundException} will be thrown in case
   *             that RM has restarted and AM is supposed to handle this
   *             exception by shutting down itself.
   */
  AM_SHUTDOWN
}
