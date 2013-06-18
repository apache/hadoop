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
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;

/**
 * Command sent by the Resource Manager to the Application Master in the 
 * AllocateResponse 
 * @see AllocateResponse
 */
@Public
@Unstable
public enum AMCommand {
  /**
   * Sent by Resource Manager when it is out of sync with the AM and wants the 
   * AM get back in sync.
   */
  AM_RESYNC,
  
  /**
   * Sent by Resource Manager when it wants the AM to shutdown. Eg. when the 
   * node is going down for maintenance. The AM should save any state and 
   * prepare to be restarted at a later time. 
   */
  AM_SHUTDOWN
}
