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

package org.apache.hadoop.yarn.applications.distributedshell;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Constants used in both Client and Application Master
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class DSConstants {

  /**
   * Environment key name pointing to the shell script's location
   */
  public static final String DISTRIBUTEDSHELLSCRIPTLOCATION = "DISTRIBUTEDSHELLSCRIPTLOCATION";

  /**
   * Environment key name denoting the file timestamp for the shell script. 
   * Used to validate the local resource. 
   */
  public static final String DISTRIBUTEDSHELLSCRIPTTIMESTAMP = "DISTRIBUTEDSHELLSCRIPTTIMESTAMP";

  /**
   * Environment key name denoting the file content length for the shell script. 
   * Used to validate the local resource. 
   */
  public static final String DISTRIBUTEDSHELLSCRIPTLEN = "DISTRIBUTEDSHELLSCRIPTLEN";

  /**
   * Environment key name denoting the timeline domain ID.
   */
  public static final String DISTRIBUTEDSHELLTIMELINEDOMAIN = "DISTRIBUTEDSHELLTIMELINEDOMAIN";
}
