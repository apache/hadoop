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

package org.apache.hadoop.mapreduce.util;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

@Private
@Unstable
public class HostUtil {

  /**
   * Construct the taskLogUrl
   * @param taskTrackerHostName
   * @param httpPort
   * @param taskAttemptID
   * @return the taskLogUrl
   */
  public static String getTaskLogUrl(String scheme, String taskTrackerHostName,
    String httpPort, String taskAttemptID) {
    return (scheme + taskTrackerHostName + ":" +
        httpPort + "/tasklog?attemptid=" + taskAttemptID);
  }

  public static String convertTrackerNameToHostName(String trackerName) {
    // Ugly!
    // Convert the trackerName to its host name
    int indexOfColon = trackerName.indexOf(":");
    String trackerHostName = (indexOfColon == -1) ? 
      trackerName : 
      trackerName.substring(0, indexOfColon);
    return trackerHostName.substring("tracker_".length());
  }

}
