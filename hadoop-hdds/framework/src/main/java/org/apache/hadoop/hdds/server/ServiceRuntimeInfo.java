/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.server;

/**
 * Common runtime information for any service components.
 *
 * Note: it's intentional to not use MXBean or MBean as a suffix  of the name.
 *
 * Most of the services extends the ServiceRuntimeInfoImpl class and also
 * implements a specific MXBean interface which extends this interface.
 *
 * This inheritance from multiple path could confuse the jmx system and
 * some jmx properties could be disappeared.
 *
 * The solution is to always extend this interface and use the jmx naming
 * convention in the new interface..
 */
public interface ServiceRuntimeInfo {

  /**
   * Gets the version of Hadoop.
   *
   * @return the version
   */
  String getVersion();

  /**
   * Get the version of software running on the Namenode.
   *
   * @return a string representing the version
   */
  String getSoftwareVersion();

  /**
   * Get the compilation information which contains date, user and branch.
   *
   * @return the compilation information, as a JSON string.
   */
  String getCompileInfo();

  /**
   * Gets the NN start time in milliseconds.
   *
   * @return the NN start time in msec
   */
  long getStartedTimeInMillis();

}
