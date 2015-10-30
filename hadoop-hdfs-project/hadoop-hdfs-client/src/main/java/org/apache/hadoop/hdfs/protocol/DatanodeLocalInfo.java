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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


/**
 * Locally available datanode information
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DatanodeLocalInfo {
  private final String softwareVersion;
  private final String configVersion;
  private final long uptime; // datanode uptime in seconds.

  public DatanodeLocalInfo(String softwareVersion,
      String configVersion, long uptime) {
    this.softwareVersion = softwareVersion;
    this.configVersion = configVersion;
    this.uptime = uptime;
  }

  /** get software version */
  public String getSoftwareVersion() {
    return this.softwareVersion;
  }

  /** get config version */
  public String getConfigVersion() {
    return this.configVersion;
  }

  /** get uptime */
  public long getUptime() {
    return this.uptime;
  }

  /** A formatted string for printing the status of the DataNode. */
  public String getDatanodeLocalReport() {
    return ("Uptime: " + getUptime())
        + ", Software version: " + getSoftwareVersion()
        + ", Config version: " + getConfigVersion();
  }
}
