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
package org.apache.hadoop.hdfs.server.protocol;

/**
 * DataNode bandwidth command instructs each datanode to change its value for
 * the max amount of network bandwidth.
 */
public class DataNodeBandwidthCommand extends DatanodeCommand {
  private final static long DBC_DEFAULTBANDWIDTH = 0L;

  private final long bandwidth;
  private final String type;

  /**
   * DataNode Bandwidth Command constructor. Sets bandwidth to 0.
   */
  DataNodeBandwidthCommand() {
    this(DBC_DEFAULTBANDWIDTH, null);
  }

  /**
   * DataNode Bandwidth Command constructor.
   *
   * @param bandwidth Bandwidth in bytes per second.
   */
  public DataNodeBandwidthCommand(long bandwidth, String type) {
    super(DatanodeProtocol.DNA_DATANODEBANDWIDTHUPDATE);
    this.bandwidth = bandwidth;
    this.type = type;
  }

  /**
   * Get current value of the max bandwidth in bytes per second.
   *
   * @return bandwidth DataNode bandwidth in bytes per second for this datanode.
   */
  public long getDataNodeBandwidthValue() {
    return this.bandwidth;
  }

  /**
   * Get current value of bandwidth type.
   *
   * @return datanode bandwidth type.
   */
  public String getDataNodeBandwidthType() {
    return this.type;
  }
}
