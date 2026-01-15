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
package org.apache.hadoop.hdfs.net;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;

/**
 * Interface for mapping DataNode to weight for block placement.
 */
public interface DNSToWeightMapping extends DataNodeWeightSupplier, Configurable {

  /**
   * Resolves weight for the given IP-address/DNS-name.
   * <p>
   * NOTE: which parameter is used to resolve depends on the implementation.
   *
   * @param ipAddress IP-address
   * @param hostName  DNS-name
   * @return resolved weight
   */
  int resolve(String ipAddress, String hostName);

  /**
   * Resolves weight for the given IP-address.
   *
   * @param ipAddress IP-address
   * @return resolved weight
   */
  default int resolve(String ipAddress) {
    return resolve(ipAddress, null);
  }

  @Override
  default int resolve(DatanodeDescriptor dn) {
    return resolve(dn.getIpAddr(), dn.getHostName());
  }

  /**
   * Reload the weight mapping.
   */
  void reload();

}
