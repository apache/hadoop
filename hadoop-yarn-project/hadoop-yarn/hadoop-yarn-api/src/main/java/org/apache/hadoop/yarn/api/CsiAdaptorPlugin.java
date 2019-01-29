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
package org.apache.hadoop.yarn.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * csi-adaptor is a plugin, user can provide customized implementation
 * according to this interface. NM will init and load this into a NM aux
 * service, and it can run multiple csi-adaptor servers.
 *
 * User needs to implement all the methods defined in
 * {@link CsiAdaptorProtocol}, and plus the methods in this interface.
 */
public interface CsiAdaptorPlugin extends CsiAdaptorProtocol {

  /**
   * A csi-adaptor implementation can init its state within this function.
   * Configuration is available so the implementation can retrieve some
   * customized configuration from yarn-site.xml.
   * @param driverName the name of the csi-driver.
   * @param conf configuration.
   * @throws YarnException
   */
  void init(String driverName, Configuration conf) throws YarnException;

  /**
   * Returns the driver name of the csi-driver this adaptor works with.
   * The name should be consistent on all the places being used, ideally
   * it should come from the value when init is done.
   * @return the name of the csi-driver that this adaptor works with.
   */
  String getDriverName();
}
