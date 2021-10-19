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
package org.apache.hadoop.yarn.util.csi;

import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.net.InetSocketAddress;

/**
 * Utility class for CSI in the API level.
 */
public final class CsiConfigUtils {

  private CsiConfigUtils() {
    // Hide constructor for utility class.
  }

  public static String[] getCsiDriverNames(Configuration conf) {
    return conf.getStrings(YarnConfiguration.NM_CSI_DRIVER_NAMES);
  }

  public static String getCsiDriverEndpoint(String driverName,
      Configuration conf) throws YarnException {
    String driverEndpointProperty = YarnConfiguration.NM_CSI_DRIVER_PREFIX
        + driverName + YarnConfiguration.NM_CSI_DRIVER_ENDPOINT_SUFFIX;
    String driverEndpoint = conf.get(driverEndpointProperty);
    if (Strings.isNullOrEmpty(driverEndpoint)) {
      throw new YarnException("CSI driver's endpoint is not specified or"
          + " invalid, property "+ driverEndpointProperty + " is not defined");
    }
    return driverEndpoint;
  }

  /**
   * Resolve the CSI adaptor address for a CSI driver from configuration.
   * Expected configuration property name is
   * yarn.nodemanager.csi-driver-adaptor.${driverName}.address.
   * @param driverName
   * @param conf
   * @return adaptor service address
   * @throws YarnException
   */
  public static InetSocketAddress getCsiAdaptorAddressForDriver(
      String driverName, Configuration conf) throws YarnException {
    String configName = YarnConfiguration.NM_CSI_ADAPTOR_PREFIX
        + driverName + YarnConfiguration.NM_CSI_ADAPTOR_ADDRESS_SUFFIX;
    String errorMessage = "Failed to load CSI adaptor address for driver "
        + driverName + ", configuration property " + configName
        + " is not defined or invalid.";
    try {
      InetSocketAddress address = conf
          .getSocketAddr(configName, null, -1);
      if (address == null) {
        throw new YarnException(errorMessage);
      }
      return address;
    } catch (IllegalArgumentException e) {
      throw new YarnException(errorMessage);
    }
  }
}
