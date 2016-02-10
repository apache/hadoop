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
package org.apache.hadoop.yarn.client.util;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * This class is a container for utility methods that are useful when creating
 * YARN clients.
 */
public abstract class YarnClientUtils {
  /**
   * Look up and return the resource manager's principal. This method
   * automatically does the <code>_HOST</code> replacement in the principal and
   * correctly handles HA resource manager configurations.
   *
   * @param conf the {@link Configuration} file from which to read the
   * principal
   * @return the resource manager's principal string or null if the
   * {@link YarnConfiguration#RM_PRINCIPAL} property is not set in the
   * {@code conf} parameter
   * @throws IOException thrown if there's an error replacing the host name
   */
  public static String getRmPrincipal(Configuration conf) throws IOException {
    String principal = conf.get(YarnConfiguration.RM_PRINCIPAL);
    String prepared = null;

    if (principal != null) {
      prepared = getRmPrincipal(principal, conf);
    }

    return prepared;
  }

  /**
   * Perform the <code>_HOST</code> replacement in the {@code principal},
   * Returning the result. Correctly handles HA resource manager configurations.
   *
   * @param rmPrincipal the principal string to prepare
   * @param conf the configuration
   * @return the prepared principal string
   * @throws IOException thrown if there's an error replacing the host name
   */
  public static String getRmPrincipal(String rmPrincipal, Configuration conf)
      throws IOException {
    if (rmPrincipal == null) {
      throw new IllegalArgumentException("RM principal string is null");
    }

    if (HAUtil.isHAEnabled(conf)) {
      conf = getYarnConfWithRmHaId(conf);
    }

    String hostname = conf.getSocketAddr(
        YarnConfiguration.RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_PORT).getHostName();

    return SecurityUtil.getServerPrincipal(rmPrincipal, hostname);
  }

  /**
   * Returns a {@link YarnConfiguration} built from the {@code conf} parameter
   * that is guaranteed to have the {@link YarnConfiguration#RM_HA_ID}
   * property set.
   *
   * @param conf the base configuration
   * @return a {@link YarnConfiguration} built from the base
   * {@link Configuration}
   * @throws IOException thrown if the {@code conf} parameter contains
   * inconsistent properties
   */
  @VisibleForTesting
  static YarnConfiguration getYarnConfWithRmHaId(Configuration conf)
      throws IOException {
    YarnConfiguration yarnConf = new YarnConfiguration(conf);

    if (yarnConf.get(YarnConfiguration.RM_HA_ID) == null) {
      // If RM_HA_ID is not configured, use the first of RM_HA_IDS.
      // Any valid RM HA ID should work.
      String[] rmIds = yarnConf.getStrings(YarnConfiguration.RM_HA_IDS);

      if ((rmIds != null) && (rmIds.length > 0)) {
        yarnConf.set(YarnConfiguration.RM_HA_ID, rmIds[0]);
      } else {
        throw new IOException("RM_HA_IDS property is not set for HA resource "
            + "manager");
      }
    }

    return yarnConf;
  }
}
