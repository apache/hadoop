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

package org.apache.hadoop.yarn.conf;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@InterfaceAudience.Private
public class HAUtil {
  private static Log LOG = LogFactory.getLog(HAUtil.class);

  public static final List<String> RPC_ADDRESS_CONF_KEYS =
      Collections.unmodifiableList(Arrays.asList(
          YarnConfiguration.RM_ADDRESS,
          YarnConfiguration.RM_SCHEDULER_ADDRESS,
          YarnConfiguration.RM_ADMIN_ADDRESS,
          YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
          YarnConfiguration.RM_WEBAPP_ADDRESS));

  private HAUtil() { /* Hidden constructor */ }

  private static void throwBadConfigurationException(String msg) {
    throw new YarnRuntimeException("Invalid configuration! " + msg);
  }

  /**
   * Returns true if Resource Manager HA is configured.
   *
   * @param conf Configuration
   * @return true if HA is configured in the configuration; else false.
   */
  public static boolean isHAEnabled(Configuration conf) {
    return conf.getBoolean(YarnConfiguration.RM_HA_ENABLED,
        YarnConfiguration.DEFAULT_RM_HA_ENABLED);
  }

  public static Collection<String> getRMHAIds(Configuration conf) {
    return conf.getTrimmedStringCollection(YarnConfiguration.RM_HA_IDS);
  }

  /**
   * @param conf Configuration
   * @return RM Id on success
   * @throws YarnRuntimeException for configurations without a node id
   */
  @VisibleForTesting
  public static String getRMHAId(Configuration conf) {
    String rmId = conf.get(YarnConfiguration.RM_HA_ID);
    if (rmId == null) {
      throwBadConfigurationException(YarnConfiguration.RM_HA_ID +
          " needs to be set in a HA configuration");
    }
    return rmId;
  }

  private static String getConfValueForRMInstance(String prefix,
                                                  Configuration conf) {
    String confKey = addSuffix(prefix, getRMHAId(conf));
    String retVal = conf.get(confKey);
    if (LOG.isTraceEnabled()) {
      LOG.trace("getConfValueForRMInstance: prefix = " + prefix +
          "; confKey being looked up = " + confKey +
          "; value being set to = " + retVal);
    }
    return retVal;
  }

  static String getConfValueForRMInstance(String prefix, String defaultValue,
                                          Configuration conf) {
    String value = getConfValueForRMInstance(prefix, conf);
    return (value == null) ? defaultValue : value;
  }

  private static void setConfValue(String prefix, Configuration conf) {
    conf.set(prefix, getConfValueForRMInstance(prefix, conf));
  }

  public static void setAllRpcAddresses(Configuration conf) {
    for (String confKey : RPC_ADDRESS_CONF_KEYS) {
      setConfValue(confKey, conf);
    }
  }

  /** Add non empty and non null suffix to a key */
  @VisibleForTesting
  public static String addSuffix(String key, String suffix) {
    if (suffix == null || suffix.isEmpty()) {
      return key;
    }
    if (suffix.startsWith(".")) {
      throw new IllegalArgumentException("suffix '" + suffix + "' should not " +
          "already have '.' prepended.");
    }
    return key + "." + suffix;
  }
}
