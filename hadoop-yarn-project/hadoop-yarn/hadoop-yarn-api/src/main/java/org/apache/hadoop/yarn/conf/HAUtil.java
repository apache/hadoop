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
import org.apache.hadoop.util.StringUtils;
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
          YarnConfiguration.RM_WEBAPP_ADDRESS,
          YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS,
          // TODO Remove after YARN-1318
          YarnConfiguration.RM_HA_ADMIN_ADDRESS));

  public static final String BAD_CONFIG_MESSAGE_PREFIX =
    "Invalid configuration! ";

  private HAUtil() { /* Hidden constructor */ }

  private static void throwBadConfigurationException(String msg) {
    throw new YarnRuntimeException(BAD_CONFIG_MESSAGE_PREFIX + msg);
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

  /**
   * Verify configuration for Resource Manager HA.
   * @param conf Configuration
   * @throws YarnRuntimeException
   */
  public static void verifyAndSetConfiguration(Configuration conf)
    throws YarnRuntimeException {
    verifyAndSetRMHAIds(conf);
    verifyAndSetRMHAId(conf);
    verifyAndSetAllRpcAddresses(conf);
  }


  private static void verifyAndSetRMHAIds(Configuration conf) {
    Collection<String> ids =
      conf.getTrimmedStringCollection(YarnConfiguration.RM_HA_IDS);
    if (ids.size() <= 0) {
      throwBadConfigurationException(
        getInvalidValueMessage(YarnConfiguration.RM_HA_IDS,
          conf.get(YarnConfiguration.RM_HA_IDS)));
    } else if (ids.size() == 1) {
      LOG.warn(getRMHAIdsWarningMessage(ids.toString()));
    }

    StringBuilder setValue = new StringBuilder();
    for (String id: ids) {
      setValue.append(id);
      setValue.append(",");
    }
    conf.set(YarnConfiguration.RM_HA_IDS,
      setValue.substring(0, setValue.length() - 1));
  }

  private static void verifyAndSetRMHAId(Configuration conf) {
    String rmId = conf.getTrimmed(YarnConfiguration.RM_HA_ID);
    if (rmId == null) {
      throwBadConfigurationException(
        getNeedToSetValueMessage(YarnConfiguration.RM_HA_ID));
    } else {
      Collection<String> ids = getRMHAIds(conf);
      if (!ids.contains(rmId)) {
        throwBadConfigurationException(
          getRMHAIdNeedToBeIncludedMessage(ids.toString(), rmId));
      }
    }
    conf.set(YarnConfiguration.RM_HA_ID, rmId);
  }

  private static void verifyAndSetConfValue(String prefix, Configuration conf) {
    String confKey = null;
    String confValue = null;
    try {
      confKey = getConfKeyForRMInstance(prefix, conf);
      confValue = getConfValueForRMInstance(prefix, conf);
      conf.set(prefix, confValue);
    } catch (YarnRuntimeException yre) {
      // Error at getRMHAId()
      throw yre;
    } catch (IllegalArgumentException iae) {
      String errmsg;
      if (confKey == null) {
        // Error at addSuffix
        errmsg = getInvalidValueMessage(YarnConfiguration.RM_HA_ID,
          getRMHAId(conf));
      } else {
        // Error at Configuration#set.
        errmsg = getNeedToSetValueMessage(confKey);
      }
      throwBadConfigurationException(errmsg);
    }
  }

  public static void verifyAndSetAllRpcAddresses(Configuration conf) {
    for (String confKey : RPC_ADDRESS_CONF_KEYS) {
     verifyAndSetConfValue(confKey, conf);
    }
  }

  /**
   * @param conf Configuration. Please use getRMHAIds to check.
   * @return RM Ids on success
   */
  public static Collection<String> getRMHAIds(Configuration conf) {
    return  conf.getStringCollection(YarnConfiguration.RM_HA_IDS);
  }

  /**
   * @param conf Configuration. Please use verifyAndSetRMHAId to check.
   * @return RM Id on success
   */
  @VisibleForTesting
  static String getRMHAId(Configuration conf) {
    return conf.get(YarnConfiguration.RM_HA_ID);
  }

  @VisibleForTesting
  static String getNeedToSetValueMessage(String confKey) {
    return confKey + " needs to be set in a HA configuration.";
  }

  @VisibleForTesting
  static String getInvalidValueMessage(String confKey,
                                              String invalidValue){
    return "Invalid value of "  + confKey +". "
      + "Current value is " + invalidValue;
  }

  @VisibleForTesting
  static String getRMHAIdNeedToBeIncludedMessage(String ids,
                                                        String rmId) {
    return YarnConfiguration.RM_HA_IDS + "("
      + ids +  ") need to contain " + YarnConfiguration.RM_HA_ID + "("
      + rmId + ") in a HA configuration.";
  }

  @VisibleForTesting
  static String getRMHAIdsWarningMessage(String ids) {
    return  "Resource Manager HA is enabled, but " +
      YarnConfiguration.RM_HA_IDS + " has only one id(" +
      ids.toString() + ")";
  }

  private static String getConfKeyForRMInstance(String prefix,
                                                Configuration conf) {
    return addSuffix(prefix, getRMHAId(conf));
  }

  private static String getConfValueForRMInstance(String prefix,
                                                  Configuration conf) {
    String confKey = getConfKeyForRMInstance(prefix, conf);
    String retVal = conf.getTrimmed(confKey);
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
