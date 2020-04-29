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

import java.net.InetSocketAddress;
import java.util.Collection;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class HAUtil {
  private static Logger LOG = LoggerFactory.getLogger(HAUtil.class);

  @VisibleForTesting
  public static final String BAD_CONFIG_MESSAGE_PREFIX =
    "Invalid configuration! ";

  private HAUtil() { /* Hidden constructor */ }

  private static void throwBadConfigurationException(String msg) {
    throw new YarnRuntimeException(BAD_CONFIG_MESSAGE_PREFIX + msg);
  }

  /**
   * Returns true if Federation is configured.
   *
   * @param conf Configuration
   * @return true if federation is configured in the configuration; else false.
   */
  public static boolean isFederationEnabled(Configuration conf) {
    return conf.getBoolean(YarnConfiguration.FEDERATION_ENABLED,
        YarnConfiguration.DEFAULT_FEDERATION_ENABLED);
  }

  /**
   * Returns true if RM failover is enabled in a Federation setting.
   *
   * @param conf Configuration
   * @return if RM failover is enabled in conjunction with Federation in the
   *         configuration; else false.
   */
  public static boolean isFederationFailoverEnabled(Configuration conf) {
    return conf.getBoolean(YarnConfiguration.FEDERATION_FAILOVER_ENABLED,
        YarnConfiguration.DEFAULT_FEDERATION_FAILOVER_ENABLED);
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

  public static boolean isAutomaticFailoverEnabled(Configuration conf) {
    return conf.getBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED,
        YarnConfiguration.DEFAULT_AUTO_FAILOVER_ENABLED);
  }

  public static boolean isAutomaticFailoverEnabledAndEmbedded(
      Configuration conf) {
    return isAutomaticFailoverEnabled(conf) &&
        isAutomaticFailoverEmbedded(conf);
  }

  public static boolean isAutomaticFailoverEmbedded(Configuration conf) {
    return conf.getBoolean(YarnConfiguration.AUTO_FAILOVER_EMBEDDED,
        YarnConfiguration.DEFAULT_AUTO_FAILOVER_EMBEDDED);
  }

  /**
   * Verify configuration for Resource Manager HA.
   * @param conf Configuration
   * @throws YarnRuntimeException
   */
  public static void verifyAndSetConfiguration(Configuration conf)
    throws YarnRuntimeException {
    verifyAndSetRMHAIdsList(conf);
    verifyAndSetCurrentRMHAId(conf);
    verifyLeaderElection(conf);
    verifyAndSetAllServiceAddresses(conf);
  }

  /**
   * Verify configuration that there are at least two RM-ids
   * and RPC addresses are specified for each RM-id.
   * Then set the RM-ids.
   */
  private static void verifyAndSetRMHAIdsList(Configuration conf) {
    Collection<String> ids =
      conf.getTrimmedStringCollection(YarnConfiguration.RM_HA_IDS);
    if (ids.size() < 2) {
      throwBadConfigurationException(
        getInvalidValueMessage(YarnConfiguration.RM_HA_IDS,
          conf.get(YarnConfiguration.RM_HA_IDS) +
          "\nHA mode requires atleast two RMs"));
    }

    StringBuilder setValue = new StringBuilder();
    for (String id: ids) {
      // verify the RM service addresses configurations for every RMIds
      for (String prefix : YarnConfiguration.getServiceAddressConfKeys(conf)) {
        checkAndSetRMRPCAddress(prefix, id, conf);
      }
      setValue.append(id)
          .append(",");
    }
    conf.set(YarnConfiguration.RM_HA_IDS,
      setValue.substring(0, setValue.length() - 1));
  }

  private static void verifyAndSetCurrentRMHAId(Configuration conf) {
    String rmId = getRMHAId(conf);
    if (rmId == null) {
      StringBuilder msg = new StringBuilder();
      msg.append("Can not find valid RM_HA_ID. None of ");
      for (String id : conf
          .getTrimmedStringCollection(YarnConfiguration.RM_HA_IDS)) {
        msg.append(addSuffix(YarnConfiguration.RM_ADDRESS, id)).append(" ");
      }
      msg.append(" are matching" +
          " the local address OR " + YarnConfiguration.RM_HA_ID + " is not" +
          " specified in HA Configuration");
      throwBadConfigurationException(msg.toString());
    } else {
      Collection<String> ids = getRMHAIds(conf);
      if (!ids.contains(rmId)) {
        throwBadConfigurationException(
          getRMHAIdNeedToBeIncludedMessage(ids.toString(), rmId));
      }
    }
    conf.set(YarnConfiguration.RM_HA_ID, rmId);
  }

  /**
   * This method validates that some leader election service is enabled. YARN
   * allows leadership election to be disabled in the configuration, which
   * breaks automatic failover. If leadership election is disabled, this
   * method will throw an exception via
   * {@link #throwBadConfigurationException(java.lang.String)}.
   *
   * @param conf the {@link Configuration} to validate
   */
  private static void verifyLeaderElection(Configuration conf) {
    if (isAutomaticFailoverEnabled(conf) &&
        !conf.getBoolean(YarnConfiguration.CURATOR_LEADER_ELECTOR,
          YarnConfiguration.DEFAULT_CURATOR_LEADER_ELECTOR_ENABLED) &&
        !isAutomaticFailoverEmbedded(conf)) {
      throwBadConfigurationException(NO_LEADER_ELECTION_MESSAGE);
    }
  }

  @VisibleForTesting
  static final String NO_LEADER_ELECTION_MESSAGE =
      "The yarn.resourcemanager.ha.automatic-failover.embedded "
      + "and yarn.resourcemanager.ha.curator-leader-elector.enabled "
      + "properties are both false. One of these two properties must "
      + "be true when yarn.resourcemanager.ha.automatic-failover.enabled "
      + "is true";

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

  public static void verifyAndSetAllServiceAddresses(Configuration conf) {
    for (String confKey : YarnConfiguration.getServiceAddressConfKeys(conf)) {
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
  public static String getRMHAId(Configuration conf) {
    int found = 0;
    String currentRMId = conf.getTrimmed(YarnConfiguration.RM_HA_ID);
    if(currentRMId == null) {
      for(String rmId : getRMHAIds(conf)) {
        String key = addSuffix(YarnConfiguration.RM_ADDRESS, rmId);
        String addr = conf.get(key);
        if (addr == null) {
          continue;
        }
        InetSocketAddress s;
        try {
          s = NetUtils.createSocketAddr(addr);
        } catch (Exception e) {
          LOG.warn("Exception in creating socket address " + addr, e);
          continue;
        }
        if (!s.isUnresolved() && NetUtils.isLocalAddress(s.getAddress())) {
          currentRMId = rmId.trim();
          found++;
        }
      }
    }
    if (found > 1) { // Only one address must match the local address
      String msg = "The HA Configuration has multiple addresses that match "
          + "local node's address.";
      throw new HadoopIllegalArgumentException(msg);
    }
    return currentRMId;
  }

  @VisibleForTesting
  static String getNeedToSetValueMessage(String confKey) {
    return confKey + " needs to be set in an HA configuration.";
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
      + rmId + ") in an HA configuration.";
  }

  @VisibleForTesting
  static String getRMHAIdsWarningMessage(String ids) {
    return  "Resource Manager HA is enabled, but " +
      YarnConfiguration.RM_HA_IDS + " has only one id(" +
      ids.toString() + ")";
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  static String getConfKeyForRMInstance(String prefix, Configuration conf) {
    if (!YarnConfiguration.getServiceAddressConfKeys(conf).contains(prefix)) {
      return prefix;
    } else {
      String RMId = getRMHAId(conf);
      checkAndSetRMRPCAddress(prefix, RMId, conf);
      return addSuffix(prefix, RMId);
    }
  }

  public static String getConfValueForRMInstance(String prefix,
                                                 Configuration conf) {
    String confKey = getConfKeyForRMInstance(prefix, conf);
    String retVal = conf.getTrimmed(confKey);
    if (LOG.isTraceEnabled()) {
      LOG.trace("getConfValueForRMInstance: prefix = {};" +
          " confKey being looked up = {};" +
          " value being set to = {}", prefix, confKey, retVal);
    }
    return retVal;
  }

  public static String getConfValueForRMInstance(
      String prefix, String defaultValue, Configuration conf) {
    String value = getConfValueForRMInstance(prefix, conf);
    return (value == null) ? defaultValue : value;
  }

  /**
   * Add non empty and non null suffix to a key.
   * @return the suffixed key
   */
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

  private static void checkAndSetRMRPCAddress(String prefix, String RMId,
      Configuration conf) {
    String rpcAddressConfKey = null;
    try {
      rpcAddressConfKey = addSuffix(prefix, RMId);
      if (conf.getTrimmed(rpcAddressConfKey) == null) {
        String hostNameConfKey = addSuffix(YarnConfiguration.RM_HOSTNAME, RMId);
        String confVal = conf.getTrimmed(hostNameConfKey);
        if (confVal == null) {
          throwBadConfigurationException(getNeedToSetValueMessage(
              hostNameConfKey + " or " + addSuffix(prefix, RMId)));
        } else {
          conf.set(addSuffix(prefix, RMId), confVal + ":"
              + YarnConfiguration.getRMDefaultPortNumber(prefix, conf));
        }
      }
    } catch (IllegalArgumentException iae) {
      String errmsg = iae.getMessage();
      if (rpcAddressConfKey == null) {
        // Error at addSuffix
        errmsg = getInvalidValueMessage(YarnConfiguration.RM_HA_ID, RMId);
      }
      throwBadConfigurationException(errmsg);
    }
  }
}
