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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.DomainNameResolver;
import org.apache.hadoop.net.DomainNameResolverFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class HAUtil {
  private static Logger LOG = LoggerFactory.getLogger(HAUtil.class);

  @VisibleForTesting
  public static final String BAD_CONFIG_MESSAGE_PREFIX =
    "Invalid configuration! ";

  private final static List<String> RM_ADDRESS_CONFIG_KEYS = Arrays.asList(
      YarnConfiguration.RM_ADDRESS,
      YarnConfiguration.RM_SCHEDULER_ADDRESS,
      YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
      YarnConfiguration.RM_ADMIN_ADDRESS,
      YarnConfiguration.RM_WEBAPP_ADDRESS,
      YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS
  );

  private static DomainNameResolver dnr;
  private static Collection<String> originalRMIDs = null;

  private HAUtil() { /* Hidden constructor */ }

  public static DomainNameResolver getDnr() {
    return dnr;
  }

  public static void setDnrByConfiguration(Configuration conf) {
    HAUtil.dnr = DomainNameResolverFactory.newInstance(
        conf, YarnConfiguration.RESOLVE_RM_ADDRESS_KEY);
  }

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
    // Federation failover is not enabled unless federation is enabled. This previously caused
    // YARN RMProxy to use the HA Retry policy in a non-HA & non-federation environments because
    // the default federation failover enabled value is true.
    return isFederationEnabled(conf) &&
        conf.getBoolean(YarnConfiguration.FEDERATION_FAILOVER_ENABLED,
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
   * @throws YarnRuntimeException thrown by a remote service.
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
    boolean resolveNeeded = conf.getBoolean(
        YarnConfiguration.RESOLVE_RM_ADDRESS_NEEDED_KEY,
        YarnConfiguration.RESOLVE_RM_ADDRESS_NEEDED_DEFAULT);
    if (resolveNeeded) {
      getRMHAId(conf);
    }
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
   * Instead of returning RM_HA_IDS in current configurations, it
   * would return the originally preset one in case of DNS resolving.
   * @param conf Configuration.
   * @return RM Ids from original xml file
   */
  public static Collection<String> getOriginalRMHAIds(Configuration conf) {
    return originalRMIDs == null ? getRMHAIds(conf) : originalRMIDs;
  }

  /**
   * @param conf Configuration. Please use verifyAndSetRMHAId to check.
   * @return RM Id on success
   */
  public static String getRMHAId(Configuration conf) {
    int found = 0;
    String currentRMId = conf.getTrimmed(YarnConfiguration.RM_HA_ID);
    if(currentRMId == null) {
      Map<String, InetSocketAddress> idAddressPairs = getResolvedRMIdPairs(conf);
      for (Map.Entry<String, InetSocketAddress> entry : idAddressPairs.entrySet()) {
        String rmId = entry.getKey();
        InetSocketAddress s = entry.getValue();
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

  /**
   * This function resolves all RMIds with their address. For multi-A DNS records,
   * it will resolve all of them, and generate a new Id for each of them.
   *
   * @param conf Configuration
   * @return Map key as RMId, value as its address
   */
  public static Map<String, InetSocketAddress> getResolvedRMIdPairs(
      Configuration conf) {
    boolean resolveNeeded = conf.getBoolean(
        YarnConfiguration.RESOLVE_RM_ADDRESS_NEEDED_KEY,
        YarnConfiguration.RESOLVE_RM_ADDRESS_NEEDED_DEFAULT);
    boolean requireFQDN = conf.getBoolean(
        YarnConfiguration.RESOLVE_RM_ADDRESS_TO_FQDN,
        YarnConfiguration.RESOLVE_RM_ADDRESS_TO_FQDN_DEFAULT);
    // In case client using DIFFERENT addresses for each service address
    // need to categorize them first
    Map<List<String>, List<String>> addressesConfigKeysMap = new HashMap<>();
    Collection<String> rmIds = getOriginalRMHAIds(conf);
    for (String configKey : RM_ADDRESS_CONFIG_KEYS) {
      List<String> addresses = new ArrayList<>();
      for (String rmId : rmIds) {
        String keyToRead = addSuffix(configKey, rmId);
        InetSocketAddress address = getInetSocketAddressFromString(
            conf.get(keyToRead));
        if (address != null) {
          addresses.add(address.getHostName());
        }
      }
      Collections.sort(addresses);
      List<String> configKeysOfTheseAddresses = addressesConfigKeysMap.get(addresses);
      if (configKeysOfTheseAddresses == null) {
        configKeysOfTheseAddresses = new ArrayList<>();
        addressesConfigKeysMap.put(addresses, configKeysOfTheseAddresses);
      }
      configKeysOfTheseAddresses.add(configKey);
    }
    // We need to resolve and override by group (categorized by their input host)
    // But since the function is called from "getRMHAId",
    // this function would only return value which is corresponded to YarnConfiguration.RM_ADDRESS
    Map<String, InetSocketAddress> ret = null;
    for (List<String> configKeys : addressesConfigKeysMap.values()) {
      Map<String, InetSocketAddress> res = getResolvedIdPairs(
          conf, resolveNeeded, requireFQDN, getOriginalRMHAIds(conf),
          configKeys.get(0), YarnConfiguration.RM_HA_IDS, configKeys);
      if (configKeys.contains(YarnConfiguration.RM_ADDRESS)) {
        ret = res;
      }
    }
    return ret;
  }

  private static Map<String, InetSocketAddress> getResolvedIdPairs(
      Configuration conf, boolean resolveNeeded, boolean requireFQDN, Collection<String> ids,
      String configKey, String configKeyToReplace, List<String> listOfConfigKeysToReplace) {
    Map<String, InetSocketAddress> idAddressPairs = new HashMap<>();
    Map<String, String> generatedIdToOriginalId = new HashMap<>();
    for (String id : ids) {
      String key = addSuffix(configKey, id);
      String addr = conf.get(key); // string with port
      InetSocketAddress address = getInetSocketAddressFromString(addr);
      if (address == null) {
        continue;
      }
      if (resolveNeeded) {
        if (dnr == null) {
          setDnrByConfiguration(conf);
        }
        // If the address needs to be resolved, get all of the IP addresses
        // from this address and pass them into the map
        LOG.info("Multi-A domain name {} will be resolved by {}" + addr, dnr.getClass().getName());
        int port = address.getPort();
        String[] resolvedHostNames;
        try {
          resolvedHostNames = dnr.getAllResolvedHostnameByDomainName(
              address.getHostName(), requireFQDN);
        } catch (UnknownHostException e) {
          LOG.warn("Exception in resolving socket address {}", address.getHostName(), e);
          continue;
        }
        LOG.info("Resolved addresses for {} is {}", addr, Arrays.toString(resolvedHostNames));
        if (resolvedHostNames == null || resolvedHostNames.length < 1) {
          LOG.warn("Cannot resolve from address {}", address.getHostName());
        } else {
          // If multiple address resolved, corresponding id needs to be created
          for (int i = 0; i < resolvedHostNames.length; i++) {
            String generatedRMId = id + "_resolved_" + (i + 1);
            idAddressPairs.put(generatedRMId,
                new InetSocketAddress(resolvedHostNames[i], port));
            generatedIdToOriginalId.put(generatedRMId, id);
          }
        }
        overrideIdsInConfiguration(
            idAddressPairs, generatedIdToOriginalId, configKeyToReplace,
            listOfConfigKeysToReplace, conf);
      } else {
        idAddressPairs.put(id, address);
      }
    }
    return idAddressPairs;
  }

  /**
   * This function override all RMIds and their addresses by the input Map.
   *
   * @param idAddressPairs          key as Id, value as its address
   * @param generatedIdToOriginalId key as generated rmId from multi-A,
   *                                value as its original input Id
   * @param configKeyToReplace      key to replace
   * @param listOfConfigKeysToReplace list of keys to replace/add
   * @param conf                    Configuration
   */
  synchronized static void overrideIdsInConfiguration(
      Map<String, InetSocketAddress> idAddressPairs,
      Map<String, String> generatedIdToOriginalId,
      String configKeyToReplace, List<String> listOfConfigKeysToReplace,
      Configuration conf) {
    Collection<String> currentIds = getRMHAIds(conf);
    Set<String> resolvedIds = new HashSet<>(idAddressPairs.keySet());
    // override rm-ids
    if (originalRMIDs == null) {
      originalRMIDs = currentIds;
    }
    // if it is already resolved, we need to form a superset
    resolvedIds.addAll((currentIds));
    resolvedIds.removeAll(generatedIdToOriginalId.values());
    conf.setStrings(configKeyToReplace,
        resolvedIds.toArray(new String[0]));
    // override/add address configuration entries for each rm-id
    for (Map.Entry<String, InetSocketAddress> entry : idAddressPairs.entrySet()) {
      String rmId = entry.getKey();
      String addr = entry.getValue().getHostName();
      String originalRMId = generatedIdToOriginalId.get(rmId);
      if (originalRMId != null) {
        // for each required configKeys, get its port and then set it back
        for (String configKey : listOfConfigKeysToReplace) {
          String keyToRead = addSuffix(configKey, originalRMId);
          InetSocketAddress originalAddress = getInetSocketAddressFromString(
              conf.get(keyToRead));
          if (originalAddress == null) {
            LOG.warn("Missing configuration for key {}", keyToRead);
            continue;
          }
          int port = originalAddress.getPort();
          String keyToWrite = addSuffix(configKey, rmId);
          conf.setSocketAddr(keyToWrite, new InetSocketAddress(addr, port));
        }
      }
    }
  }

  /**
   * Helper function to create InetsocketAddress from string address.
   *
   * @param addr string format of address accepted by NetUtils.createSocketAddr
   * @return InetSocketAddress of input, would return null upon any kinds of invalid inout
   */
  public static InetSocketAddress getInetSocketAddressFromString(String addr) {
    if (addr == null) {
      return null;
    }
    InetSocketAddress address;
    try {
      address = NetUtils.createSocketAddr(addr);
    } catch (Exception e) {
      LOG.warn("Exception in creating socket address {}", addr, e);
      return null;
    }
    return address;
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
   * Add non-empty and non-null suffix to a key.
   *
   * @param key key.
   * @param suffix suffix.
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
