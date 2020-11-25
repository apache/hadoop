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

package org.apache.hadoop.yarn.server.federation.utils;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.registry.client.api.BindFlags;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Helper class that handles reads and writes to Yarn Registry to support UAM HA
 * and second attempt.
 */
public class FederationRegistryClient {
  private static final Logger LOG =
      LoggerFactory.getLogger(FederationRegistryClient.class);

  private RegistryOperations registry;

  private UserGroupInformation user;

  // AppId -> SubClusterId -> UAM token
  private Map<ApplicationId, Map<String, Token<AMRMTokenIdentifier>>>
      appSubClusterTokenMap;

  // Structure in registry: <registryBaseDir>/<AppId>/<SubClusterId> -> UAMToken
  private String registryBaseDir;

  public FederationRegistryClient(Configuration conf,
      RegistryOperations registry, UserGroupInformation user) {
    this.registry = registry;
    this.user = user;
    this.appSubClusterTokenMap = new ConcurrentHashMap<>();
    this.registryBaseDir =
        conf.get(YarnConfiguration.FEDERATION_REGISTRY_BASE_KEY,
            YarnConfiguration.DEFAULT_FEDERATION_REGISTRY_BASE_KEY);
    LOG.info("Using registry {} with base directory: {}",
        this.registry.getClass().getName(), this.registryBaseDir);
  }

  /**
   * Get the list of known applications in the registry.
   *
   * @return the list of known applications
   */
  public synchronized List<String> getAllApplications() {
    // Suppress the exception here because it is valid that the entry does not
    // exist
    List<String> applications = null;
    try {
      applications = listDirRegistry(this.registry, this.user,
          getRegistryKey(null, null), false);
    } catch (YarnException e) {
      LOG.warn("Unexpected exception from listDirRegistry", e);
    }
    if (applications == null) {
      // It is valid for listDirRegistry to return null
      return new ArrayList<>();
    }
    return applications;
  }

  /**
   * For testing, delete all application records in registry.
   */
  @VisibleForTesting
  public synchronized void cleanAllApplications() {
    try {
      removeKeyRegistry(this.registry, this.user, getRegistryKey(null, null),
          true, false);
    } catch (YarnException e) {
      LOG.warn("Unexpected exception from removeKeyRegistry", e);
    }
  }

  /**
   * Write/update the UAM token for an application and a sub-cluster.
   *
   * @param subClusterId sub-cluster id of the token
   * @param token the UAM of the application
   * @return whether the amrmToken is added or updated to a new value
   */
  public synchronized boolean writeAMRMTokenForUAM(ApplicationId appId,
      String subClusterId, Token<AMRMTokenIdentifier> token) {
    Map<String, Token<AMRMTokenIdentifier>> subClusterTokenMap =
        this.appSubClusterTokenMap.get(appId);
    if (subClusterTokenMap == null) {
      subClusterTokenMap = new ConcurrentHashMap<>();
      this.appSubClusterTokenMap.put(appId, subClusterTokenMap);
    }

    boolean update = !token.equals(subClusterTokenMap.get(subClusterId));
    if (!update) {
      LOG.debug("Same amrmToken received from {}, skip writing registry for {}",
          subClusterId, appId);
      return update;
    }

    LOG.info("Writing/Updating amrmToken for {} to registry for {}",
        subClusterId, appId);
    try {
      // First, write the token entry
      writeRegistry(this.registry, this.user,
          getRegistryKey(appId, subClusterId), token.encodeToUrlString(), true);

      // Then update the subClusterTokenMap
      subClusterTokenMap.put(subClusterId, token);
    } catch (YarnException | IOException e) {
      LOG.error(
          "Failed writing AMRMToken to registry for subcluster " + subClusterId,
          e);
    }
    return update;
  }

  /**
   * Load the information of one application from registry.
   *
   * @param appId application id
   * @return the sub-cluster to UAM token mapping
   */
  public synchronized Map<String, Token<AMRMTokenIdentifier>>
      loadStateFromRegistry(ApplicationId appId) {
    Map<String, Token<AMRMTokenIdentifier>> retMap = new HashMap<>();
    // Suppress the exception here because it is valid that the entry does not
    // exist
    List<String> subclusters = null;
    try {
      subclusters = listDirRegistry(this.registry, this.user,
          getRegistryKey(appId, null), false);
    } catch (YarnException e) {
      LOG.warn("Unexpected exception from listDirRegistry", e);
    }

    if (subclusters == null) {
      LOG.info("Application {} does not exist in registry", appId);
      return retMap;
    }

    // Read the amrmToken for each sub-cluster with an existing UAM
    for (String scId : subclusters) {
      LOG.info("Reading amrmToken for subcluster {} for {}", scId, appId);
      String key = getRegistryKey(appId, scId);
      try {
        String tokenString = readRegistry(this.registry, this.user, key, true);
        if (tokenString == null) {
          throw new YarnException("Null string from readRegistry key " + key);
        }
        Token<AMRMTokenIdentifier> amrmToken = new Token<>();
        amrmToken.decodeFromUrlString(tokenString);
        // Clear the service field, as if RM just issued the token
        amrmToken.setService(new Text());

        retMap.put(scId, amrmToken);
      } catch (Exception e) {
        LOG.error("Failed reading registry key " + key
            + ", skipping subcluster " + scId, e);
      }
    }

    // Override existing map if there
    this.appSubClusterTokenMap.put(appId, new ConcurrentHashMap<>(retMap));
    return retMap;
  }

  /**
   * Remove an application from registry.
   *
   * @param appId application id
   */
  public synchronized void removeAppFromRegistry(ApplicationId appId) {
    Map<String, Token<AMRMTokenIdentifier>> subClusterTokenMap =
        this.appSubClusterTokenMap.get(appId);
    LOG.info("Removing all registry entries for {}", appId);

    if (subClusterTokenMap == null || subClusterTokenMap.size() == 0) {
      return;
    }

    // Lastly remove the application directory
    String key = getRegistryKey(appId, null);
    try {
      removeKeyRegistry(this.registry, this.user, key, true, true);
      subClusterTokenMap.clear();
    } catch (YarnException e) {
      LOG.error("Failed removing registry directory key " + key, e);
    }
  }

  private String getRegistryKey(ApplicationId appId, String fileName) {
    if (appId == null) {
      return this.registryBaseDir;
    }
    if (fileName == null) {
      return this.registryBaseDir + appId.toString();
    }
    return this.registryBaseDir + appId.toString() + "/" + fileName;
  }

  private String readRegistry(final RegistryOperations registryImpl,
      UserGroupInformation ugi, final String key, final boolean throwIfFails)
      throws YarnException {
    // Use the ugi loaded with app credentials to access registry
    String result = ugi.doAs(new PrivilegedAction<String>() {
      @Override
      public String run() {
        try {
          ServiceRecord value = registryImpl.resolve(key);
          if (value != null) {
            return value.description;
          }
        } catch (Throwable e) {
          if (throwIfFails) {
            LOG.error("Registry resolve key " + key + " failed", e);
          }
        }
        return null;
      }
    });
    if (result == null && throwIfFails) {
      throw new YarnException("Registry resolve key " + key + " failed");
    }
    return result;
  }

  private void removeKeyRegistry(final RegistryOperations registryImpl,
      UserGroupInformation ugi, final String key, final boolean recursive,
      final boolean throwIfFails) throws YarnException {
    // Use the ugi loaded with app credentials to access registry
    boolean success = ugi.doAs(new PrivilegedAction<Boolean>() {
      @Override
      public Boolean run() {
        try {
          registryImpl.delete(key, recursive);
          return true;
        } catch (Throwable e) {
          if (throwIfFails) {
            LOG.error("Registry remove key " + key + " failed", e);
          }
        }
        return false;
      }
    });
    if (!success && throwIfFails) {
      throw new YarnException("Registry remove key " + key + " failed");
    }
  }

  /**
   * Write registry entry, override if exists.
   */
  private void writeRegistry(final RegistryOperations registryImpl,
      UserGroupInformation ugi, final String key, final String value,
      final boolean throwIfFails) throws YarnException {

    final ServiceRecord recordValue = new ServiceRecord();
    recordValue.description = value;
    // Use the ugi loaded with app credentials to access registry
    boolean success = ugi.doAs(new PrivilegedAction<Boolean>() {
      @Override
      public Boolean run() {
        try {
          registryImpl.bind(key, recordValue, BindFlags.OVERWRITE);
          return true;
        } catch (Throwable e) {
          if (throwIfFails) {
            LOG.error("Registry write key " + key + " failed", e);
          }
        }
        return false;
      }
    });
    if (!success && throwIfFails) {
      throw new YarnException("Registry write key " + key + " failed");
    }
  }

  /**
   * List the sub directories in the given directory.
   */
  private List<String> listDirRegistry(final RegistryOperations registryImpl,
      UserGroupInformation ugi, final String key, final boolean throwIfFails)
      throws YarnException {
    List<String> result = ugi.doAs(new PrivilegedAction<List<String>>() {
      @Override
      public List<String> run() {
        try {
          return registryImpl.list(key);
        } catch (Throwable e) {
          if (throwIfFails) {
            LOG.error("Registry list key " + key + " failed", e);
          }
        }
        return null;
      }
    });
    if (result == null && throwIfFails) {
      throw new YarnException("Registry list key " + key + " failed");
    }
    return result;
  }

}
