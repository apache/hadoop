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
package org.apache.hadoop.crypto.key.kms.server;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.kms.server.KeyAuthorizationKeyProvider.KeyOpType;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Provides access to the <code>AccessControlList</code>s used by KMS,
 * hot-reloading them if the <code>kms-acls.xml</code> file where the ACLs
 * are defined has been updated.
 */
@InterfaceAudience.Private
public class FileBasedKMSACLs extends KMSACLs {
  private static final Logger LOG = LoggerFactory.getLogger(KMSACLs.class);


  private static final String ACL_DEFAULT =
      AccessControlList.WILDCARD_ACL_VALUE;

  private volatile Map<Type, AccessControlList> acls;
  private volatile Map<Type, AccessControlList> blacklistedAcls;
  @VisibleForTesting
  volatile Map<String, HashMap<KeyOpType, AccessControlList>> keyAcls;
  @VisibleForTesting
  volatile Map<KeyOpType, AccessControlList> defaultKeyAcls = new HashMap<>();
  @VisibleForTesting
  volatile Map<KeyOpType, AccessControlList> whitelistKeyAcls = new HashMap<>();
  private long lastReload;

  FileBasedKMSACLs(Configuration conf) {
    if (conf == null) {
      conf = loadACLs();
    }
    setKMSACLs(conf);
    setKeyACLs(conf);
  }

  public FileBasedKMSACLs() {
    this(null);
  }

  private void setKMSACLs(Configuration conf) {
    Map<Type, AccessControlList> tempAcls = new HashMap<Type, AccessControlList>();
    Map<Type, AccessControlList> tempBlacklist = new HashMap<Type, AccessControlList>();
    for (Type aclType : Type.values()) {
      String aclStr = conf.get(aclType.getAclConfigKey(), ACL_DEFAULT);
      tempAcls.put(aclType, new AccessControlList(aclStr));
      String blacklistStr = conf.get(aclType.getBlacklistConfigKey());
      if (blacklistStr != null) {
        // Only add if blacklist is present
        tempBlacklist.put(aclType, new AccessControlList(blacklistStr));
        LOG.info("'{}' Blacklist '{}'", aclType, blacklistStr);
      }
      LOG.info("'{}' ACL '{}'", aclType, aclStr);
    }
    acls = tempAcls;
    blacklistedAcls = tempBlacklist;
  }

  @VisibleForTesting
  void setKeyACLs(Configuration conf) {
    Map<String, HashMap<KeyOpType, AccessControlList>> tempKeyAcls =
        new HashMap<String, HashMap<KeyOpType,AccessControlList>>();
    Map<String, String> allKeyACLS =
        conf.getValByRegex(KMSConfiguration.KEY_ACL_PREFIX_REGEX);
    for (Map.Entry<String, String> keyAcl : allKeyACLS.entrySet()) {
      String k = keyAcl.getKey();
      // this should be of type "key.acl.<KEY_NAME>.<OP_TYPE>"
      int keyNameStarts = KMSConfiguration.KEY_ACL_PREFIX.length();
      int keyNameEnds = k.lastIndexOf(".");
      if (keyNameStarts >= keyNameEnds) {
        LOG.warn("Invalid key name '{}'", k);
      } else {
        String aclStr = keyAcl.getValue();
        String keyName = k.substring(keyNameStarts, keyNameEnds);
        String keyOp = k.substring(keyNameEnds + 1);
        KeyOpType aclType = null;
        try {
          aclType = KeyOpType.valueOf(keyOp);
        } catch (IllegalArgumentException e) {
          LOG.warn("Invalid key Operation '{}'", keyOp);
        }
        if (aclType != null) {
          // On the assumption this will be single threaded.. else we need to
          // ConcurrentHashMap
          HashMap<KeyOpType,AccessControlList> aclMap =
              tempKeyAcls.get(keyName);
          if (aclMap == null) {
            aclMap = new HashMap<KeyOpType, AccessControlList>();
            tempKeyAcls.put(keyName, aclMap);
          }
          aclMap.put(aclType, new AccessControlList(aclStr));
          LOG.info("KEY_NAME '{}' KEY_OP '{}' ACL '{}'",
              keyName, aclType, aclStr);
        }
      }
    }
    keyAcls = tempKeyAcls;

    final Map<KeyOpType, AccessControlList> tempDefaults = new HashMap<>();
    final Map<KeyOpType, AccessControlList> tempWhitelists = new HashMap<>();
    for (KeyOpType keyOp : KeyOpType.values()) {
      parseAclsWithPrefix(conf, KMSConfiguration.DEFAULT_KEY_ACL_PREFIX,
          keyOp, tempDefaults);
      parseAclsWithPrefix(conf, KMSConfiguration.WHITELIST_KEY_ACL_PREFIX,
          keyOp, tempWhitelists);
    }
    defaultKeyAcls = tempDefaults;
    whitelistKeyAcls = tempWhitelists;
  }

  /**
   * Parse the acls from configuration with the specified prefix. Currently
   * only 2 possible prefixes: whitelist and default.
   *
   * @param conf The configuration.
   * @param prefix The prefix.
   * @param keyOp The key operation.
   * @param results The collection of results to add to.
   */
  private void parseAclsWithPrefix(final Configuration conf,
      final String prefix, final KeyOpType keyOp,
      Map<KeyOpType, AccessControlList> results) {
    String confKey = prefix + keyOp;
    String aclStr = conf.get(confKey);
    if (aclStr != null) {
      if (keyOp == KeyOpType.ALL) {
        // Ignore All operation for default key and whitelist key acls
        LOG.warn("Invalid KEY_OP '{}' for {}, ignoring", keyOp, prefix);
      } else {
        if (aclStr.equals("*")) {
          LOG.info("{} for KEY_OP '{}' is set to '*'", prefix, keyOp);
        }
        results.put(keyOp, new AccessControlList(aclStr));
      }
    }
  }

  @Override
  public void loadAcls(boolean forceReload) {
    try {
      if (forceReload || KMSConfiguration.isACLsFileNewer(lastReload)) {
        setKMSACLs(loadACLs());
        setKeyACLs(loadACLs());
      }
    } catch (Exception ex) {
      LOG.warn(
          String.format("Could not reload ACLs file: '%s'", ex.toString()), ex);
    }
  }

  private Configuration loadACLs() {
    LOG.debug("Loading ACLs file");
    lastReload = System.currentTimeMillis();
    Configuration conf = KMSConfiguration.getACLsConf();
    // triggering the resource loading.
    conf.get(Type.CREATE.getAclConfigKey());
    return conf;
  }

  /**
   * First Check if user is in ACL for the KMS operation, if yes, then
   * return true if user is not present in any configured blacklist for
   * the operation
   * @param keyOperation KMS Operation
   * @param ugi UserGroupInformation of user
   * @return true is user has access
   */
  @Override
  public boolean hasAccess(Type keyOperation, UserGroupInformation ugi) {
    boolean access = acls.get(keyOperation).isUserAllowed(ugi);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Checking user [{}] for: {} {} ", ugi.getShortUserName(),
          keyOperation.toString(), acls.get(keyOperation).getAclString());
    }
    if (access) {
      AccessControlList blacklist = blacklistedAcls.get(keyOperation);
      access = (blacklist == null) || !blacklist.isUserInList(ugi);
      if (LOG.isDebugEnabled()) {
        if (blacklist == null) {
          LOG.debug("No blacklist for {}", keyOperation.toString());
        } else if (access) {
          LOG.debug("user is not in {}" , blacklist.getAclString());
        } else {
          LOG.debug("user is in {}" , blacklist.getAclString());
        }
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("User: [{}], Type: {} Result: {}", ugi.getShortUserName(),
          keyOperation.toString(), access);
    }
    return access;
  }

  @Override
  public boolean hasAccessToKey(String keyName, UserGroupInformation ugi,
      KeyOpType opType) {
    boolean access = checkKeyAccess(keyName, ugi, opType)
        || checkKeyAccess(whitelistKeyAcls, ugi, opType);
    if (!access) {
      KMSWebApp.getKMSAudit().unauthorized(ugi, opType, keyName);
    }
    return access;
  }

  private boolean checkKeyAccess(String keyName, UserGroupInformation ugi,
      KeyOpType opType) {
    Map<KeyOpType, AccessControlList> keyAcl = keyAcls.get(keyName);
    if (keyAcl == null) {
      // If No key acl defined for this key, check to see if
      // there are key defaults configured for this operation
      LOG.debug("Key: {} has no ACLs defined, using defaults.", keyName);
      keyAcl = defaultKeyAcls;
    }
    boolean access = checkKeyAccess(keyAcl, ugi, opType);
    if (LOG.isDebugEnabled()) {
      LOG.debug("User: [{}], OpType: {}, KeyName: {} Result: {}",
          ugi.getShortUserName(), opType.toString(), keyName, access);
    }
    return access;
  }

  private boolean checkKeyAccess(Map<KeyOpType, AccessControlList> keyAcl,
      UserGroupInformation ugi, KeyOpType opType) {
    AccessControlList acl = keyAcl.get(opType);
    if (acl == null) {
      // If no acl is specified for this operation,
      // deny access
      LOG.debug("No ACL available for key, denying access for {}", opType);
      return false;
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Checking user [{}] for: {}: {}", ugi.getShortUserName(),
            opType.toString(), acl.getAclString());
      }
      return acl.isUserAllowed(ugi);
    }
  }


  @Override
  public boolean isACLPresent(String keyName, KeyOpType opType) {
    return (keyAcls.containsKey(keyName)
        || defaultKeyAcls.containsKey(opType)
        || whitelistKeyAcls.containsKey(opType));
  }
}
