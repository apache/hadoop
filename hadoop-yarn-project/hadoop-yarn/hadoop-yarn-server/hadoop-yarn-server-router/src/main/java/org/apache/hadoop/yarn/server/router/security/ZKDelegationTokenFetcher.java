/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.router.security;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.curator.ZKCuratorManager;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStoreUtils;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.RMDelegationTokenIdentifierData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

public class ZKDelegationTokenFetcher extends DelegationTokenFetcher {

  private static final Logger LOG =
      LoggerFactory.getLogger(ZKDelegationTokenFetcher.class);

  protected static final String RM_DT_SECRET_MANAGER_ROOT = "RMDTSecretManagerRoot";
  protected static final String DELEGATION_KEY_PREFIX = "DelegationKey_";
  protected static final String DELEGATION_TOKEN_PREFIX = "RMDelegationToken_";

  private static final String RM_DELEGATION_TOKENS_ROOT_ZNODE_NAME = "RMDelegationTokensRoot";
  private static final String RM_DT_SEQUENTIAL_NUMBER_ZNODE_NAME = "RMDTSequentialNumber";
  private static final String RM_DT_MASTER_KEYS_ROOT_ZNODE_NAME = "RMDTMasterKeysRoot";
  public static final String ROOT_ZNODE_NAME = "ZKRMStateRoot";

  private ZKCuratorManager zkManager;
  private String rootPath;

  public ZKDelegationTokenFetcher(Configuration conf, ZKCuratorManager zkcuratorManager,
      RouterDelegationTokenSecretManager secretManager) throws Exception {
    super(secretManager);
    this.zkManager = zkcuratorManager;
    this.rootPath = conf.get("yarn.resourcemanager.zk-state-store.rootpath", "/federation");
  }

  @Override
  public void start() throws Exception {
    PathChildrenCache subClusterCache =
        new PathChildrenCache(zkManager.getCurator(), rootPath, true);
    subClusterCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
    subClusterCache.getListenable().addListener(new PathChildrenCacheListener() {
      @Override
      public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
        switch (event.getType()) {
          case CHILD_ADDED:
          case CHILD_UPDATED:
          case CHILD_REMOVED:
          default:
            break;
        }
      }
    });

    for (ChildData data : subClusterCache.getCurrentData()) {
      processSubCluster(data.getPath(), data.getData());
    }
  }

  private void processSubCluster(String path, byte[] data) throws Exception {
    LOG.info("Monitor SubCluster path: {}.", path);
    rootPath = path + "/" + ROOT_ZNODE_NAME + "/" + RM_DT_SECRET_MANAGER_ROOT;
    zkManager.createRootDirRecursively(rootPath);
    monitorMasterKey(rootPath + "/" + RM_DT_MASTER_KEYS_ROOT_ZNODE_NAME);
    monitorDelegationToken(rootPath + "/" + RM_DELEGATION_TOKENS_ROOT_ZNODE_NAME);
  }

  private void monitorDelegationToken(String path) throws Exception {
    if (!zkManager.exists(path)) {
      zkManager.create(path);
    }
    LOG.info("Monitor DelegationToken path: {}.", path);
    PathChildrenCache delegationTokenCache =
        new PathChildrenCache(zkManager.getCurator(), path, true);
    delegationTokenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

    PathChildrenCacheListener listener = new PathChildrenCacheListener() {
      @Override
      public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
          throws Exception {
        LOG.info("Path: {}, Type: {}.", event.getData().getPath(), event.getType());
        switch (event.getType()) {
          case CHILD_ADDED:
          case CHILD_UPDATED:
            processDTNode(event.getData().getPath(), event.getData().getData(), true);
            break;
          case CHILD_REMOVED:
            processDTNode(event.getData().getPath(), event.getData().getData(), false);
            break;
          default:
            break;
        }
      }
    };
    delegationTokenCache.getListenable().addListener(listener);
    for (ChildData data : delegationTokenCache.getCurrentData()) {
      processDTNode(data.getPath(), data.getData(), true);
    }
  }

  private void monitorMasterKey(String path) throws Exception {
    if (!zkManager.exists(path)) {
      zkManager.create(path);
    }
    LOG.info("Monitor MasterKey path: {}.", path);
    PathChildrenCache masterKeyCache = new PathChildrenCache(zkManager.getCurator(), path, true);
    masterKeyCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
    PathChildrenCacheListener listener = (client, event) -> {
      LOG.info("Path: {}, Type: {}.", event.getData().getPath(), event.getType());
      switch (event.getType()) {
      case CHILD_ADDED:
      case CHILD_UPDATED:
        processKeyNode(event.getData().getPath(), event.getData().getData(), true);
        break;
      case CHILD_REMOVED:
      default:
        break;
      }
    };
    masterKeyCache.getListenable().addListener(listener);
    for (ChildData data : masterKeyCache.getCurrentData()) {
      processKeyNode(data.getPath(), data.getData(), true);
    }
  }

  private void processKeyNode(String path, byte[] data, boolean isUpdate) throws Exception {
    if (!getChildName(path).startsWith(DELEGATION_KEY_PREFIX)) {
      LOG.info("Path: {} is not start with {}.", path, DELEGATION_KEY_PREFIX);
      return;
    }
    if (data == null) {
      LOG.warn("Content of {} is broken.", path);
    } else {
      String cluster = getClusterName(path);
      ByteArrayInputStream is = new ByteArrayInputStream(data);
      try (DataInputStream fsIn = new DataInputStream(is)) {
        DelegationKey key = new DelegationKey();
        key.readFields(fsIn);
        if (isUpdate) {
          RouterDelegationKey ekey = new RouterDelegationKey(cluster, key);
          updateMasterKey(ekey);
          if (LOG.isInfoEnabled()) {
            LOG.info("Loaded delegation key: keyId = {}, expirationDate = {}.",
                key.getKeyId(), key.getExpiryDate());
          }
        }
      }
    }
  }

  private void processDTNode(String path, byte[] data, boolean isUpdate) throws Exception {
    if (!getChildName(path).startsWith(DELEGATION_TOKEN_PREFIX)) {
      LOG.info("Path: {} is not start with {}.", path, DELEGATION_TOKEN_PREFIX);
      return;
    }

    if (data == null) {
      LOG.warn("Content of {} is broken.", path);
    } else {
      String cluster = getClusterName(path);
      ByteArrayInputStream is = new ByteArrayInputStream(data);
      try (DataInputStream fsIn = new DataInputStream(is)) {
        RMDelegationTokenIdentifierData identifierData =
            RMStateStoreUtils.readRMDelegationTokenIdentifierData(fsIn);
        RMDelegationTokenIdentifier identifier =
            identifierData.getTokenIdentifier();
        long renewDate = identifierData.getRenewDate();
        if (isUpdate) {
          RMDelegationTokenIdentifier eIdentifier =
              new RouterRMDelegationTokenIdentifier(cluster, identifier);
          updateToken(eIdentifier, renewDate);
          if (LOG.isInfoEnabled()) {
            LOG.info("Loaded RMDelegationTokenIdentifier: {}, renewDate: {}.",
                identifier, renewDate);
          }
        } else {
          Token fakeToken = new Token(identifier.getBytes(), null, null, null);
          removeToken(fakeToken, identifier.getUser().getUserName());
          if (LOG.isInfoEnabled()) {
            LOG.info("Removed RMDelegationTokenIdentifier: {}, renewDate: {}.",
                identifier, renewDate);
          }
        }
      }
    }
  }

  private String getChildName(String path) {
    int index = path.lastIndexOf("/");
    if (index == -1){
      return path;
    } else {
      return path.substring(index + 1);
    }
  }

  private String getClusterName(String path) {
    if (path.startsWith(rootPath)) {
      String subPath = path.substring(rootPath.length() + 1);
      int index = subPath.indexOf("/");
      return subPath.substring(0, index);
    }
    return null;
  }
}
