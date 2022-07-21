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

import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
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
import java.io.IOException;

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
      RouterDelegationTokenSecretManager secretManager) {
    super(secretManager);
    this.zkManager = zkcuratorManager;
    this.rootPath = conf.get("yarn.resourcemanager.zk-state-store.rootpath", "/federation");
  }

  @Override
  public void start() throws Exception {
    CuratorCache curatorCache = CuratorCache.build(zkManager.getCurator(), rootPath);
    curatorCache.start();
    CuratorCacheListener listener = CuratorCacheListener.builder()
        .forInitialized(() -> LOG.info("Cache initialized."))
        .build();
    curatorCache.listenable().addListener(listener);
    curatorCache.stream().forEach(data -> processSubCluster(data.getPath()));
  }

  private void processSubCluster(String path) {
    LOG.info("Monitor SubCluster path: {}.", path);
    rootPath = path + "/" + ROOT_ZNODE_NAME + "/" + RM_DT_SECRET_MANAGER_ROOT;
    try {
      zkManager.createRootDirRecursively(rootPath);
      monitorMasterKey(rootPath + "/" + RM_DT_MASTER_KEYS_ROOT_ZNODE_NAME);
      monitorDelegationToken(rootPath + "/" + RM_DELEGATION_TOKENS_ROOT_ZNODE_NAME);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void monitorDelegationToken(String path) throws Exception {
    if (!zkManager.exists(path)) {
      zkManager.create(path);
    }
    LOG.info("Monitor DelegationToken path: {}.", path);
    CuratorCache curatorCache = CuratorCache.build(zkManager.getCurator(), path);
    curatorCache.start();
    CuratorCacheListener listener =
        CuratorCacheListener.builder().
        forCreatesAndChanges((oldNode, node) ->
            processDTNode(node.getPath(), node.getData(), true)).
        forDeletes(node ->
            processDTNode(node.getPath(), node.getData(), true)).build();

    curatorCache.listenable().addListener(listener);
    curatorCache.stream().forEach(data -> processDTNode(data.getPath(), data.getData(), true));
  }

  private void monitorMasterKey(String path) throws Exception {
    if (!zkManager.exists(path)) {
      zkManager.create(path);
    }
    LOG.info("Monitor MasterKey path: {}.", path);
    CuratorCache masterKeyCache = CuratorCache.build(zkManager.getCurator(), path);
    masterKeyCache.start();
    CuratorCacheListener listener =
        CuratorCacheListener.builder().
        forCreatesAndChanges((oldNode, node) ->
        processKeyNode(node.getPath(), node.getData(), true)).build();
    masterKeyCache.listenable().addListener(listener);
    masterKeyCache.stream().forEach(data ->
        processDTNode(data.getPath(), data.getData(), true));
  }

  private void processKeyNode(String path, byte[] data, boolean isUpdate) {
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
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void processDTNode(String path, byte[] data, boolean isUpdate)  {
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
          Token<RMDelegationTokenIdentifier> fakeToken =
              new Token<>(identifier.getBytes(), null, null, null);
          removeToken(fakeToken, identifier.getUser().getUserName());
          if (LOG.isInfoEnabled()) {
            LOG.info("Removed RMDelegationTokenIdentifier: {}, renewDate: {}.",
                identifier, renewDate);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
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
