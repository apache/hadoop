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

package org.apache.hadoop.hdfs.server.federation.router.security.token;

import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager;
import org.apache.hadoop.util.Time;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Zookeeper based router delegation token store implementation.
 */
public class ZKDelegationTokenSecretManagerImpl extends
    ZKDelegationTokenSecretManager<AbstractDelegationTokenIdentifier> {

  public static final String ZK_DTSM_ROUTER_TOKEN_SYNC_INTERVAL =
      ZK_CONF_PREFIX + "router.token.sync.interval";
  public static final int ZK_DTSM_ROUTER_TOKEN_SYNC_INTERVAL_DEFAULT = 5;

  private static final Logger LOG =
      LoggerFactory.getLogger(ZKDelegationTokenSecretManagerImpl.class);

  private Configuration conf;

  private final ScheduledExecutorService scheduler =
      Executors.newSingleThreadScheduledExecutor();

  // Local cache of delegation tokens, used for deprecating tokens from
  // currentTokenMap
  private final Set<AbstractDelegationTokenIdentifier> localTokenCache =
      new HashSet<>();

  private final String TOKEN_PATH = "/" + zkClient.getNamespace()
      + ZK_DTSM_TOKENS_ROOT;
  // The flag used to issue an extra check before deletion
  // Since cancel token and token remover thread use the same
  // API here and one router could have a token that is renewed
  // by another router, thus token remover should always check ZK
  // to confirm whether it has been renewed or not
  private ThreadLocal<Boolean> checkAgainstZkBeforeDeletion =
      new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
          return true;
        }
      };

  public ZKDelegationTokenSecretManagerImpl(Configuration conf) {
    super(conf);
    this.conf = conf;
    try {
      startThreads();
    } catch (IOException e) {
      LOG.error("Error starting threads for zkDelegationTokens", e);
    }
    LOG.info("Zookeeper delegation token secret manager instantiated");
  }

  @Override
  public void startThreads() throws IOException {
    super.startThreads();
    // start token cache related work when watcher is disabled
    if (!isTokenWatcherEnabled()) {
      LOG.info("Watcher for tokens is disabled in this secret manager");
      try {
        // By default set this variable
        checkAgainstZkBeforeDeletion.set(true);
        // Ensure the token root path exists
        if (zkClient.checkExists().forPath(ZK_DTSM_TOKENS_ROOT) == null) {
          zkClient.create().creatingParentsIfNeeded()
              .withMode(CreateMode.PERSISTENT)
              .forPath(ZK_DTSM_TOKENS_ROOT);
        }

        LOG.info("Start loading token cache");
        long start = Time.now();
        rebuildTokenCache(true);
        LOG.info("Loaded token cache in {} milliseconds", Time.now() - start);

        int syncInterval = conf.getInt(ZK_DTSM_ROUTER_TOKEN_SYNC_INTERVAL,
            ZK_DTSM_ROUTER_TOKEN_SYNC_INTERVAL_DEFAULT);
        scheduler.scheduleAtFixedRate(new Runnable() {
          @Override
          public void run() {
            try {
              rebuildTokenCache(false);
            } catch (Exception e) {
              // ignore
            }
          }
        }, syncInterval, syncInterval, TimeUnit.SECONDS);
      } catch (Exception e) {
        LOG.error("Error rebuilding local cache for zkDelegationTokens ", e);
      }
    }
  }

  @Override
  public void stopThreads() {
    super.stopThreads();
    scheduler.shutdown();
  }

  @Override
  public DelegationTokenIdentifier createIdentifier() {
    return new DelegationTokenIdentifier();
  }

  /**
   * This function will rebuild local token cache from zk storage.
   * It is first called when the secret manager is initialized and
   * then regularly at a configured interval.
   *
   * @param initial whether this is called during initialization
   * @throws IOException
   */
  private void rebuildTokenCache(boolean initial) throws IOException {
    localTokenCache.clear();
    // Use bare zookeeper client to get all children since curator will
    // wrap the same API with a sorting process. This is time consuming given
    // millions of tokens
    List<String> zkTokens;
    try {
      zkTokens = getZooKeeperClient().getChildren(TOKEN_PATH, false);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Tokens cannot be fetched from path "
          + TOKEN_PATH, e);
    }
    byte[] data;
    for (String tokenPath : zkTokens) {
      try {
        data = zkClient.getData().forPath(
            ZK_DTSM_TOKENS_ROOT + "/" + tokenPath);
      } catch (KeeperException.NoNodeException e) {
        LOG.debug("No node in path [" + tokenPath + "]");
        continue;
      } catch (Exception ex) {
        throw new IOException(ex);
      }
      // Store data to currentTokenMap
      AbstractDelegationTokenIdentifier ident = processTokenAddOrUpdate(data);
      // Store data to localTokenCache for sync
      localTokenCache.add(ident);
    }
    if (!initial) {
      // Sync zkTokens with local cache, specifically
      // 1) add/update tokens to local cache from zk, which is done through
      //    processTokenAddOrUpdate above
      // 2) remove tokens in local cache but not in zk anymore
      for (AbstractDelegationTokenIdentifier ident : currentTokens.keySet()) {
        if (!localTokenCache.contains(ident)) {
            currentTokens.remove(ident);
        }
      }
    }
    syncTokenOwnerStats();
  }

  @Override
  public AbstractDelegationTokenIdentifier cancelToken(
      Token<AbstractDelegationTokenIdentifier> token, String canceller)
      throws IOException {
    checkAgainstZkBeforeDeletion.set(false);
    AbstractDelegationTokenIdentifier ident = super.cancelToken(token,
        canceller);
    checkAgainstZkBeforeDeletion.set(true);
    return ident;
  }

  @Override
  protected void removeStoredToken(AbstractDelegationTokenIdentifier ident)
      throws IOException {
    super.removeStoredToken(ident, checkAgainstZkBeforeDeletion.get());
  }

  @Override
  protected void addOrUpdateToken(AbstractDelegationTokenIdentifier ident,
      DelegationTokenInformation info, boolean isUpdate) throws Exception {
    // Store the data in local memory first
    currentTokens.put(ident, info);
    super.addOrUpdateToken(ident, info, isUpdate);
  }

  private ZooKeeper getZooKeeperClient() throws IOException {
    // get zookeeper client
    ZooKeeper zookeeper = null;
    try {
      zookeeper = zkClient.getZookeeperClient().getZooKeeper();
    } catch (Exception e) {
      LOG.info("Cannot get zookeeper client ", e);
    } finally {
      if (zookeeper == null) {
        throw new IOException("Zookeeper client is null");
      }
    }
    return zookeeper;
  }
}
