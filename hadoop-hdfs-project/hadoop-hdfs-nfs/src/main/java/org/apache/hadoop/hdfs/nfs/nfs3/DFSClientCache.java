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
package org.apache.hadoop.hdfs.nfs.nfs3;

import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * A cache saves DFSClient objects for different users
 */
class DFSClientCache {
  private static final Log LOG = LogFactory.getLog(DFSClientCache.class);
  /**
   * Cache that maps User id to the corresponding DFSClient.
   */
  @VisibleForTesting
  final LoadingCache<String, DFSClient> clientCache;

  final static int DEFAULT_DFS_CLIENT_CACHE_SIZE = 256;

  /**
   * Cache that maps <DFSClient, inode path> to the corresponding
   * FSDataInputStream.
   */
  final LoadingCache<DFSInputStreamCaheKey, FSDataInputStream> inputstreamCache;

  /**
   * Time to live for a DFSClient (in seconds)
   */
  final static int DEFAULT_DFS_INPUTSTREAM_CACHE_SIZE = 1024;
  final static int DEFAULT_DFS_INPUTSTREAM_CACHE_TTL = 10 * 60;

  private final NfsConfiguration config;

  private static class DFSInputStreamCaheKey {
    final String userId;
    final String inodePath;

    private DFSInputStreamCaheKey(String userId, String inodePath) {
      super();
      this.userId = userId;
      this.inodePath = inodePath;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof DFSInputStreamCaheKey) {
        DFSInputStreamCaheKey k = (DFSInputStreamCaheKey) obj;
        return userId.equals(k.userId) && inodePath.equals(k.inodePath);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(userId, inodePath);
    }
  }

  DFSClientCache(NfsConfiguration config) {
    this(config, DEFAULT_DFS_CLIENT_CACHE_SIZE);
  }
  
  DFSClientCache(NfsConfiguration config, int clientCache) {
    this.config = config;
    this.clientCache = CacheBuilder.newBuilder()
        .maximumSize(clientCache)
        .removalListener(clientRemovalListener())
        .build(clientLoader());

    this.inputstreamCache = CacheBuilder.newBuilder()
        .maximumSize(DEFAULT_DFS_INPUTSTREAM_CACHE_SIZE)
        .expireAfterAccess(DEFAULT_DFS_INPUTSTREAM_CACHE_TTL, TimeUnit.SECONDS)
        .removalListener(inputStreamRemovalListener())
        .build(inputStreamLoader());
    
    ShutdownHookManager.get().addShutdownHook(new CacheFinalizer(),
        SHUTDOWN_HOOK_PRIORITY);
  }

  /**
   * Priority of the FileSystem shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 10;
  
  private class CacheFinalizer implements Runnable {
    @Override
    public synchronized void run() {
      try {
        closeAll(true);
      } catch (IOException e) {
        LOG.info("DFSClientCache.closeAll() threw an exception:\n", e);
      }
    }
  }
  
  /**
   * Close all DFSClient instances in the Cache.
   * @param onlyAutomatic only close those that are marked for automatic closing
   */
  synchronized void closeAll(boolean onlyAutomatic) throws IOException {
    List<IOException> exceptions = new ArrayList<IOException>();

    ConcurrentMap<String, DFSClient> map = clientCache.asMap();

    for (Entry<String, DFSClient> item : map.entrySet()) {
      final DFSClient client = item.getValue();
      if (client != null) {
        try {
          client.close();
        } catch (IOException ioe) {
          exceptions.add(ioe);
        }
      }
    }

    if (!exceptions.isEmpty()) {
      throw MultipleIOException.createIOException(exceptions);
    }
  }
  
  private CacheLoader<String, DFSClient> clientLoader() {
    return new CacheLoader<String, DFSClient>() {
      @Override
      public DFSClient load(String userName) throws Exception {
        UserGroupInformation ugi = getUserGroupInformation(
                userName,
                UserGroupInformation.getCurrentUser());

        // Guava requires CacheLoader never returns null.
        return ugi.doAs(new PrivilegedExceptionAction<DFSClient>() {
          @Override
          public DFSClient run() throws IOException {
            return new DFSClient(NameNode.getAddress(config), config);
          }
        });
      }
    };
  }

  /**
   * This method uses the currentUser, and real user to create a proxy
   * @param effectiveUser The user who is being proxied by the real user
   * @param realUser The actual user who does the command
   * @return Proxy UserGroupInformation
   * @throws IOException If proxying fails
   */
  UserGroupInformation getUserGroupInformation(
          String effectiveUser,
          UserGroupInformation realUser)
          throws IOException {
    Preconditions.checkNotNull(effectiveUser);
    Preconditions.checkNotNull(realUser);
    realUser.checkTGTAndReloginFromKeytab();

    UserGroupInformation ugi =
            UserGroupInformation.createProxyUser(effectiveUser, realUser);
    if (LOG.isDebugEnabled()){
      LOG.debug(String.format("Created ugi:" +
              " %s for username: %s", ugi, effectiveUser));
    }
    return ugi;
  }

  private RemovalListener<String, DFSClient> clientRemovalListener() {
    return new RemovalListener<String, DFSClient>() {
      @Override
      public void onRemoval(RemovalNotification<String, DFSClient> notification) {
        DFSClient client = notification.getValue();
        try {
          client.close();
        } catch (IOException e) {
          LOG.warn(String.format(
              "IOException when closing the DFSClient(%s), cause: %s", client,
              e));
        }
      }
    };
  }

  private RemovalListener<DFSInputStreamCaheKey, FSDataInputStream> inputStreamRemovalListener() {
    return new RemovalListener<DFSClientCache.DFSInputStreamCaheKey, FSDataInputStream>() {

      @Override
      public void onRemoval(
          RemovalNotification<DFSInputStreamCaheKey, FSDataInputStream> notification) {
        try {
          notification.getValue().close();
        } catch (IOException ignored) {
        }
      }
    };
  }

  private CacheLoader<DFSInputStreamCaheKey, FSDataInputStream> inputStreamLoader() {
    return new CacheLoader<DFSInputStreamCaheKey, FSDataInputStream>() {

      @Override
      public FSDataInputStream load(DFSInputStreamCaheKey key) throws Exception {
        DFSClient client = getDfsClient(key.userId);
        DFSInputStream dis = client.open(key.inodePath);
        return client.createWrappedInputStream(dis);
      }
    };
  }

  DFSClient getDfsClient(String userName) {
    DFSClient client = null;
    try {
      client = clientCache.get(userName);
    } catch (ExecutionException e) {
      LOG.error("Failed to create DFSClient for user:" + userName + " Cause:"
          + e);
    }
    return client;
  }

  FSDataInputStream getDfsInputStream(String userName, String inodePath) {
    DFSInputStreamCaheKey k = new DFSInputStreamCaheKey(userName, inodePath);
    FSDataInputStream s = null;
    try {
      s = inputstreamCache.get(k);
    } catch (ExecutionException e) {
      LOG.warn("Failed to create DFSInputStream for user:" + userName
          + " Cause:" + e);
    }
    return s;
  }

  public void invalidateDfsInputStream(String userName, String inodePath) {
    DFSInputStreamCaheKey k = new DFSInputStreamCaheKey(userName, inodePath);
    inputstreamCache.invalidate(k);
  }
}
