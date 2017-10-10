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
import java.net.URI;
import java.nio.file.FileSystemException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfigKeys;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfiguration;
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
 * A cache saves DFSClient objects for different users.
 */
class DFSClientCache {
  private static final Log LOG = LogFactory.getLog(DFSClientCache.class);
  /**
   * Cache that maps User id to the corresponding DFSClient.
   */
  private final LoadingCache<DfsClientKey, DFSClient> clientCache;

  final static int DEFAULT_DFS_CLIENT_CACHE_SIZE = 256;

  /**
   * Cache that maps <DFSClient, inode path, nnid> to the corresponding
   * FSDataInputStream.
   */
  private final LoadingCache<DFSInputStreamCacheKey,
                          FSDataInputStream> inputstreamCache;

  /**
   * Time to live for a DFSClient (in seconds).
   */
  final static int DEFAULT_DFS_INPUTSTREAM_CACHE_SIZE = 1024;
  final static int DEFAULT_DFS_INPUTSTREAM_CACHE_TTL = 10 * 60;

  private final NfsConfiguration config;
  private final HashMap<Integer, URI> namenodeUriMap;

  private static final class DFSInputStreamCacheKey {
    private final String userId;
    private final String inodePath;
    private final int namenodeId;

    private DFSInputStreamCacheKey(String userId, String inodePath,
                                   int namenodeId) {
      super();
      this.userId = userId;
      this.inodePath = inodePath;
      this.namenodeId = namenodeId;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof DFSInputStreamCacheKey) {
        DFSInputStreamCacheKey k = (DFSInputStreamCacheKey) obj;
        return userId.equals(k.userId) &&
               inodePath.equals(k.inodePath) &&
               (namenodeId == k.namenodeId);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(userId, inodePath, namenodeId);
    }
  }

  private static final class DfsClientKey {
    private final String userName;
    private final int namenodeId;

    private DfsClientKey(String userName, int namenodeId) {
      this.userName = userName;
      this.namenodeId = namenodeId;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof DfsClientKey) {
        DfsClientKey k = (DfsClientKey) obj;
        return userName.equals(k.userName) &&
            (namenodeId == k.namenodeId);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(userName, namenodeId);
    }
  }

  DFSClientCache(NfsConfiguration config) throws IOException {
    this(config, DEFAULT_DFS_CLIENT_CACHE_SIZE);
  }

  DFSClientCache(NfsConfiguration config, int clientCache) throws IOException {
    this.config = config;
    namenodeUriMap = new HashMap<>();
    prepareAddressMap();

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

  private void prepareAddressMap() throws IOException {
    FileSystem fs = FileSystem.get(config);
    String[] exportsPath =
        config.getStrings(NfsConfigKeys.DFS_NFS_EXPORT_POINT_KEY,
            NfsConfigKeys.DFS_NFS_EXPORT_POINT_DEFAULT);
    for (String exportPath : exportsPath) {
      URI exportURI = Nfs3Utils.getResolvedURI(fs, exportPath);
      int namenodeId = Nfs3Utils.getNamenodeId(config, exportURI);
      URI value = namenodeUriMap.get(namenodeId);
      // if a unique nnid, add it to the map
      if (value == null) {
        LOG.info("Added export:" + exportPath + " FileSystem URI:" + exportURI
              + " with namenodeId:" + namenodeId);
        namenodeUriMap.put(namenodeId, exportURI);
      } else {
        // if the nnid already exists, it better be the for the same namenode
        String msg = String.format("FS:%s, Namenode ID collision for path:%s "
                + "nnid:%s uri being added:%s existing uri:%s", fs.getScheme(),
            exportPath, namenodeId, exportURI, value);
        LOG.error(msg);
        throw new FileSystemException(msg);
      }
    }
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

  @VisibleForTesting
  public LoadingCache<DfsClientKey, DFSClient> getClientCache() {
    return clientCache;
  }

  /**
   * Close all DFSClient instances in the Cache.
   * @param onlyAutomatic only close those that are marked for automatic closing
   */
  synchronized void closeAll(boolean onlyAutomatic) throws IOException {
    List<IOException> exceptions = new ArrayList<IOException>();

    ConcurrentMap<DfsClientKey, DFSClient> map = clientCache.asMap();

    for (Entry<DfsClientKey, DFSClient> item : map.entrySet()) {
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

  private CacheLoader<DfsClientKey, DFSClient> clientLoader() {
    return new CacheLoader<DfsClientKey, DFSClient>() {
      @Override
      public DFSClient load(final DfsClientKey key) throws Exception {
        UserGroupInformation ugi = getUserGroupInformation(
            key.userName, UserGroupInformation.getCurrentUser());

        // Guava requires CacheLoader never returns null.
        return ugi.doAs(new PrivilegedExceptionAction<DFSClient>() {
          @Override
          public DFSClient run() throws IOException {
            URI namenodeURI = namenodeUriMap.get(key.namenodeId);
            if (namenodeURI == null) {
              throw new IOException("No namenode URI found for user:" +
                  key.userName + " namenodeId:" + key.namenodeId);
            }
            return new DFSClient(namenodeURI, config);
          }
        });
      }
    };
  }

  /**
   * This method uses the currentUser, and real user to create a proxy.
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

  private RemovalListener<DfsClientKey, DFSClient> clientRemovalListener() {
    return new RemovalListener<DfsClientKey, DFSClient>() {
      @Override
      public void onRemoval(
          RemovalNotification<DfsClientKey, DFSClient> notification) {
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

  private RemovalListener
      <DFSInputStreamCacheKey, FSDataInputStream> inputStreamRemovalListener() {
    return new RemovalListener
        <DFSClientCache.DFSInputStreamCacheKey, FSDataInputStream>() {

      @Override
      public void onRemoval(
          RemovalNotification<DFSInputStreamCacheKey, FSDataInputStream>
                                                            notification) {
        try {
          notification.getValue().close();
        } catch (IOException ignored) {
        }
      }
    };
  }

  private CacheLoader<DFSInputStreamCacheKey, FSDataInputStream>
                                                      inputStreamLoader() {
    return new CacheLoader<DFSInputStreamCacheKey, FSDataInputStream>() {

      @Override
      public FSDataInputStream
                    load(DFSInputStreamCacheKey key) throws Exception {
        DFSClient client = getDfsClient(key.userId, key.namenodeId);
        DFSInputStream dis = client.open(key.inodePath);
        return client.createWrappedInputStream(dis);
      }
    };
  }

  DFSClient getDfsClient(String userName, int namenodeId) {
    DFSClient client = null;
    try {
      client = clientCache.get(new DfsClientKey(userName, namenodeId));
    } catch (ExecutionException e) {
      LOG.error("Failed to create DFSClient for user:" + userName + " Cause:"
          + e);
    }
    return client;
  }

  FSDataInputStream getDfsInputStream(String userName, String inodePath,
                                      int namenodeId) {
    DFSInputStreamCacheKey k =
        new DFSInputStreamCacheKey(userName, inodePath, namenodeId);
    FSDataInputStream s = null;
    try {
      s = inputstreamCache.get(k);
    } catch (ExecutionException e) {
      LOG.warn("Failed to create DFSInputStream for user:" + userName
          + " Cause:" + e);
    }
    return s;
  }

  public void invalidateDfsInputStream(String userName, String inodePath,
                                       int namenodeId) {
    DFSInputStreamCacheKey k =
        new DFSInputStreamCacheKey(userName, inodePath, namenodeId);
    inputstreamCache.invalidate(k);
  }
}
