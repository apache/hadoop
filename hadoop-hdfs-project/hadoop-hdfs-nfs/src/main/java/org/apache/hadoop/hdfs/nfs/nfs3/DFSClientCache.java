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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.security.UserGroupInformation;

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

  private final Configuration config;

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

  DFSClientCache(Configuration config) {
    this(config, DEFAULT_DFS_CLIENT_CACHE_SIZE);
  }

  DFSClientCache(Configuration config, int clientCache) {
    this.config = config;
    this.clientCache = CacheBuilder.newBuilder()
        .maximumSize(clientCache)
        .removalListener(clientRemovealListener())
        .build(clientLoader());

    this.inputstreamCache = CacheBuilder.newBuilder()
        .maximumSize(DEFAULT_DFS_INPUTSTREAM_CACHE_SIZE)
        .expireAfterAccess(DEFAULT_DFS_INPUTSTREAM_CACHE_TTL, TimeUnit.SECONDS)
        .removalListener(inputStreamRemovalListener())
        .build(inputStreamLoader());
  }

  private CacheLoader<String, DFSClient> clientLoader() {
    return new CacheLoader<String, DFSClient>() {
      @Override
      public DFSClient load(String userName) throws Exception {
        UserGroupInformation ugi = UserGroupInformation
            .createRemoteUser(userName);

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

  private RemovalListener<String, DFSClient> clientRemovealListener() {
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
        } catch (IOException e) {
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
        return new FSDataInputStream(dis);
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
