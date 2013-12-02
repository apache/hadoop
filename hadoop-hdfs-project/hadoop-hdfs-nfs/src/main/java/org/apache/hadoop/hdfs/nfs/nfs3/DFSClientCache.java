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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.annotations.VisibleForTesting;
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
   * Cache that maps User id to corresponding DFSClient.
   */
  @VisibleForTesting
  final LoadingCache<String, DFSClient> clientCache;

  final static int DEFAULT_DFS_CLIENT_CACHE_SIZE = 256;

  private final Configuration config;

  DFSClientCache(Configuration config) {
    this(config, DEFAULT_DFS_CLIENT_CACHE_SIZE);
  }

  DFSClientCache(Configuration config, int clientCache) {
    this.config = config;
    this.clientCache = CacheBuilder.newBuilder()
        .maximumSize(clientCache)
        .removalListener(clientRemovealListener())
        .build(clientLoader());
  }

  private CacheLoader<String, DFSClient> clientLoader() {
    return new CacheLoader<String, DFSClient>() {
      @Override
      public DFSClient load(String userName) throws Exception {
        UserGroupInformation ugi = UserGroupInformation
            .createRemoteUser(userName);

        // Guava requires CacheLoader never returns null.
        return ugi.doAs(new PrivilegedExceptionAction<DFSClient>() {
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

  DFSClient get(String userName) {
    DFSClient client = null;
    try {
      client = clientCache.get(userName);
    } catch (ExecutionException e) {
      LOG.error("Failed to create DFSClient for user:" + userName + " Cause:"
          + e);
    }
    return client;
  }
}
