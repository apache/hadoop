/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .SCM_CONTAINER_CLIENT_MAX_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .SCM_CONTAINER_CLIENT_MAX_SIZE_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .SCM_CONTAINER_CLIENT_STALE_THRESHOLD_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .SCM_CONTAINER_CLIENT_STALE_THRESHOLD_KEY;

/**
 * XceiverClientManager is responsible for the lifecycle of XceiverClient
 * instances.  Callers use this class to acquire an XceiverClient instance
 * connected to the desired container pipeline.  When done, the caller also uses
 * this class to release the previously acquired XceiverClient instance.
 *
 *
 * This class caches connection to container for reuse purpose, such that
 * accessing same container frequently will be through the same connection
 * without reestablishing connection. But the connection will be closed if
 * not being used for a period of time.
 */
public class XceiverClientManager implements Closeable {

  //TODO : change this to SCM configuration class
  private final Configuration conf;
  private final Cache<String, XceiverClientSpi> clientCache;
  private final boolean useRatis;

  private static XceiverClientMetrics metrics;
  private boolean isSecurityEnabled;
  /**
   * Creates a new XceiverClientManager.
   *
   * @param conf configuration
   */
  public XceiverClientManager(Configuration conf) {
    Preconditions.checkNotNull(conf);
    int maxSize = conf.getInt(SCM_CONTAINER_CLIENT_MAX_SIZE_KEY,
        SCM_CONTAINER_CLIENT_MAX_SIZE_DEFAULT);
    long staleThresholdMs = conf.getTimeDuration(
        SCM_CONTAINER_CLIENT_STALE_THRESHOLD_KEY,
        SCM_CONTAINER_CLIENT_STALE_THRESHOLD_DEFAULT, TimeUnit.MILLISECONDS);
    this.useRatis = conf.getBoolean(
        ScmConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY,
        ScmConfigKeys.DFS_CONTAINER_RATIS_ENABLED_DEFAULT);
    this.conf = conf;
    this.isSecurityEnabled = OzoneSecurityUtil.isSecurityEnabled(conf);
    this.clientCache = CacheBuilder.newBuilder()
        .expireAfterAccess(staleThresholdMs, TimeUnit.MILLISECONDS)
        .maximumSize(maxSize)
        .removalListener(
            new RemovalListener<String, XceiverClientSpi>() {
            @Override
            public void onRemoval(
                RemovalNotification<String, XceiverClientSpi>
                  removalNotification) {
              synchronized (clientCache) {
                // Mark the entry as evicted
                XceiverClientSpi info = removalNotification.getValue();
                info.setEvicted();
              }
            }
          }).build();
  }

  @VisibleForTesting
  public Cache<String, XceiverClientSpi> getClientCache() {
    return clientCache;
  }

  /**
   * Acquires a XceiverClientSpi connected to a container capable of
   * storing the specified key.
   *
   * If there is already a cached XceiverClientSpi, simply return
   * the cached otherwise create a new one.
   *
   * @param pipeline the container pipeline for the client connection
   * @return XceiverClientSpi connected to a container
   * @throws IOException if a XceiverClientSpi cannot be acquired
   */
  public XceiverClientSpi acquireClient(Pipeline pipeline)
      throws IOException {
    Preconditions.checkNotNull(pipeline);
    Preconditions.checkArgument(pipeline.getNodes() != null);
    Preconditions.checkArgument(!pipeline.getNodes().isEmpty());

    synchronized (clientCache) {
      XceiverClientSpi info = getClient(pipeline);
      info.incrementReference();
      return info;
    }
  }

  /**
   * Releases a XceiverClientSpi after use.
   *
   * @param client client to release
   * @param invalidateClient if true, invalidates the client in cache
   */
  public void releaseClient(XceiverClientSpi client, boolean invalidateClient) {
    Preconditions.checkNotNull(client);
    synchronized (clientCache) {
      client.decrementReference();
      if (invalidateClient) {
        Pipeline pipeline = client.getPipeline();
        String key = pipeline.getId().getId().toString() + pipeline.getType();
        XceiverClientSpi cachedClient = clientCache.getIfPresent(key);
        if (cachedClient == client) {
          clientCache.invalidate(key);
        }
      }
    }
  }

  private XceiverClientSpi getClient(Pipeline pipeline)
      throws IOException {
    HddsProtos.ReplicationType type = pipeline.getType();
    try {
      String key = pipeline.getId().getId().toString() + type;
      // Append user short name to key to prevent a different user
      // from using same instance of xceiverClient.
      key = isSecurityEnabled ?
          key + UserGroupInformation.getCurrentUser().getShortUserName() : key;
      return clientCache.get(key, new Callable<XceiverClientSpi>() {
        @Override
          public XceiverClientSpi call() throws Exception {
            XceiverClientSpi client = null;
            switch (type) {
            case RATIS:
              client = XceiverClientRatis.newXceiverClientRatis(pipeline, conf);
              client.connect();
              break;
            case STAND_ALONE:
              client = new XceiverClientGrpc(pipeline, conf);
              break;
            case CHAINED:
            default:
              throw new IOException("not implemented" + pipeline.getType());
            }
            return client;
          }
        });
    } catch (Exception e) {
      throw new IOException(
          "Exception getting XceiverClient: " + e.toString(), e);
    }
  }

  /**
   * Close and remove all the cached clients.
   */
  @Override
  public void close() {
    //closing is done through RemovalListener
    clientCache.invalidateAll();
    clientCache.cleanUp();

    if (metrics != null) {
      metrics.unRegister();
    }
  }

  /**
   * Tells us if Ratis is enabled for this cluster.
   * @return True if Ratis is enabled.
   */
  public boolean isUseRatis() {
    return useRatis;
  }

  /**
   * Returns hard coded 3 as replication factor.
   * @return 3
   */
  public  HddsProtos.ReplicationFactor getFactor() {
    if(isUseRatis()) {
      return HddsProtos.ReplicationFactor.THREE;
    }
    return HddsProtos.ReplicationFactor.ONE;
  }

  /**
   * Returns the default replication type.
   * @return Ratis or Standalone
   */
  public HddsProtos.ReplicationType getType() {
    // TODO : Fix me and make Ratis default before release.
    // TODO: Remove this as replication factor and type are pipeline properties
    if(isUseRatis()) {
      return HddsProtos.ReplicationType.RATIS;
    }
    return HddsProtos.ReplicationType.STAND_ALONE;
  }

  /**
   * Get xceiver client metric.
   */
  public synchronized static XceiverClientMetrics getXceiverClientMetrics() {
    if (metrics == null) {
      metrics = XceiverClientMetrics.create();
    }

    return metrics;
  }
}
