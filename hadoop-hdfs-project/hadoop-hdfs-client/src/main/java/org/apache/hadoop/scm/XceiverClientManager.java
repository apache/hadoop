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

package org.apache.hadoop.scm;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;

import static org.apache.hadoop.scm.ScmConfigKeys.SCM_CONTAINER_CLIENT_STALE_THRESHOLD_DEFAULT;
import static org.apache.hadoop.scm.ScmConfigKeys.SCM_CONTAINER_CLIENT_STALE_THRESHOLD_KEY;

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
public class XceiverClientManager {

  //TODO : change this to SCM configuration class
  private final Configuration conf;
  private Cache<String, XceiverClientWithAccessInfo> openClient;
  private final long staleThresholdMs;

  /**
   * Creates a new XceiverClientManager.
   *
   * @param conf configuration
   */
  public XceiverClientManager(Configuration conf) {
    Preconditions.checkNotNull(conf);
    this.staleThresholdMs = conf.getTimeDuration(
        SCM_CONTAINER_CLIENT_STALE_THRESHOLD_KEY,
        SCM_CONTAINER_CLIENT_STALE_THRESHOLD_DEFAULT, TimeUnit.MILLISECONDS);
    this.conf = conf;
    this.openClient = CacheBuilder.newBuilder()
        .expireAfterAccess(this.staleThresholdMs, TimeUnit.MILLISECONDS)
        .removalListener(
            new RemovalListener<String, XceiverClientWithAccessInfo>() {
            @Override
            public void onRemoval(
                RemovalNotification<String, XceiverClientWithAccessInfo>
                  removalNotification) {
              // If the reference count is not 0, this xceiver client should not
              // be evicted, add it back to the cache.
              XceiverClientWithAccessInfo info = removalNotification.getValue();
              if (info.hasRefence()) {
                synchronized (XceiverClientManager.this.openClient) {
                  XceiverClientManager.this
                      .openClient.put(removalNotification.getKey(), info);
                }
              }
            }
          }).build();
  }

  /**
   * Acquires a XceiverClient connected to a container capable of storing the
   * specified key.
   *
   * If there is already a cached XceiverClient, simply return the cached
   * otherwise create a new one.
   *
   * @param pipeline the container pipeline for the client connection
   * @return XceiverClient connected to a container
   * @throws IOException if an XceiverClient cannot be acquired
   */
  public XceiverClientSpi acquireClient(Pipeline pipeline) throws IOException {
    Preconditions.checkNotNull(pipeline);
    Preconditions.checkArgument(pipeline.getMachines() != null);
    Preconditions.checkArgument(!pipeline.getMachines().isEmpty());
    String containerName = pipeline.getContainerName();
    XceiverClientWithAccessInfo info = openClient.getIfPresent(containerName);

    if (info != null) {
      // we do have this connection, add reference and return
      info.incrementReference();
      return info.getXceiverClient();
    } else {
      // connection not found, create new, add reference and return
      XceiverClientSpi xceiverClient = new XceiverClient(pipeline, conf);
      try {
        xceiverClient.connect();
      } catch (Exception e) {
        throw new IOException("Exception connecting XceiverClient.", e);
      }
      info = new XceiverClientWithAccessInfo(xceiverClient);
      info.incrementReference();
      synchronized (openClient) {
        openClient.put(containerName, info);
      }
      return xceiverClient;
    }
  }

  /**
   * Releases an XceiverClient after use.
   *
   * @param xceiverClient client to release
   */
  public void releaseClient(XceiverClientSpi xceiverClient) {
    Preconditions.checkNotNull(xceiverClient);
    String containerName = xceiverClient.getPipeline().getContainerName();
    XceiverClientWithAccessInfo info;
    synchronized (openClient) {
      info = openClient.getIfPresent(containerName);
    }
    Preconditions.checkNotNull(info);
    info.decrementReference();
  }

  /**
   * A helper class for caching and cleaning XceiverClient. Three parameters:
   * - the actual XceiverClient object
   * - a time stamp representing the most recent access (acquire or release)
   * - a reference count, +1 when acquire, -1 when release
   */
  private static class XceiverClientWithAccessInfo {
    final private XceiverClientSpi xceiverClient;
    final private AtomicInteger referenceCount;

    XceiverClientWithAccessInfo(XceiverClientSpi xceiverClient) {
      this.xceiverClient = xceiverClient;
      this.referenceCount = new AtomicInteger(0);
    }

    void incrementReference() {
      this.referenceCount.incrementAndGet();
    }

    void decrementReference() {
      this.referenceCount.decrementAndGet();
    }

    boolean hasRefence() {
      return this.referenceCount.get() != 0;
    }

    XceiverClientSpi getXceiverClient() {
      return xceiverClient;
    }
  }
}
