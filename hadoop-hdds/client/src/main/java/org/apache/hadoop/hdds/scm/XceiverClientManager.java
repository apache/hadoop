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
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.PERFORMANCE;

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
  private static final Logger LOG =
      LoggerFactory.getLogger(XceiverClientManager.class);
  //TODO : change this to SCM configuration class
  private final Configuration conf;
  private final Cache<String, XceiverClientSpi> clientCache;
  private final boolean useRatis;
  private X509Certificate caCert;

  private static XceiverClientMetrics metrics;
  private boolean isSecurityEnabled;
  private final boolean topologyAwareRead;
  /**
   * Creates a new XceiverClientManager for non secured ozone cluster.
   * For security enabled ozone cluster, client should use the other constructor
   * with a valid ca certificate in pem string format.
   *
   * @param conf configuration
   */
  public XceiverClientManager(Configuration conf) throws IOException {
    this(conf, OzoneConfiguration.of(conf).getObject(ScmClientConfig.class),
        null);
  }

  public XceiverClientManager(Configuration conf, ScmClientConfig clientConf,
      String caCertPem) throws IOException {
    Preconditions.checkNotNull(clientConf);
    Preconditions.checkNotNull(conf);
    long staleThresholdMs = clientConf.getStaleThreshold(MILLISECONDS);
    this.useRatis = conf.getBoolean(
        ScmConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY,
        ScmConfigKeys.DFS_CONTAINER_RATIS_ENABLED_DEFAULT);
    this.conf = conf;
    this.isSecurityEnabled = OzoneSecurityUtil.isSecurityEnabled(conf);
    if (isSecurityEnabled) {
      Preconditions.checkNotNull(caCertPem);
      try {
        this.caCert = CertificateCodec.getX509Cert(caCertPem);
      } catch (CertificateException ex) {
        throw new SCMSecurityException("Error: Fail to get SCM CA certificate",
            ex);
      }
    }

    this.clientCache = CacheBuilder.newBuilder()
        .expireAfterAccess(staleThresholdMs, MILLISECONDS)
        .maximumSize(clientConf.getMaxSize())
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
    topologyAwareRead = conf.getBoolean(
        OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_KEY,
        OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_DEFAULT);
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
    return acquireClient(pipeline, false);
  }

  /**
   * Acquires a XceiverClientSpi connected to a container for read.
   *
   * If there is already a cached XceiverClientSpi, simply return
   * the cached otherwise create a new one.
   *
   * @param pipeline the container pipeline for the client connection
   * @return XceiverClientSpi connected to a container
   * @throws IOException if a XceiverClientSpi cannot be acquired
   */
  public XceiverClientSpi acquireClientForReadData(Pipeline pipeline)
      throws IOException {
    return acquireClient(pipeline, true);
  }

  private XceiverClientSpi acquireClient(Pipeline pipeline, boolean read)
      throws IOException {
    Preconditions.checkNotNull(pipeline);
    Preconditions.checkArgument(pipeline.getNodes() != null);
    Preconditions.checkArgument(!pipeline.getNodes().isEmpty());

    synchronized (clientCache) {
      XceiverClientSpi info = getClient(pipeline, read);
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
    releaseClient(client, invalidateClient, false);
  }

  /**
   * Releases a read XceiverClientSpi after use.
   *
   * @param client client to release
   * @param invalidateClient if true, invalidates the client in cache
   */
  public void releaseClientForReadData(XceiverClientSpi client,
      boolean invalidateClient) {
    releaseClient(client, invalidateClient, true);
  }

  private void releaseClient(XceiverClientSpi client, boolean invalidateClient,
      boolean read) {
    Preconditions.checkNotNull(client);
    synchronized (clientCache) {
      client.decrementReference();
      if (invalidateClient) {
        Pipeline pipeline = client.getPipeline();
        String key = getPipelineCacheKey(pipeline, read);
        XceiverClientSpi cachedClient = clientCache.getIfPresent(key);
        if (cachedClient == client) {
          clientCache.invalidate(key);
        }
      }
    }
  }

  private XceiverClientSpi getClient(Pipeline pipeline, boolean forRead)
      throws IOException {
    HddsProtos.ReplicationType type = pipeline.getType();
    try {
      // create different client for read different pipeline node based on
      // network topology
      String key = getPipelineCacheKey(pipeline, forRead);
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
              client = XceiverClientRatis.newXceiverClientRatis(pipeline, conf,
                  caCert);
              client.connect();
              break;
            case STAND_ALONE:
              client = new XceiverClientGrpc(pipeline, conf, caCert);
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

  private String getPipelineCacheKey(Pipeline pipeline, boolean forRead) {
    String key = pipeline.getId().getId().toString() + pipeline.getType();
    if (topologyAwareRead && forRead) {
      try {
        key += pipeline.getClosestNode().getHostName();
      } catch (IOException e) {
        LOG.error("Failed to get closest node to create pipeline cache key:" +
            e.getMessage());
      }
    }
    return key;
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

  public Function<ByteBuffer, ByteString> byteBufferToByteStringConversion(){
    return ByteStringConversion.createByteBufferConversion(conf);
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

  /**
   * Configuration for HDDS client.
   */
  @ConfigGroup(prefix = "scm.container.client")
  public static class ScmClientConfig {

    private int maxSize;
    private long staleThreshold;
    private int maxOutstandingRequests;

    public long getStaleThreshold(TimeUnit unit) {
      return unit.convert(staleThreshold, MILLISECONDS);
    }

    @Config(key = "idle.threshold",
        type = ConfigType.TIME, timeUnit = MILLISECONDS,
        defaultValue = "10s",
        tags = { OZONE, PERFORMANCE },
        description =
            "In the standalone pipelines, the SCM clients use netty to "
            + " communicate with the container. It also uses connection pooling"
            + " to reduce client side overheads. This allows a connection to"
            + " stay idle for a while before the connection is closed."
    )
    public void setStaleThreshold(long staleThreshold) {
      this.staleThreshold = staleThreshold;
    }

    public int getMaxSize() {
      return maxSize;
    }

    @Config(key = "max.size",
        defaultValue = "256",
        tags = { OZONE, PERFORMANCE },
        description =
            "Controls the maximum number of connections that are cached via"
            + " client connection pooling. If the number of connections"
            + " exceed this count, then the oldest idle connection is evicted."
    )
    public void setMaxSize(int maxSize) {
      this.maxSize = maxSize;
    }

    public int getMaxOutstandingRequests() {
      return maxOutstandingRequests;
    }

    @Config(key = "max.outstanding.requests",
        defaultValue = "100",
        tags = { OZONE, PERFORMANCE },
        description =
            "Controls the maximum number of outstanding async requests that can"
            + " be handled by the Standalone as well as Ratis client."
    )
    public void setMaxOutstandingRequests(int maxOutstandingRequests) {
      this.maxOutstandingRequests = maxOutstandingRequests;
    }
  }

}
