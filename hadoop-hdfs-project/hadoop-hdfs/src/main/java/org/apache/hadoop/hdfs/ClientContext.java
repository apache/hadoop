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
package org.apache.hadoop.hdfs;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.hdfs.shortcircuit.DomainSocketFactory;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitCache;
import org.apache.hadoop.hdfs.util.ByteArrayManager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;

/**
 * ClientContext contains context information for a client.
 * 
 * This allows us to share caches such as the socket cache across
 * DFSClient instances.
 */
@InterfaceAudience.Private
public class ClientContext {
  private static final Log LOG = LogFactory.getLog(ClientContext.class);

  /**
   * Global map of context names to caches contexts.
   */
  private final static HashMap<String, ClientContext> CACHES =
      new HashMap<String, ClientContext>();

  /**
   * Name of context.
   */
  private final String name;

  /**
   * String representation of the configuration.
   */
  private final String confString;

  /**
   * Caches short-circuit file descriptors, mmap regions.
   */
  private final ShortCircuitCache shortCircuitCache;

  /**
   * Caches TCP and UNIX domain sockets for reuse.
   */
  private final PeerCache peerCache;

  /**
   * Stores information about socket paths.
   */
  private final DomainSocketFactory domainSocketFactory;

  /**
   * Caches key Providers for the DFSClient
   */
  private final KeyProviderCache keyProviderCache;
  /**
   * True if we should use the legacy BlockReaderLocal.
   */
  private final boolean useLegacyBlockReaderLocal;

  /**
   * True if the legacy BlockReaderLocal is disabled.
   *
   * The legacy block reader local gets disabled completely whenever there is an
   * error or miscommunication.  The new block reader local code handles this
   * case more gracefully inside DomainSocketFactory.
   */
  private volatile boolean disableLegacyBlockReaderLocal = false;

  /** Creating byte[] for {@link DFSOutputStream}. */
  private final ByteArrayManager byteArrayManager;  

  /**
   * Whether or not we complained about a DFSClient fetching a CacheContext that
   * didn't match its config values yet.
   */
  private boolean printedConfWarning = false;

  private ClientContext(String name, Conf conf) {
    this.name = name;
    this.confString = confAsString(conf);
    this.shortCircuitCache = new ShortCircuitCache(
        conf.shortCircuitStreamsCacheSize,
        conf.shortCircuitStreamsCacheExpiryMs,
        conf.shortCircuitMmapCacheSize,
        conf.shortCircuitMmapCacheExpiryMs,
        conf.shortCircuitMmapCacheRetryTimeout,
        conf.shortCircuitCacheStaleThresholdMs,
        conf.shortCircuitSharedMemoryWatcherInterruptCheckMs);
    this.peerCache =
          new PeerCache(conf.socketCacheCapacity, conf.socketCacheExpiry);
    this.keyProviderCache = new KeyProviderCache(conf.keyProviderCacheExpiryMs);
    this.useLegacyBlockReaderLocal = conf.useLegacyBlockReaderLocal;
    this.domainSocketFactory = new DomainSocketFactory(conf);

    this.byteArrayManager = ByteArrayManager.newInstance(conf.writeByteArrayManagerConf);
  }

  public static String confAsString(Conf conf) {
    StringBuilder builder = new StringBuilder();
    builder.append("shortCircuitStreamsCacheSize = ").
      append(conf.shortCircuitStreamsCacheSize).
      append(", shortCircuitStreamsCacheExpiryMs = ").
      append(conf.shortCircuitStreamsCacheExpiryMs).
      append(", shortCircuitMmapCacheSize = ").
      append(conf.shortCircuitMmapCacheSize).
      append(", shortCircuitMmapCacheExpiryMs = ").
      append(conf.shortCircuitMmapCacheExpiryMs).
      append(", shortCircuitMmapCacheRetryTimeout = ").
      append(conf.shortCircuitMmapCacheRetryTimeout).
      append(", shortCircuitCacheStaleThresholdMs = ").
      append(conf.shortCircuitCacheStaleThresholdMs).
      append(", socketCacheCapacity = ").
      append(conf.socketCacheCapacity).
      append(", socketCacheExpiry = ").
      append(conf.socketCacheExpiry).
      append(", shortCircuitLocalReads = ").
      append(conf.shortCircuitLocalReads).
      append(", useLegacyBlockReaderLocal = ").
      append(conf.useLegacyBlockReaderLocal).
      append(", domainSocketDataTraffic = ").
      append(conf.domainSocketDataTraffic).
      append(", shortCircuitSharedMemoryWatcherInterruptCheckMs = ").
      append(conf.shortCircuitSharedMemoryWatcherInterruptCheckMs).
      append(", keyProviderCacheExpiryMs = ").
      append(conf.keyProviderCacheExpiryMs);

    return builder.toString();
  }

  public static ClientContext get(String name, Conf conf) {
    ClientContext context;
    synchronized(ClientContext.class) {
      context = CACHES.get(name);
      if (context == null) {
        context = new ClientContext(name, conf);
        CACHES.put(name, context);
      } else {
        context.printConfWarningIfNeeded(conf);
      }
    }
    return context;
  }

  /**
   * Get a client context, from a Configuration object.
   *
   * This method is less efficient than the version which takes a DFSClient#Conf
   * object, and should be mostly used by tests.
   */
  @VisibleForTesting
  public static ClientContext getFromConf(Configuration conf) {
    return get(conf.get(DFSConfigKeys.DFS_CLIENT_CONTEXT,
        DFSConfigKeys.DFS_CLIENT_CONTEXT_DEFAULT),
            new DFSClient.Conf(conf));
  }

  private void printConfWarningIfNeeded(Conf conf) {
    String existing = this.getConfString();
    String requested = confAsString(conf);
    if (!existing.equals(requested)) {
      if (!printedConfWarning) {
        printedConfWarning = true;
        LOG.warn("Existing client context '" + name + "' does not match " +
            "requested configuration.  Existing: " + existing + 
            ", Requested: " + requested);
      }
    }
  }

  public String getConfString() {
    return confString;
  }

  public ShortCircuitCache getShortCircuitCache() {
    return shortCircuitCache;
  }

  public PeerCache getPeerCache() {
    return peerCache;
  }

  public KeyProviderCache getKeyProviderCache() {
    return keyProviderCache;
  }

  public boolean getUseLegacyBlockReaderLocal() {
    return useLegacyBlockReaderLocal;
  }

  public boolean getDisableLegacyBlockReaderLocal() {
    return disableLegacyBlockReaderLocal;
  }

  public void setDisableLegacyBlockReaderLocal() {
    disableLegacyBlockReaderLocal = true;
  }

  public DomainSocketFactory getDomainSocketFactory() {
    return domainSocketFactory;
  }

  public ByteArrayManager getByteArrayManager() {
    return byteArrayManager;
  }
}
