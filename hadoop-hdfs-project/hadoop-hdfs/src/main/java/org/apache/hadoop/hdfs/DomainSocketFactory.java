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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSClient.Conf;

import org.apache.hadoop.net.unix.DomainSocket;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

class DomainSocketFactory {
  public static final Log LOG = LogFactory.getLog(DomainSocketFactory.class);
  private final Conf conf;

  enum PathStatus {
    UNUSABLE,
    SHORT_CIRCUIT_DISABLED,
  }

  /**
   * Information about domain socket paths.
   */
  Cache<String, PathStatus> pathInfo =
      CacheBuilder.newBuilder()
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .build();

  public DomainSocketFactory(Conf conf) {
    this.conf = conf;

    String feature = null;
    if (conf.shortCircuitLocalReads && (!conf.useLegacyBlockReaderLocal)) {
      feature = "The short-circuit local reads feature";
    } else if (conf.domainSocketDataTraffic) {
      feature = "UNIX domain socket data traffic";
    }
    if (feature != null) {
      if (conf.domainSocketPath.isEmpty()) {
        LOG.warn(feature + " is disabled because you have not set " +
            DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY);
      } else if (DomainSocket.getLoadingFailureReason() != null) {
        LOG.warn(feature + " is disabled because " +
              DomainSocket.getLoadingFailureReason());
      } else {
        LOG.debug(feature + "is enabled.");
      }
    }
  }

  /**
   * Create a DomainSocket.
   * 
   * @param addr        The address of the DataNode
   * @param stream      The DFSInputStream the socket will be created for.
   *
   * @return            null if the socket could not be created; the
   *                    socket otherwise.  If there was an error while
   *                    creating the socket, we will add the socket path
   *                    to our list of failed domain socket paths.
   */
  DomainSocket create(InetSocketAddress addr, DFSInputStream stream) {
    // If there is no domain socket path configured, we can't use domain
    // sockets.
    if (conf.domainSocketPath.isEmpty()) return null;
    // If we can't do anything with the domain socket, don't create it.
    if ((conf.domainSocketDataTraffic == false) &&
        ((!conf.shortCircuitLocalReads) || conf.useLegacyBlockReaderLocal)) {
      return null;
    }
    // UNIX domain sockets can only be used to talk to local peers
    if (!DFSClient.isLocalAddress(addr)) return null;
    // If the DomainSocket code is not loaded, we can't create
    // DomainSocket objects.
    if (DomainSocket.getLoadingFailureReason() != null) return null;
    String escapedPath = DomainSocket.
        getEffectivePath(conf.domainSocketPath, addr.getPort());
    PathStatus info = pathInfo.getIfPresent(escapedPath);
    if (info == PathStatus.UNUSABLE) {
      // We tried to connect to this domain socket before, and it was totally
      // unusable.
      return null;
    }
    if ((!conf.domainSocketDataTraffic) &&
        ((info == PathStatus.SHORT_CIRCUIT_DISABLED) || 
            stream.shortCircuitForbidden())) {
      // If we don't want to pass data over domain sockets, and we don't want
      // to pass file descriptors over them either, we have no use for domain
      // sockets.
      return null;
    }
    boolean success = false;
    DomainSocket sock = null;
    try {
      sock = DomainSocket.connect(escapedPath);
      sock.setAttribute(DomainSocket.RECEIVE_TIMEOUT, conf.socketTimeout);
      success = true;
    } catch (IOException e) {
      LOG.warn("error creating DomainSocket", e);
      // fall through
    } finally {
      if (!success) {
        if (sock != null) {
          IOUtils.closeQuietly(sock);
        }
        pathInfo.put(escapedPath, PathStatus.UNUSABLE);
        sock = null;
      }
    }
    return sock;
  }

  public void disableShortCircuitForPath(String path) {
    pathInfo.put(path, PathStatus.SHORT_CIRCUIT_DISABLED);
  }

  public void disableDomainSocketPath(String path) {
    pathInfo.put(path, PathStatus.UNUSABLE);
  }
}
