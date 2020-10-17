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
package org.apache.hadoop.hdfs.shortcircuit;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf.ShortCircuitConf;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.util.PerformanceAdvisory;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.cache.Cache;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DomainSocketFactory {
  private static final Logger LOG = LoggerFactory.getLogger(
      DomainSocketFactory.class);

  public enum PathState {
    UNUSABLE(false, false),
    SHORT_CIRCUIT_DISABLED(true, false),
    VALID(true, true);

    PathState(boolean usableForDataTransfer, boolean usableForShortCircuit) {
      this.usableForDataTransfer = usableForDataTransfer;
      this.usableForShortCircuit = usableForShortCircuit;
    }

    public boolean getUsableForDataTransfer() {
      return usableForDataTransfer;
    }

    public boolean getUsableForShortCircuit() {
      return usableForShortCircuit;
    }

    private final boolean usableForDataTransfer;
    private final boolean usableForShortCircuit;
  }

  public static class PathInfo {
    private final static PathInfo NOT_CONFIGURED =
          new PathInfo("", PathState.UNUSABLE);

    final private String path;
    final private PathState state;

    PathInfo(String path, PathState state) {
      this.path = path;
      this.state = state;
    }

    public String getPath() {
      return path;
    }

    public PathState getPathState() {
      return state;
    }

    @Override
    public String toString() {
      return "PathInfo{path=" + path + ", state=" + state + "}";
    }
  }

  /**
   * Information about domain socket paths.
   */
  private final long pathExpireSeconds;
  private final Cache<String, PathState> pathMap;

  public DomainSocketFactory(ShortCircuitConf conf) {
    final String feature;
    if (conf.isShortCircuitLocalReads() && (!conf.isUseLegacyBlockReaderLocal())) {
      feature = "The short-circuit local reads feature";
    } else if (conf.isDomainSocketDataTraffic()) {
      feature = "UNIX domain socket data traffic";
    } else {
      feature = null;
    }

    if (feature == null) {
      PerformanceAdvisory.LOG.debug(
          "Both short-circuit local reads and UNIX domain socket are disabled.");
    } else {
      if (conf.getDomainSocketPath().isEmpty()) {
        throw new HadoopIllegalArgumentException(feature + " is enabled but "
            + HdfsClientConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY + " is not set.");
      } else if (DomainSocket.getLoadingFailureReason() != null) {
        LOG.warn(feature + " cannot be used because "
            + DomainSocket.getLoadingFailureReason());
      } else {
        LOG.debug(feature + " is enabled.");
      }
    }

    pathExpireSeconds = conf.getDomainSocketDisableIntervalSeconds();
    pathMap = CacheBuilder.newBuilder()
        .expireAfterWrite(pathExpireSeconds, TimeUnit.SECONDS).build();
  }

  /**
   * Get information about a domain socket path.
   *
   * @param addr         The inet address to use.
   * @param conf         The client configuration.
   *
   * @return             Information about the socket path.
   */
  public PathInfo getPathInfo(InetSocketAddress addr, ShortCircuitConf conf)
      throws IOException {
    // If there is no domain socket path configured, we can't use domain
    // sockets.
    if (conf.getDomainSocketPath().isEmpty()) return PathInfo.NOT_CONFIGURED;
    // If we can't do anything with the domain socket, don't create it.
    if (!conf.isDomainSocketDataTraffic() &&
        (!conf.isShortCircuitLocalReads() || conf.isUseLegacyBlockReaderLocal())) {
      return PathInfo.NOT_CONFIGURED;
    }
    // If the DomainSocket code is not loaded, we can't create
    // DomainSocket objects.
    if (DomainSocket.getLoadingFailureReason() != null) {
      return PathInfo.NOT_CONFIGURED;
    }
    // UNIX domain sockets can only be used to talk to local peers
    if (!DFSUtilClient.isLocalAddress(addr)) return PathInfo.NOT_CONFIGURED;
    String escapedPath = DomainSocket.getEffectivePath(
        conf.getDomainSocketPath(), addr.getPort());
    PathState status = pathMap.getIfPresent(escapedPath);
    if (status == null) {
      return new PathInfo(escapedPath, PathState.VALID);
    } else {
      return new PathInfo(escapedPath, status);
    }
  }

  public DomainSocket createSocket(PathInfo info, int socketTimeout) {
    Preconditions.checkArgument(info.getPathState() != PathState.UNUSABLE);
    boolean success = false;
    DomainSocket sock = null;
    try {
      sock = DomainSocket.connect(info.getPath());
      sock.setAttribute(DomainSocket.RECEIVE_TIMEOUT, socketTimeout);
      success = true;
    } catch (IOException e) {
      LOG.warn("error creating DomainSocket", e);
      // fall through
    } finally {
      if (!success) {
        if (sock != null) {
          IOUtils.closeQuietly(sock);
        }
        pathMap.put(info.getPath(), PathState.UNUSABLE);
        sock = null;
      }
    }
    return sock;
  }

  public void disableShortCircuitForPath(String path) {
    pathMap.put(path, PathState.SHORT_CIRCUIT_DISABLED);
  }

  public void disableDomainSocketPath(String path) {
    pathMap.put(path, PathState.UNUSABLE);
  }

  @VisibleForTesting
  public void clearPathMap() {
    pathMap.invalidateAll();
  }

  public long getPathExpireSeconds() {
    return pathExpireSeconds;
  }
}
