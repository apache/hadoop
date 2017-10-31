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

package org.apache.hadoop.yarn.client.api.impl;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.NMProxy;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class to manage container manager proxies
 */
@LimitedPrivate({ "MapReduce", "YARN" })
public class ContainerManagementProtocolProxy {
  static final Logger LOG =
          LoggerFactory.getLogger(ContainerManagementProtocolProxy.class);

  private final int maxConnectedNMs;
  private final Map<String, ContainerManagementProtocolProxyData> cmProxy;
  private final Configuration conf;
  private final YarnRPC rpc;
  private NMTokenCache nmTokenCache;
  
  public ContainerManagementProtocolProxy(Configuration conf) {
    this(conf, NMTokenCache.getSingleton());
  }

  public ContainerManagementProtocolProxy(Configuration conf,
      NMTokenCache nmTokenCache) {
    this.conf = new Configuration(conf);
    this.nmTokenCache = nmTokenCache;

    maxConnectedNMs =
        conf.getInt(YarnConfiguration.NM_CLIENT_MAX_NM_PROXIES,
            YarnConfiguration.DEFAULT_NM_CLIENT_MAX_NM_PROXIES);
    if (maxConnectedNMs < 0) {
      throw new YarnRuntimeException(
          YarnConfiguration.NM_CLIENT_MAX_NM_PROXIES
              + " (" + maxConnectedNMs + ") can not be less than 0.");
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(YarnConfiguration.NM_CLIENT_MAX_NM_PROXIES + " : " +
          maxConnectedNMs);
    }

    if (maxConnectedNMs > 0) {
      cmProxy =
          new LinkedHashMap<String, ContainerManagementProtocolProxyData>();
    } else {
      cmProxy = Collections.emptyMap();
      // Connections are not being cached so ensure connections close quickly
      // to avoid creating thousands of RPC client threads on large clusters.
      this.conf.setInt(
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
          0);
    }
    rpc = YarnRPC.create(conf);
  }
  
  public synchronized ContainerManagementProtocolProxyData getProxy(
      String containerManagerBindAddr, ContainerId containerId)
      throws InvalidToken {
    
    // This get call will update the map which is working as LRU cache.
    ContainerManagementProtocolProxyData proxy =
        cmProxy.get(containerManagerBindAddr);

    while (proxy != null
        && !proxy.token.getIdentifier().equals(
            nmTokenCache.getToken(containerManagerBindAddr).getIdentifier())) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Refreshing proxy as NMToken got updated for node : "
            + containerManagerBindAddr);
      }
      // Token is updated. check if anyone has already tried closing it.
      if (!proxy.scheduledForClose) {
        // try closing the proxy. Here if someone is already using it
        // then we might not close it. In which case we will wait.
        removeProxy(proxy);
      } else {
        try {
          this.wait();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      if (proxy.activeCallers < 0) {
        proxy = cmProxy.get(containerManagerBindAddr);
      }
    }
    
    if (proxy == null) {
      proxy =
          new ContainerManagementProtocolProxyData(rpc, containerManagerBindAddr,
              containerId, nmTokenCache.getToken(containerManagerBindAddr));
      if (maxConnectedNMs > 0) {
        addProxyToCache(containerManagerBindAddr, proxy);
      }
    }
    // This is to track active users of this proxy.
    proxy.activeCallers++;
    updateLRUCache(containerManagerBindAddr);
    
    return proxy;
  }

  private void addProxyToCache(String containerManagerBindAddr,
      ContainerManagementProtocolProxyData proxy) {
    while (cmProxy.size() >= maxConnectedNMs) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cleaning up the proxy cache, size=" + cmProxy.size()
            + " max=" + maxConnectedNMs);
      }
      boolean removedProxy = false;
      for (ContainerManagementProtocolProxyData otherProxy : cmProxy.values()) {
        removedProxy = removeProxy(otherProxy);
        if (removedProxy) {
          break;
        }
      }
      if (!removedProxy) {
        // all of the proxies are currently in use and already scheduled
        // for removal, so we need to wait until at least one of them closes
        try {
          this.wait();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    if (maxConnectedNMs > 0) {
      cmProxy.put(containerManagerBindAddr, proxy);
    }
  }
  
  private void updateLRUCache(String containerManagerBindAddr) {
    if (maxConnectedNMs > 0) {
      ContainerManagementProtocolProxyData proxy =
          cmProxy.remove(containerManagerBindAddr);
      cmProxy.put(containerManagerBindAddr, proxy);
    }
  }

  public synchronized void mayBeCloseProxy(
      ContainerManagementProtocolProxyData proxy) {
    tryCloseProxy(proxy);
  }

  private boolean tryCloseProxy(
      ContainerManagementProtocolProxyData proxy) {
    proxy.activeCallers--;
    if (proxy.scheduledForClose && proxy.activeCallers < 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Closing proxy : " + proxy.containerManagerBindAddr);
      }
      cmProxy.remove(proxy.containerManagerBindAddr);
      try {
        rpc.stopProxy(proxy.getContainerManagementProtocol(), conf);
      } finally {
        this.notifyAll();
      }
      return true;
    }
    return false;
  }

  private synchronized boolean removeProxy(
      ContainerManagementProtocolProxyData proxy) {
    if (!proxy.scheduledForClose) {
      proxy.scheduledForClose = true;
      return tryCloseProxy(proxy);
    }
    return false;
  }
  
  public synchronized void stopAllProxies() {
    List<String> nodeIds = new ArrayList<String>();
    nodeIds.addAll(this.cmProxy.keySet());
    for (String nodeId : nodeIds) {
      ContainerManagementProtocolProxyData proxy = cmProxy.get(nodeId);
      // Explicitly reducing the proxy count to allow stopping proxy.
      proxy.activeCallers = 0;
      try {
        removeProxy(proxy);
      } catch (Throwable t) {
        LOG.error("Error closing connection", t);
      }
    }
    cmProxy.clear();
  }
  
  public class ContainerManagementProtocolProxyData {
    private final String containerManagerBindAddr;
    private final ContainerManagementProtocol proxy;
    private int activeCallers;
    private boolean scheduledForClose;
    private final Token token;
    
    @Private
    @VisibleForTesting
    public ContainerManagementProtocolProxyData(YarnRPC rpc,
        String containerManagerBindAddr,
        ContainerId containerId, Token token) throws InvalidToken {
      this.containerManagerBindAddr = containerManagerBindAddr;
      ;
      this.activeCallers = 0;
      this.scheduledForClose = false;
      this.token = token;
      this.proxy = newProxy(rpc, containerManagerBindAddr, containerId, token);
    }

    @Private
    @VisibleForTesting
    protected ContainerManagementProtocol newProxy(final YarnRPC rpc,
        String containerManagerBindAddr, ContainerId containerId, Token token)
        throws InvalidToken {

      if (token == null) {
        throw new InvalidToken("No NMToken sent for "
            + containerManagerBindAddr);
      }
      
      final InetSocketAddress cmAddr =
          NetUtils.createSocketAddr(containerManagerBindAddr);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Opening proxy : " + containerManagerBindAddr);
      }
      // the user in createRemoteUser in this context has to be ContainerID
      UserGroupInformation user =
          UserGroupInformation.createRemoteUser(containerId
              .getApplicationAttemptId().toString());

      org.apache.hadoop.security.token.Token<NMTokenIdentifier> nmToken =
          ConverterUtils.convertFromYarn(token, cmAddr);
      user.addToken(nmToken);

      return NMProxy.createNMProxy(conf, ContainerManagementProtocol.class,
        user, rpc, cmAddr);
    }

    public ContainerManagementProtocol getContainerManagementProtocol() {
      return proxy;
    }
  }
  
}
