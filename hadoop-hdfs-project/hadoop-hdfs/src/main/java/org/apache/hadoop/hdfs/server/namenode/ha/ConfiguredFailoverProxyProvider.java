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
package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * A FailoverProxyProvider implementation which allows one to configure two URIs
 * to connect to during fail-over. The first configured address is tried first,
 * and on a fail-over event the other address is tried.
 */
public class ConfiguredFailoverProxyProvider implements FailoverProxyProvider,
    Configurable {
  
  public static final String CONFIGURED_NAMENODE_ADDRESSES
      = "dfs.ha.namenode.addresses";
  
  private static final Log LOG =
      LogFactory.getLog(ConfiguredFailoverProxyProvider.class);
  
  private Configuration conf;
  private int currentProxyIndex = 0;
  private List<AddressRpcProxyPair> proxies = new ArrayList<AddressRpcProxyPair>();
  private UserGroupInformation ugi;

  @Override
  public Class<?> getInterface() {
    return ClientProtocol.class;
  }

  /**
   * Lazily initialize the RPC proxy object.
   */
  @Override
  public synchronized Object getProxy() {
    AddressRpcProxyPair current = proxies.get(currentProxyIndex);
    if (current.namenode == null) {
      try {
        current.namenode = DFSUtil.createRPCNamenode(current.address, conf, ugi);
      } catch (IOException e) {
        LOG.error("Failed to create RPC proxy to NameNode", e);
        throw new RuntimeException(e);
      }
    }
    return current.namenode;
  }

  @Override
  public synchronized void performFailover(Object currentProxy) {
    currentProxyIndex = (currentProxyIndex + 1) % proxies.size();
  }

  @Override
  public synchronized Configuration getConf() {
    return conf;
  }

  @Override
  public synchronized void setConf(Configuration conf) {
    this.conf = conf;
    try {
      ugi = UserGroupInformation.getCurrentUser();
      
      Collection<String> addresses = conf.getTrimmedStringCollection(
          CONFIGURED_NAMENODE_ADDRESSES);
      if (addresses == null || addresses.size() == 0) {
        throw new RuntimeException(this.getClass().getSimpleName() +
            " is configured but " + CONFIGURED_NAMENODE_ADDRESSES +
            " is not set.");
      }
      for (String address : addresses) {
        proxies.add(new AddressRpcProxyPair(
            NameNode.getAddress(new URI(address).getAuthority())));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (URISyntaxException e) {
      throw new RuntimeException("Malformed URI set in " +
          CONFIGURED_NAMENODE_ADDRESSES, e);
    }
  }

  /**
   * A little pair object to store the address and connected RPC proxy object to
   * an NN. Note that {@link AddressRpcProxyPair#namenode} may be null.
   */
  private static class AddressRpcProxyPair {
    public InetSocketAddress address;
    public ClientProtocol namenode;
    
    public AddressRpcProxyPair(InetSocketAddress address) {
      this.address = address;
    }
  }

  /**
   * Close all the proxy objects which have been opened over the lifetime of
   * this proxy provider.
   */
  @Override
  public synchronized void close() throws IOException {
    for (AddressRpcProxyPair proxy : proxies) {
      if (proxy.namenode != null) {
        RPC.stopProxy(proxy.namenode);
      }
    }
  }
}
