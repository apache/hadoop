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
package org.apache.hadoop.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import javax.net.SocketFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolClientSideTranslatorPB;
import org.apache.hadoop.ha.protocolPB.ZKFCProtocolClientSideTranslatorPB;
import org.apache.hadoop.net.NetUtils;

import com.google.common.collect.Maps;

/**
 * Represents a target of the client side HA administration commands.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class HAServiceTarget {

  private static final String HOST_SUBST_KEY = "host";
  private static final String PORT_SUBST_KEY = "port";
  private static final String ADDRESS_SUBST_KEY = "address";

  /**
   * @return the IPC address of the target node.
   */
  public abstract InetSocketAddress getAddress();

  /**
   * Returns an optional separate RPC server address for health checks at the
   * target node.  If defined, then this address is used by the health monitor
   * for the {@link HAServiceProtocol#monitorHealth()} and
   * {@link HAServiceProtocol#getServiceStatus()} calls.  This can be useful for
   * separating out these calls onto separate RPC handlers to protect against
   * resource exhaustion in the main RPC handler pool.  If null (which is the
   * default implementation), then all RPC calls go to the address defined by
   * {@link #getAddress()}.
   *
   * @return IPC address of the lifeline RPC server on the target node, or null
   *     if no lifeline RPC server is used
   */
  public InetSocketAddress getHealthMonitorAddress() {
    return null;
  }

  /**
   * @return the IPC address of the ZKFC on the target node
   */
  public abstract InetSocketAddress getZKFCAddress();

  /**
   * @return a Fencer implementation configured for this target node
   */
  public abstract NodeFencer getFencer();
  
  /**
   * @throws BadFencingConfigurationException if the fencing configuration
   * appears to be invalid. This is divorced from the above
   * {@link #getFencer()} method so that the configuration can be checked
   * during the pre-flight phase of failover.
   */
  public abstract void checkFencingConfigured()
      throws BadFencingConfigurationException;
  
  /**
   * @return a proxy to connect to the target HA Service.
   */
  public HAServiceProtocol getProxy(Configuration conf, int timeoutMs)
      throws IOException {
    return getProxyForAddress(conf, timeoutMs, getAddress());
  }

  /**
   * Returns a proxy to connect to the target HA service for health monitoring.
   * If {@link #getHealthMonitorAddress()} is implemented to return a non-null
   * address, then this proxy will connect to that address.  Otherwise, the
   * returned proxy defaults to using {@link #getAddress()}, which means this
   * method's behavior is identical to {@link #getProxy(Configuration, int)}.
   *
   * @param conf Configuration
   * @param timeoutMs timeout in milliseconds
   * @return a proxy to connect to the target HA service for health monitoring
   * @throws IOException if there is an error
   */
  public HAServiceProtocol getHealthMonitorProxy(Configuration conf,
      int timeoutMs) throws IOException {
    InetSocketAddress addr = getHealthMonitorAddress();
    if (addr == null) {
      addr = getAddress();
    }
    return getProxyForAddress(conf, timeoutMs, addr);
  }

  private HAServiceProtocol getProxyForAddress(Configuration conf,
      int timeoutMs, InetSocketAddress addr) throws IOException {
    Configuration confCopy = new Configuration(conf);
    // Lower the timeout so we quickly fail to connect
    confCopy.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 1);
    SocketFactory factory = NetUtils.getDefaultSocketFactory(confCopy);
    return new HAServiceProtocolClientSideTranslatorPB(
        addr,
        confCopy, factory, timeoutMs);
  }

  /**
   * @return a proxy to the ZKFC which is associated with this HA service.
   */
  public ZKFCProtocol getZKFCProxy(Configuration conf, int timeoutMs)
      throws IOException {
    Configuration confCopy = new Configuration(conf);
    // Lower the timeout so we quickly fail to connect
    confCopy.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 1);
    SocketFactory factory = NetUtils.getDefaultSocketFactory(confCopy);
    return new ZKFCProtocolClientSideTranslatorPB(
        getZKFCAddress(),
        confCopy, factory, timeoutMs);
  }
  
  public final Map<String, String> getFencingParameters() {
    Map<String, String> ret = Maps.newHashMap();
    addFencingParameters(ret);
    return ret;
  }
  
  /**
   * Hook to allow subclasses to add any parameters they would like to
   * expose to fencing implementations/scripts. Fencing methods are free
   * to use this map as they see fit -- notably, the shell script
   * implementation takes each entry, prepends 'target_', substitutes
   * '_' for '.', and adds it to the environment of the script.
   *
   * Subclass implementations should be sure to delegate to the superclass
   * implementation as well as adding their own keys.
   *
   * @param ret map which can be mutated to pass parameters to the fencer
   */
  protected void addFencingParameters(Map<String, String> ret) {
    ret.put(ADDRESS_SUBST_KEY, String.valueOf(getAddress()));
    ret.put(HOST_SUBST_KEY, getAddress().getHostName());
    ret.put(PORT_SUBST_KEY, String.valueOf(getAddress().getPort()));
  }

  /**
   * @return true if auto failover should be considered enabled
   */
  public boolean isAutoFailoverEnabled() {
    return false;
  }

  /**
   * @return true if this target supports the Observer state, false otherwise.
   */
  public boolean supportObserver() {
    return false;
  }
}
