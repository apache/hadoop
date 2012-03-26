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

import javax.net.SocketFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolClientSideTranslatorPB;
import org.apache.hadoop.net.NetUtils;

/**
 * Represents a target of the client side HA administration commands.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class HAServiceTarget {

  /**
   * @return the IPC address of the target node.
   */
  public abstract InetSocketAddress getAddress();

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
    Configuration confCopy = new Configuration(conf);
    // Lower the timeout so we quickly fail to connect
    confCopy.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 1);
    SocketFactory factory = NetUtils.getDefaultSocketFactory(confCopy);
    return new HAServiceProtocolClientSideTranslatorPB(
        getAddress(),
        confCopy, factory, timeoutMs);
  }

  /**
   * @return a proxy to connect to the target HA Service.
   */
  public final HAServiceProtocol getProxy() throws IOException {
    return getProxy(new Configuration(), 0); // default conf, timeout
  }
}
