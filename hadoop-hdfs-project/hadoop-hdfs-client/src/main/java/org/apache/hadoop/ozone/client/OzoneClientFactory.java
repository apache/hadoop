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

package org.apache.hadoop.ozone.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Proxy;

import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_CLIENT_PROTOCOL;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_CLIENT_PROTOCOL_REST;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_CLIENT_PROTOCOL_RPC;

/**
 * Factory class to create different types of OzoneClients.
 * Based on <code>ozone.client.protocol</code>, it decides which
 * protocol to use for the communication.
 * Default value is
 * <code>org.apache.hadoop.ozone.client.rpc.RpcClient</code>.<br>
 * OzoneClientFactory constructs a proxy using
 * {@link OzoneClientInvocationHandler}
 * and creates OzoneClient instance with it.
 * {@link OzoneClientInvocationHandler} dispatches the call to
 * underlying {@link ClientProtocol} implementation.
 */
public final class OzoneClientFactory {

  private enum ClientType {
    RPC, REST
  }

  /**
   * Private constructor, class is not meant to be initialized.
   */
  private OzoneClientFactory(){}

  private static final Logger LOG = LoggerFactory.getLogger(
      OzoneClientFactory.class);

  private static Configuration configuration;

  /**
   * Returns an OzoneClient which will use protocol defined through
   * <code>ozone.client.protocol</code> to perform client operations.
   * @return OzoneClient
   * @throws IOException
   */
  public static OzoneClient getClient() throws IOException {
    return getClient(null);
  }

  /**
   * Returns an OzoneClient which will use RPC protocol to perform
   * client operations.
   * @return OzoneClient
   * @throws IOException
   */
  public static OzoneClient getRpcClient() throws IOException {
    return getClient(ClientType.RPC);
  }

  /**
   * Returns an OzoneClient which will use REST protocol to perform
   * client operations.
   * @return OzoneClient
   * @throws IOException
   */
  public static OzoneClient getRestClient() throws IOException {
    return getClient(ClientType.REST);
  }

  /**
   * Returns OzoneClient with protocol type set base on ClientType.
   * @param clientType
   * @return OzoneClient
   * @throws IOException
   */
  private static OzoneClient getClient(ClientType clientType)
      throws IOException {
    OzoneClientInvocationHandler clientHandler =
        new OzoneClientInvocationHandler(getProtocolClass(clientType));
    ClientProtocol proxy = (ClientProtocol) Proxy.newProxyInstance(
        OzoneClientInvocationHandler.class.getClassLoader(),
        new Class<?>[]{ClientProtocol.class}, clientHandler);
    return new OzoneClient(proxy);
  }

  /**
   * Returns the configuration if it's already set, else creates a new
   * {@link OzoneConfiguration} and returns it.
   *
   * @return Configuration
   */
  private static synchronized Configuration getConfiguration() {
    if(configuration == null) {
      setConfiguration(new OzoneConfiguration());
    }
    return configuration;
  }

  /**
   * Based on the clientType, client protocol instance is created.
   * If clientType is null, <code>ozone.client.protocol</code> property
   * will be used to decide the protocol to be used.
   * @param clientType type of client protocol to be created
   * @return ClientProtocol implementation
   * @throws IOException
   */
  private static ClientProtocol getProtocolClass(ClientType clientType)
      throws IOException {
    Class<? extends ClientProtocol> protocolClass = null;
    if(clientType != null) {
      switch (clientType) {
      case RPC:
        protocolClass = OZONE_CLIENT_PROTOCOL_RPC;
        break;
      case REST:
        protocolClass = OZONE_CLIENT_PROTOCOL_REST;
        break;
      default:
        LOG.warn("Invalid ClientProtocol type, falling back to RPC.");
        protocolClass = OZONE_CLIENT_PROTOCOL_RPC;
        break;
      }
    } else {
      protocolClass = (Class<ClientProtocol>)
          getConfiguration().getClass(
              OZONE_CLIENT_PROTOCOL, OZONE_CLIENT_PROTOCOL_RPC);
    }
    try {
      Constructor<? extends ClientProtocol> ctor =
          protocolClass.getConstructor(Configuration.class);
      return ctor.newInstance(getConfiguration());
    } catch (Exception e) {
      final String message = "Couldn't create protocol " + protocolClass;
      LOG.warn(message, e);
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException(message, e);
      }
    }
  }

  /**
   * Sets the configuration, which will be used while creating OzoneClient.
   *
   * @param conf
   */
  public static void setConfiguration(Configuration conf) {
    configuration = conf;
  }

}
