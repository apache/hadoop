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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.rest.RestClient;
import org.apache.hadoop.ozone.client.rpc.RpcClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_PROTOCOL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY;

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

  private static final Logger LOG = LoggerFactory.getLogger(
      OzoneClientFactory.class);

  /**
   * Private constructor, class is not meant to be initialized.
   */
  private OzoneClientFactory(){}


  /**
   * Constructs and return an OzoneClient with default configuration.
   *
   * @return OzoneClient
   *
   * @throws IOException
   */
  public static OzoneClient getClient() throws IOException {
    LOG.info("Creating OzoneClient with default configuration.");
    return getClient(new OzoneConfiguration());
  }

  /**
   * Constructs and return an OzoneClient based on the configuration object.
   * Protocol type is decided by <code>ozone.client.protocol</code>.
   *
   * @param config
   *        Configuration to be used for OzoneClient creation
   *
   * @return OzoneClient
   *
   * @throws IOException
   */
  public static OzoneClient getClient(Configuration config)
      throws IOException {
    Preconditions.checkNotNull(config);
    Class<? extends ClientProtocol> clazz = (Class<? extends ClientProtocol>)
        config.getClass(OZONE_CLIENT_PROTOCOL, RpcClient.class);
    return getClient(getClientProtocol(clazz, config), config);
  }

  /**
   * Returns an OzoneClient which will use RPC protocol.
   *
   * @param omHost
   *        hostname of OzoneManager to connect.
   *
   * @return OzoneClient
   *
   * @throws IOException
   */
  public static OzoneClient getRpcClient(String omHost)
      throws IOException {
    Configuration config = new OzoneConfiguration();
    int port = OmUtils.getOmRpcPort(config);
    return getRpcClient(omHost, port, config);
  }

  /**
   * Returns an OzoneClient which will use RPC protocol.
   *
   * @param omHost
   *        hostname of OzoneManager to connect.
   *
   * @param omRpcPort
   *        RPC port of OzoneManager.
   *
   * @return OzoneClient
   *
   * @throws IOException
   */
  public static OzoneClient getRpcClient(String omHost, Integer omRpcPort)
      throws IOException {
    return getRpcClient(omHost, omRpcPort, new OzoneConfiguration());
  }

  /**
   * Returns an OzoneClient which will use RPC protocol.
   *
   * @param omHost
   *        hostname of OzoneManager to connect.
   *
   * @param omRpcPort
   *        RPC port of OzoneManager.
   *
   * @param config
   *        Configuration to be used for OzoneClient creation
   *
   * @return OzoneClient
   *
   * @throws IOException
   */
  public static OzoneClient getRpcClient(String omHost, Integer omRpcPort,
                                         Configuration config)
      throws IOException {
    Preconditions.checkNotNull(omHost);
    Preconditions.checkNotNull(omRpcPort);
    Preconditions.checkNotNull(config);
    config.set(OZONE_OM_ADDRESS_KEY, omHost + ":" + omRpcPort);
    return getRpcClient(config);
  }

  /**
   * Returns an OzoneClient which will use RPC protocol.
   *
   * @param config
   *        used for OzoneClient creation
   *
   * @return OzoneClient
   *
   * @throws IOException
   */
  public static OzoneClient getRpcClient(Configuration config)
      throws IOException {
    Preconditions.checkNotNull(config);
    return getClient(getClientProtocol(RpcClient.class, config),
        config);
  }

  /**
   * Returns an OzoneClient which will use REST protocol.
   *
   * @param omHost
   *        hostname of OzoneManager to connect.
   *
   * @return OzoneClient
   *
   * @throws IOException
   */
  public static OzoneClient getRestClient(String omHost)
      throws IOException {
    Configuration config = new OzoneConfiguration();
    int port = OmUtils.getOmRestPort(config);
    return getRestClient(omHost, port, config);
  }

  /**
   * Returns an OzoneClient which will use REST protocol.
   *
   * @param omHost
   *        hostname of OzoneManager to connect.
   *
   * @param omHttpPort
   *        HTTP port of OzoneManager.
   *
   * @return OzoneClient
   *
   * @throws IOException
   */
  public static OzoneClient getRestClient(String omHost, Integer omHttpPort)
      throws IOException {
    return getRestClient(omHost, omHttpPort, new OzoneConfiguration());
  }

  /**
   * Returns an OzoneClient which will use REST protocol.
   *
   * @param omHost
   *        hostname of OzoneManager to connect.
   *
   * @param omHttpPort
   *        HTTP port of OzoneManager.
   *
   * @param config
   *        Configuration to be used for OzoneClient creation
   *
   * @return OzoneClient
   *
   * @throws IOException
   */
  public static OzoneClient getRestClient(String omHost, Integer omHttpPort,
                                          Configuration config)
      throws IOException {
    Preconditions.checkNotNull(omHost);
    Preconditions.checkNotNull(omHttpPort);
    Preconditions.checkNotNull(config);
    config.set(OZONE_OM_HTTP_ADDRESS_KEY, omHost + ":" + omHttpPort);
    return getRestClient(config);
  }

  /**
   * Returns an OzoneClient which will use REST protocol.
   *
   * @param config
   *        Configuration to be used for OzoneClient creation
   *
   * @return OzoneClient
   *
   * @throws IOException
   */
  public static OzoneClient getRestClient(Configuration config)
      throws IOException {
    Preconditions.checkNotNull(config);
    return getClient(getClientProtocol(RestClient.class, config),
        config);
  }

  /**
   * Creates OzoneClient with the given ClientProtocol and Configuration.
   *
   * @param clientProtocol
   *        Protocol to be used by the OzoneClient
   *
   * @param config
   *        Configuration to be used for OzoneClient creation
   */
  private static OzoneClient getClient(ClientProtocol clientProtocol,
                                       Configuration config) {
    OzoneClientInvocationHandler clientHandler =
        new OzoneClientInvocationHandler(clientProtocol);
    ClientProtocol proxy = (ClientProtocol) Proxy.newProxyInstance(
        OzoneClientInvocationHandler.class.getClassLoader(),
        new Class<?>[]{ClientProtocol.class}, clientHandler);
    return new OzoneClient(config, proxy);
  }

  /**
   * Returns an instance of Protocol class.
   *
   * @param protocolClass
   *        Class object of the ClientProtocol.
   *
   * @param config
   *        Configuration used to initialize ClientProtocol.
   *
   * @return ClientProtocol
   *
   * @throws IOException
   */
  private static ClientProtocol getClientProtocol(
      Class<? extends ClientProtocol> protocolClass, Configuration config)
      throws IOException {
    try {
      LOG.debug("Using {} as client protocol.",
          protocolClass.getCanonicalName());
      Constructor<? extends ClientProtocol> ctor =
          protocolClass.getConstructor(Configuration.class);
      return ctor.newInstance(config);
    } catch (Exception e) {
      final String message = "Couldn't create protocol " + protocolClass;
      LOG.error(message + " exception: ", e);
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else if (e instanceof InvocationTargetException) {
        throw new IOException(message,
            ((InvocationTargetException) e).getTargetException());
      } else {
        throw new IOException(message, e);
      }
    }
  }

}
