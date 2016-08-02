/*
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

package org.apache.slider.server.appmaster.rpc;

import com.google.common.base.Preconditions;
import com.google.protobuf.BlockingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcEngine;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.slider.api.SliderClusterProtocol;
import org.apache.slider.common.SliderExitCodes;
import org.apache.slider.common.tools.Duration;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.apache.slider.core.exceptions.ErrorStrings;
import org.apache.slider.core.exceptions.ServiceNotReadyException;
import org.apache.slider.core.exceptions.SliderException;

import static org.apache.slider.common.SliderXmlConfKeys.*; 

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

public class RpcBinder {
  protected static final Logger log =
    LoggerFactory.getLogger(RpcBinder.class);

  /**
   * Create a protobuf server bonded to the specific socket address
   * @param addr address to listen to; 0.0.0.0 as hostname acceptable
   * @param conf config
   * @param secretManager token secret handler
   * @param numHandlers threads to service requests
   * @param blockingService service to handle
   * @param portRangeConfig range of ports
   * @return the IPC server itself
   * @throws IOException
   */
  public static Server createProtobufServer(InetSocketAddress addr,
                                            Configuration conf,
                                            SecretManager<? extends TokenIdentifier> secretManager,
                                            int numHandlers,
                                            BlockingService blockingService,
                                            String portRangeConfig) throws
                                                      IOException {
    Class<SliderClusterProtocolPB> sliderClusterAPIClass = registerSliderAPI(
        conf);
    RPC.Server server = new RPC.Builder(conf).setProtocol(sliderClusterAPIClass)
                                             .setInstance(blockingService)
                                             .setBindAddress(addr.getAddress()
                                                 .getCanonicalHostName())
                                             .setPort(addr.getPort())
                                             .setNumHandlers(numHandlers)
                                             .setVerbose(false)
                                             .setSecretManager(secretManager)
                                             .setPortRangeConfig(
                                               portRangeConfig)
                                             .build();
    log.debug(
      "Adding protocol " + sliderClusterAPIClass.getCanonicalName() + " to the server");
    server.addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER, sliderClusterAPIClass,
                       blockingService);
    return server;
  }

  /**
   * Add the protobuf engine to the configuration. Harmless and inexpensive
   * if repeated.
   * @param conf configuration to patch
   * @return the protocol class
   */
  public static Class<SliderClusterProtocolPB> registerSliderAPI(
      Configuration conf) {
    Class<SliderClusterProtocolPB> sliderClusterAPIClass =
      SliderClusterProtocolPB.class;
    RPC.setProtocolEngine(conf, sliderClusterAPIClass, ProtobufRpcEngine.class);
    
    //quick sanity check here
    assert verifyBondedToProtobuf(conf, sliderClusterAPIClass);
    
    return sliderClusterAPIClass;
  }

  /**
   * Verify that the conf is set up for protobuf transport of Slider RPC
   * @param conf configuration
   * @param sliderClusterAPIClass class for the API
   * @return true if the RPC engine is protocol buffers
   */
  public static boolean verifyBondedToProtobuf(Configuration conf,
                                                Class<SliderClusterProtocolPB> sliderClusterAPIClass) {
    return conf.getClass("rpc.engine." + sliderClusterAPIClass.getName(),
                         RpcEngine.class) .equals(ProtobufRpcEngine.class);
  }


  /**
   * Connect to a server. May include setting up retry policies
   * @param addr
   * @param currentUser
   * @param conf
   * @param rpcTimeout
   * @return
   * @throws IOException
   */
  public static SliderClusterProtocol connectToServer(InetSocketAddress addr,
                                                    UserGroupInformation currentUser,
                                                    Configuration conf,
                                                    int rpcTimeout) throws IOException {
    Class<SliderClusterProtocolPB> sliderClusterAPIClass =
        registerSliderAPI(conf);

    final RetryPolicy retryPolicy = RetryPolicies.TRY_ONCE_THEN_FAIL;
    log.debug("Connecting to Slider AM at {}", addr);
    ProtocolProxy<SliderClusterProtocolPB> protoProxy =
        RPC.getProtocolProxy(sliderClusterAPIClass,
            1,
            addr,
            currentUser,
            conf,
            NetUtils.getDefaultSocketFactory(conf),
            rpcTimeout,
            retryPolicy);
    SliderClusterProtocolPB endpoint = protoProxy.getProxy();
    return new SliderClusterProtocolProxy(endpoint, addr);
  }


  /**
   * This loops for a limited period trying to get the Proxy -
   * by doing so it handles AM failover
   * @param conf configuration to patch and use
   * @param rmClient client of the resource manager
   * @param application application to work with
   * @param connectTimeout timeout for the whole proxy operation to timeout
   * (milliseconds). Use 0 to indicate "do not attempt to wait" -fail fast.
   * @param rpcTimeout timeout for RPCs to block during communications
   * @return the proxy
   * @throws IOException IO problems
   * @throws YarnException Slider-generated exceptions related to the binding
   * failing. This can include the application finishing or timeouts
   * @throws InterruptedException if a sleep operation waiting for
   * the cluster to respond is interrupted.
   */
  @SuppressWarnings("NestedAssignment")
  public static SliderClusterProtocol getProxy(final Configuration conf,
                                      final ApplicationClientProtocol rmClient,
                                      ApplicationReport application,
                                      final int connectTimeout,
                                      final int rpcTimeout)
      throws IOException, YarnException, InterruptedException {
    ApplicationId appId;
    appId = application.getApplicationId();
    Duration timeout = new Duration(connectTimeout);
    timeout.start();
    Exception exception = null;
    YarnApplicationState state = null;
    try {
      while (application != null &&
             (state = application.getYarnApplicationState()).equals(
                 YarnApplicationState.RUNNING)) {
  
        try {
          return getProxy(conf, application, rpcTimeout);
        } catch (IOException e) {
          if (connectTimeout <= 0 || timeout.getLimitExceeded()) {
            throw e;
          }
          exception = e;
        } catch (YarnException e) {
          if (connectTimeout <= 0 || timeout.getLimitExceeded()) {
            throw e;
          }
          exception = e;
        }
        //at this point: app failed to work
        log.debug("Could not connect to {}. Waiting for getting the latest AM address...",
                  appId);
        Thread.sleep(1000);
        //or get the app report
        application =
          rmClient.getApplicationReport(
              GetApplicationReportRequest.newInstance(appId)).getApplicationReport();
      }
      //get here if the app is no longer running. Raise a specific
      //exception but init it with the previous failure
      throw new BadClusterStateException(
                              exception,
                              ErrorStrings.E_FINISHED_APPLICATION, appId, state );
    } finally {
      timeout.close();
    }
  }

  /**
   * Get a proxy from the application report
   * @param conf config to use
   * @param application app report
   * @param rpcTimeout timeout in RPC operations
   * @return the proxy
   * @throws IOException
   * @throws SliderException
   * @throws InterruptedException
   */
  public static SliderClusterProtocol getProxy(final Configuration conf,
      final ApplicationReport application,
      final int rpcTimeout)
      throws IOException, SliderException, InterruptedException {

    String host = application.getHost();
    int port = application.getRpcPort();
    org.apache.hadoop.yarn.api.records.Token clientToAMToken =
        application.getClientToAMToken();
    return createProxy(conf, host, port, clientToAMToken, rpcTimeout);
  }

  /**
   *
   * @param conf config to use
   * @param host hosname
   * @param port port
   * @param clientToAMToken auth token: only used in a secure cluster.
   * converted via {@link ConverterUtils#convertFromYarn(org.apache.hadoop.yarn.api.records.Token, InetSocketAddress)}
   * @param rpcTimeout timeout in RPC operations
   * @return the proxy
   * @throws SliderException
   * @throws IOException
   * @throws InterruptedException
   */
  public static SliderClusterProtocol createProxy(final Configuration conf,
      String host,
      int port,
      org.apache.hadoop.yarn.api.records.Token clientToAMToken,
      final int rpcTimeout) throws
      SliderException,
      IOException,
      InterruptedException {
    String address = host + ":" + port;
    if (SliderUtils.isUnset(host) || 0 == port) {
      throw new SliderException(SliderExitCodes.EXIT_CONNECTIVITY_PROBLEM,
                              "Slider instance "
                              + " isn't providing a valid address for the" +
                              " Slider RPC protocol: " + address);
    }

    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    final UserGroupInformation newUgi = UserGroupInformation.createRemoteUser(
      currentUser.getUserName());
    final InetSocketAddress serviceAddr =
        NetUtils.createSocketAddrForHost(host, port);
    SliderClusterProtocol realProxy;

    log.debug("Connecting to {}", serviceAddr);
    if (UserGroupInformation.isSecurityEnabled()) {
      Preconditions.checkArgument(clientToAMToken != null,
          "Null clientToAMToken");
      Token<ClientToAMTokenIdentifier> token =
        ConverterUtils.convertFromYarn(clientToAMToken, serviceAddr);
      newUgi.addToken(token);
      realProxy =
        newUgi.doAs(new PrivilegedExceptionAction<SliderClusterProtocol>() {
          @Override
          public SliderClusterProtocol run() throws IOException {
            return connectToServer(serviceAddr, newUgi, conf, rpcTimeout);
          }
        });
    } else {
      realProxy = connectToServer(serviceAddr, newUgi, conf, rpcTimeout);
    }
    return realProxy;
  }
}
