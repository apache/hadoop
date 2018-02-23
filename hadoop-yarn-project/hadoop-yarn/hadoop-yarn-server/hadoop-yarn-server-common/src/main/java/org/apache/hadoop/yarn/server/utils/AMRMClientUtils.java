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

package org.apache.hadoop.yarn.server.utils;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for AMRMClient.
 */
@Private
public final class AMRMClientUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(AMRMClientUtils.class);

  public static final String APP_ALREADY_REGISTERED_MESSAGE =
      "Application Master is already registered : ";

  private AMRMClientUtils() {
  }

  /**
   * Handle ApplicationNotRegistered exception and re-register.
   *
   * @param appId application Id
   * @param rmProxy RM proxy instance
   * @param registerRequest the AM re-register request
   * @throws YarnException if re-register fails
   */
  public static void handleNotRegisteredExceptionAndReRegister(
      ApplicationId appId, ApplicationMasterProtocol rmProxy,
      RegisterApplicationMasterRequest registerRequest) throws YarnException {
    LOG.info("App attempt {} not registered, most likely due to RM failover. "
        + " Trying to re-register.", appId);
    try {
      rmProxy.registerApplicationMaster(registerRequest);
    } catch (Exception e) {
      if (e instanceof InvalidApplicationMasterRequestException
          && e.getMessage().contains(APP_ALREADY_REGISTERED_MESSAGE)) {
        LOG.info("Concurrent thread successfully registered, moving on.");
      } else {
        LOG.error("Error trying to re-register AM", e);
        throw new YarnException(e);
      }
    }
  }

  /**
   * Helper method for client calling ApplicationMasterProtocol.allocate that
   * handles re-register if RM fails over.
   *
   * @param request allocate request
   * @param rmProxy RM proxy
   * @param registerRequest the register request for re-register
   * @param appId application id
   * @return allocate response
   * @throws YarnException if RM call fails
   * @throws IOException if RM call fails
   */
  public static AllocateResponse allocateWithReRegister(AllocateRequest request,
      ApplicationMasterProtocol rmProxy,
      RegisterApplicationMasterRequest registerRequest, ApplicationId appId)
      throws YarnException, IOException {
    try {
      return rmProxy.allocate(request);
    } catch (ApplicationMasterNotRegisteredException e) {
      handleNotRegisteredExceptionAndReRegister(appId, rmProxy,
          registerRequest);
      // reset responseId after re-register
      request.setResponseId(0);
      // retry allocate
      return allocateWithReRegister(request, rmProxy, registerRequest, appId);
    }
  }

  /**
   * Helper method for client calling
   * ApplicationMasterProtocol.finishApplicationMaster that handles re-register
   * if RM fails over.
   *
   * @param request finishApplicationMaster request
   * @param rmProxy RM proxy
   * @param registerRequest the register request for re-register
   * @param appId application id
   * @return finishApplicationMaster response
   * @throws YarnException if RM call fails
   * @throws IOException if RM call fails
   */
  public static FinishApplicationMasterResponse finishAMWithReRegister(
      FinishApplicationMasterRequest request, ApplicationMasterProtocol rmProxy,
      RegisterApplicationMasterRequest registerRequest, ApplicationId appId)
      throws YarnException, IOException {
    try {
      return rmProxy.finishApplicationMaster(request);
    } catch (ApplicationMasterNotRegisteredException ex) {
      handleNotRegisteredExceptionAndReRegister(appId, rmProxy,
          registerRequest);
      // retry finishAM after re-register
      return finishAMWithReRegister(request, rmProxy, registerRequest, appId);
    }
  }

  /**
   * Create a proxy for the specified protocol.
   *
   * @param configuration Configuration to generate {@link ClientRMProxy}
   * @param protocol Protocol for the proxy
   * @param user the user on whose behalf the proxy is being created
   * @param token the auth token to use for connection
   * @param <T> Type information of the proxy
   * @return Proxy to the RM
   * @throws IOException on failure
   */
  @Public
  @Unstable
  public static <T> T createRMProxy(final Configuration configuration,
      final Class<T> protocol, UserGroupInformation user,
      final Token<? extends TokenIdentifier> token) throws IOException {
    try {
      String rmClusterId = configuration.get(YarnConfiguration.RM_CLUSTER_ID,
          YarnConfiguration.DEFAULT_RM_CLUSTER_ID);
      LOG.info("Creating RMProxy to RM {} for protocol {} for user {}",
          rmClusterId, protocol.getSimpleName(), user);
      if (token != null) {
        // preserve the token service sent by the RM when adding the token
        // to ensure we replace the previous token setup by the RM.
        // Afterwards we can update the service address for the RPC layer.
        // Same as YarnServerSecurityUtils.updateAMRMToken()
        user.addToken(token);
        token.setService(ClientRMProxy.getAMRMTokenService(configuration));
        setAuthModeInConf(configuration);
      }
      final T proxyConnection = user.doAs(new PrivilegedExceptionAction<T>() {
        @Override
        public T run() throws Exception {
          return ClientRMProxy.createRMProxy(configuration, protocol);
        }
      });
      return proxyConnection;

    } catch (InterruptedException e) {
      throw new YarnRuntimeException(e);
    }
  }

  private static void setAuthModeInConf(Configuration conf) {
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        SaslRpcServer.AuthMethod.TOKEN.toString());
  }
}