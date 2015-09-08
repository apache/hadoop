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

package org.apache.hadoop.yarn.server.nodemanager.amrmproxy;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extends the AbstractRequestInterceptor class and provides an implementation
 * that simply forwards the AM requests to the cluster resource manager.
 *
 */
public final class DefaultRequestInterceptor extends
    AbstractRequestInterceptor {
  private static final Logger LOG = LoggerFactory
      .getLogger(DefaultRequestInterceptor.class);
  private ApplicationMasterProtocol rmClient;
  private UserGroupInformation user = null;

  @Override
  public void init(AMRMProxyApplicationContext appContext) {
    super.init(appContext);
    try {
      user =
          UserGroupInformation.createProxyUser(appContext
              .getApplicationAttemptId().toString(), UserGroupInformation
              .getCurrentUser());
      user.addToken(appContext.getAMRMToken());
      final Configuration conf = this.getConf();

      rmClient =
          user.doAs(new PrivilegedExceptionAction<ApplicationMasterProtocol>() {
            @Override
            public ApplicationMasterProtocol run() throws Exception {
              return ClientRMProxy.createRMProxy(conf,
                  ApplicationMasterProtocol.class);
            }
          });
    } catch (IOException e) {
      String message =
          "Error while creating of RM app master service proxy for attemptId:"
              + appContext.getApplicationAttemptId().toString();
      if (user != null) {
        message += ", user: " + user;
      }

      LOG.info(message);
      throw new YarnRuntimeException(message, e);
    } catch (Exception e) {
      throw new YarnRuntimeException(e);
    }
  }

  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      final RegisterApplicationMasterRequest request)
      throws YarnException, IOException {
    LOG.info("Forwarding registration request to the real YARN RM");
    return rmClient.registerApplicationMaster(request);
  }

  @Override
  public AllocateResponse allocate(final AllocateRequest request)
      throws YarnException, IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Forwarding allocate request to the real YARN RM");
    }
    AllocateResponse allocateResponse = rmClient.allocate(request);
    if (allocateResponse.getAMRMToken() != null) {
      updateAMRMToken(allocateResponse.getAMRMToken());
    }

    return allocateResponse;
  }

  @Override
  public FinishApplicationMasterResponse finishApplicationMaster(
      final FinishApplicationMasterRequest request) throws YarnException,
      IOException {
    LOG.info("Forwarding finish application request to "
        + "the real YARN Resource Manager");
    return rmClient.finishApplicationMaster(request);
  }

  @Override
  public void setNextInterceptor(RequestInterceptor next) {
    throw new YarnRuntimeException(
        "setNextInterceptor is being called on DefaultRequestInterceptor,"
            + "which should be the last one in the chain "
            + "Check if the interceptor pipeline configuration is correct");
  }

  private void updateAMRMToken(Token token) throws IOException {
    org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> amrmToken =
        new org.apache.hadoop.security.token.Token<AMRMTokenIdentifier>(
            token.getIdentifier().array(), token.getPassword().array(),
            new Text(token.getKind()), new Text(token.getService()));
    // Preserve the token service sent by the RM when adding the token
    // to ensure we replace the previous token setup by the RM.
    // Afterwards we can update the service address for the RPC layer.
    user.addToken(amrmToken);
    amrmToken.setService(ClientRMProxy.getAMRMTokenService(getConf()));
  }
}
