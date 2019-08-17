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
import java.util.ArrayList;

import com.google.common.base.Joiner;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.DistributedSchedulingAMProtocol;
import org.apache.hadoop.yarn.server.api.ServerRMProxy;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterDistributedSchedulingAMResponse;
import org.apache.hadoop.yarn.server.utils.YarnServerSecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

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

      rmClient = createRMClient(appContext, conf);
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

  private ApplicationMasterProtocol createRMClient(
      AMRMProxyApplicationContext appContext, final Configuration conf)
      throws IOException, InterruptedException {
    if (appContext.getNMCotext().isDistributedSchedulingEnabled()) {
      return user.doAs(
          new PrivilegedExceptionAction<DistributedSchedulingAMProtocol>() {
            @Override
            public DistributedSchedulingAMProtocol run() throws Exception {
              setAMRMTokenService(conf);
              return ServerRMProxy.createRMProxy(conf,
                  DistributedSchedulingAMProtocol.class);
            }
          });
    } else {
      return user.doAs(
          new PrivilegedExceptionAction<ApplicationMasterProtocol>() {
            @Override
            public ApplicationMasterProtocol run() throws Exception {
              setAMRMTokenService(conf);
              return ClientRMProxy.createRMProxy(conf,
                  ApplicationMasterProtocol.class);
            }
          });
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
    LOG.debug("Forwarding allocate request to the real YARN RM");
    AllocateResponse allocateResponse = rmClient.allocate(request);
    if (allocateResponse.getAMRMToken() != null) {
      YarnServerSecurityUtils.updateAMRMToken(allocateResponse.getAMRMToken(),
          this.user, getConf());
    }

    return allocateResponse;
  }

  @Override
  public RegisterDistributedSchedulingAMResponse
  registerApplicationMasterForDistributedScheduling
      (RegisterApplicationMasterRequest request) throws YarnException,
      IOException {
    if (getApplicationContext().getNMCotext()
        .isDistributedSchedulingEnabled()) {
      LOG.info("Forwarding registerApplicationMasterForDistributedScheduling" +
          "request to the real YARN RM");
      return ((DistributedSchedulingAMProtocol)rmClient)
          .registerApplicationMasterForDistributedScheduling(request);
    } else {
      throw new YarnException("Distributed Scheduling is not enabled.");
    }
  }

  @Override
  public DistributedSchedulingAllocateResponse allocateForDistributedScheduling(
      DistributedSchedulingAllocateRequest request)
      throws YarnException, IOException {
    LOG.debug("Forwarding allocateForDistributedScheduling request" +
        "to the real YARN RM");
    if (getApplicationContext().getNMCotext()
        .isDistributedSchedulingEnabled()) {
      DistributedSchedulingAllocateResponse allocateResponse =
          ((DistributedSchedulingAMProtocol)rmClient)
              .allocateForDistributedScheduling(request);
      if (allocateResponse.getAllocateResponse().getAMRMToken() != null) {
        YarnServerSecurityUtils.updateAMRMToken(
            allocateResponse.getAllocateResponse().getAMRMToken(), this.user,
            getConf());
      }
      return allocateResponse;
    } else {
      throw new YarnException("Distributed Scheduling is not enabled.");
    }
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

  @VisibleForTesting
  public void setRMClient(final ApplicationMasterProtocol rmClient) {
    if (rmClient instanceof DistributedSchedulingAMProtocol) {
      this.rmClient = (DistributedSchedulingAMProtocol)rmClient;
    } else {
      this.rmClient = new DistributedSchedulingAMProtocol() {
        @Override
        public RegisterApplicationMasterResponse registerApplicationMaster
            (RegisterApplicationMasterRequest request) throws YarnException,
            IOException {
          return rmClient.registerApplicationMaster(request);
        }

        @Override
        public FinishApplicationMasterResponse finishApplicationMaster
            (FinishApplicationMasterRequest request) throws YarnException,
            IOException {
          return rmClient.finishApplicationMaster(request);
        }

        @Override
        public AllocateResponse allocate(AllocateRequest request) throws
            YarnException, IOException {
          return rmClient.allocate(request);
        }

        @Override
        public RegisterDistributedSchedulingAMResponse
        registerApplicationMasterForDistributedScheduling
            (RegisterApplicationMasterRequest request) throws YarnException,
            IOException {
          throw new IOException("Not Supported !!");
        }

        @Override
        public DistributedSchedulingAllocateResponse
            allocateForDistributedScheduling(
            DistributedSchedulingAllocateRequest request)
                throws YarnException, IOException {
          throw new IOException("Not Supported !!");
        }
      };
    }
  }

  private static void setAMRMTokenService(final Configuration conf)
      throws IOException {
    for (org.apache.hadoop.security.token.Token<? extends TokenIdentifier> token : UserGroupInformation
        .getCurrentUser().getTokens()) {
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        token.setService(ClientRMProxy.getAMRMTokenService(conf));
      }
    }
  }

  @InterfaceStability.Unstable
  public static Text getTokenService(Configuration conf, String address,
      String defaultAddr, int defaultPort) {
    if (HAUtil.isHAEnabled(conf)) {
      // Build a list of service addresses to form the service name
      ArrayList<String> services = new ArrayList<String>();
      YarnConfiguration yarnConf = new YarnConfiguration(conf);
      for (String rmId : HAUtil.getRMHAIds(conf)) {
        // Set RM_ID to get the corresponding RM_ADDRESS
        yarnConf.set(YarnConfiguration.RM_HA_ID, rmId);
        services.add(SecurityUtil.buildTokenService(
            yarnConf.getSocketAddr(address, defaultAddr, defaultPort))
            .toString());
      }
      return new Text(Joiner.on(',').join(services));
    }

    // Non-HA case - no need to set RM_ID
    return SecurityUtil.buildTokenService(conf.getSocketAddr(address,
        defaultAddr, defaultPort));
  }
}
