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

package org.apache.hadoop.yarn.server.resourcemanager;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager;

import java.io.IOException;

public class RMSecretManagerService extends AbstractService {

  AMRMTokenSecretManager amRmTokenSecretManager;
  NMTokenSecretManagerInRM nmTokenSecretManager;
  ClientToAMTokenSecretManagerInRM clientToAMSecretManager;
  RMContainerTokenSecretManager containerTokenSecretManager;
  RMDelegationTokenSecretManager rmDTSecretManager;

  RMContextImpl rmContext;

  /**
   * Construct the service.
   *
   */
  public RMSecretManagerService(Configuration conf, RMContextImpl rmContext) {
    super(RMSecretManagerService.class.getName());
    this.rmContext = rmContext;

    // To initialize correctly, these managers should be created before
    // being called serviceInit().
    nmTokenSecretManager = createNMTokenSecretManager(conf);
    rmContext.setNMTokenSecretManager(nmTokenSecretManager);

    containerTokenSecretManager = createContainerTokenSecretManager(conf);
    rmContext.setContainerTokenSecretManager(containerTokenSecretManager);

    clientToAMSecretManager = createClientToAMTokenSecretManager();
    rmContext.setClientToAMTokenSecretManager(clientToAMSecretManager);

    amRmTokenSecretManager = createAMRMTokenSecretManager(conf, this.rmContext);
    rmContext.setAMRMTokenSecretManager(amRmTokenSecretManager);

    rmDTSecretManager =
        createRMDelegationTokenSecretManager(conf, rmContext);
    rmContext.setRMDelegationTokenSecretManager(rmDTSecretManager);
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    amRmTokenSecretManager.start();
    containerTokenSecretManager.start();
    nmTokenSecretManager.start();

    try {
      rmDTSecretManager.startThreads();
    } catch(IOException ie) {
      throw new YarnRuntimeException("Failed to start secret manager threads", ie);
    }
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    if (rmDTSecretManager != null) {
      rmDTSecretManager.stopThreads();
    }
    if (amRmTokenSecretManager != null) {
      amRmTokenSecretManager.stop();
    }
    if (containerTokenSecretManager != null) {
      containerTokenSecretManager.stop();
    }
    if(nmTokenSecretManager != null) {
      nmTokenSecretManager.stop();
    }
    super.serviceStop();
  }

  protected RMContainerTokenSecretManager createContainerTokenSecretManager(
      Configuration conf) {
    return new RMContainerTokenSecretManager(conf);
  }

  protected NMTokenSecretManagerInRM createNMTokenSecretManager(
      Configuration conf) {
    return new NMTokenSecretManagerInRM(conf);
  }

  protected AMRMTokenSecretManager createAMRMTokenSecretManager(
      Configuration conf, RMContext rmContext) {
    return new AMRMTokenSecretManager(conf, rmContext);
  }

  protected ClientToAMTokenSecretManagerInRM createClientToAMTokenSecretManager() {
    return new ClientToAMTokenSecretManagerInRM();
  }

  @VisibleForTesting
  protected RMDelegationTokenSecretManager createRMDelegationTokenSecretManager(
      Configuration conf, RMContext rmContext) {
    long secretKeyInterval =
        conf.getLong(YarnConfiguration.DELEGATION_KEY_UPDATE_INTERVAL_KEY,
            YarnConfiguration.DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT);
    long tokenMaxLifetime =
        conf.getLong(YarnConfiguration.DELEGATION_TOKEN_MAX_LIFETIME_KEY,
            YarnConfiguration.DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT);
    long tokenRenewInterval =
        conf.getLong(YarnConfiguration.DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
            YarnConfiguration.DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT);

    return new RMDelegationTokenSecretManager(secretKeyInterval,
        tokenMaxLifetime, tokenRenewInterval, 3600000, rmContext);
  }

}
