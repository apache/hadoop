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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.conf.HAUtil;

import java.io.IOException;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RMHAProtocolService extends AbstractService implements
    HAServiceProtocol {
  private static final Log LOG = LogFactory.getLog(RMHAProtocolService.class);

  private Configuration conf;
  private ResourceManager rm;
  @VisibleForTesting
  protected HAServiceState haState = HAServiceState.INITIALIZING;
  private boolean haEnabled;

  public RMHAProtocolService(ResourceManager resourceManager)  {
    super("RMHAProtocolService");
    this.rm = resourceManager;
  }

  @Override
  protected synchronized void serviceInit(Configuration conf) throws
      Exception {
    this.conf = conf;
    haEnabled = HAUtil.isHAEnabled(this.conf);
    if (haEnabled) {
      HAUtil.verifyAndSetConfiguration(conf);
      rm.setConf(this.conf);
    }
    rm.createAndInitActiveServices();
    super.serviceInit(this.conf);
  }

  @Override
  protected synchronized void serviceStart() throws Exception {
    if (haEnabled) {
      transitionToStandby(true);
    } else {
      transitionToActive();
    }

    super.serviceStart();
  }

  @Override
  protected synchronized void serviceStop() throws Exception {
    transitionToStandby(false);
    haState = HAServiceState.STOPPING;
    super.serviceStop();
  }

  @Override
  public synchronized void monitorHealth() throws HealthCheckFailedException {
    if (haState == HAServiceState.ACTIVE && !rm.areActiveServicesRunning()) {
      throw new HealthCheckFailedException(
          "Active ResourceManager services are not running!");
    }
  }

  private synchronized void transitionToActive() throws Exception {
    if (haState == HAServiceState.ACTIVE) {
      LOG.info("Already in active state");
      return;
    }

    LOG.info("Transitioning to active");
    rm.startActiveServices();
    haState = HAServiceState.ACTIVE;
    LOG.info("Transitioned to active");
  }

  @Override
  public synchronized void transitionToActive(StateChangeRequestInfo reqInfo) {
    // TODO (YARN-1177): When automatic failover is enabled,
    // check if transition should be allowed for this request
    try {
      transitionToActive();
    } catch (Exception e) {
      LOG.error("Error when transitioning to Active mode", e);
      throw new YarnRuntimeException(e);
    }
  }

  private synchronized void transitionToStandby(boolean initialize)
      throws Exception {
    if (haState == HAServiceState.STANDBY) {
      LOG.info("Already in standby state");
      return;
    }

    LOG.info("Transitioning to standby");
    if (haState == HAServiceState.ACTIVE) {
      rm.stopActiveServices();
      if (initialize) {
        rm.createAndInitActiveServices();
      }
    }
    haState = HAServiceState.STANDBY;
    LOG.info("Transitioned to standby");
  }

  @Override
  public synchronized void transitionToStandby(StateChangeRequestInfo reqInfo) {
    // TODO (YARN-1177): When automatic failover is enabled,
    // check if transition should be allowed for this request
    try {
      transitionToStandby(true);
    } catch (Exception e) {
      LOG.error("Error when transitioning to Standby mode", e);
      throw new YarnRuntimeException(e);
    }
  }

  @Override
  public synchronized HAServiceStatus getServiceStatus() throws IOException {
    HAServiceStatus ret = new HAServiceStatus(haState);
    if (haState == HAServiceState.ACTIVE || haState == HAServiceState.STANDBY) {
      ret.setReadyToBecomeActive();
    } else {
      ret.setNotReadyToBecomeActive("State is " + haState);
    }
    return ret;
  }
}
