/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.maintenance;

import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintenance mode consumer class that sets
 * maintenance mode on/off on NM.
 */
public class NMHeartbeatMaintenanceModeConsumerImpl
    extends MaintenanceModeConsumer {

  private static final Logger LOG =
      LoggerFactory.getLogger(NMHeartbeatMaintenanceModeConsumerImpl.class);

  public NMHeartbeatMaintenanceModeConsumerImpl(NodeManager nodeManager) {
    super(NMHeartbeatMaintenanceModeConsumerImpl.class.getName(), nodeManager);
  }

  /**
   * Changes NMContext maintenance flag to true.
   */
  @Override
  protected void maintenanceModeStarted() {
    LOG.info("Starting Maintenance mode on NM");
    ((NMContext) getContext()).setMaintenance(true);
  }

  /**
   * Changes NMContext maintenance flag to false.
   */
  @Override
  protected void maintenanceModeEnded() {
    LOG.info("Stopping Maintenance mode on NM");
    ((NMContext) getContext()).setMaintenance(false);
  }
}