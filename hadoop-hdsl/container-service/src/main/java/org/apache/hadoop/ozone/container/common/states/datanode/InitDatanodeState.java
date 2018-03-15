/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.common.states.datanode;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdsl.HdslUtils;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.states.DatanodeState;
import org.apache.hadoop.scm.ScmConfigKeys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdsl.HdslUtils.getSCMAddresses;

/**
 * Init Datanode State is the task that gets run when we are in Init State.
 */
public class InitDatanodeState implements DatanodeState,
    Callable<DatanodeStateMachine.DatanodeStates> {
  static final Logger LOG = LoggerFactory.getLogger(InitDatanodeState.class);
  private final SCMConnectionManager connectionManager;
  private final Configuration conf;
  private final StateContext context;
  private Future<DatanodeStateMachine.DatanodeStates> result;

  /**
   *  Create InitDatanodeState Task.
   *
   * @param conf - Conf
   * @param connectionManager - Connection Manager
   * @param context - Current Context
   */
  public InitDatanodeState(Configuration conf,
                           SCMConnectionManager connectionManager,
                           StateContext context) {
    this.conf = conf;
    this.connectionManager = connectionManager;
    this.context = context;
  }

  /**
   * Computes a result, or throws an exception if unable to do so.
   *
   * @return computed result
   * @throws Exception if unable to compute a result
   */
  @Override
  public DatanodeStateMachine.DatanodeStates call() throws Exception {
    Collection<InetSocketAddress> addresses = null;
    try {
      addresses = getSCMAddresses(conf);
    } catch (IllegalArgumentException e) {
      if(!Strings.isNullOrEmpty(e.getMessage())) {
        LOG.error("Failed to get SCM addresses: " + e.getMessage());
      }
      return DatanodeStateMachine.DatanodeStates.SHUTDOWN;
    }

    if (addresses == null || addresses.isEmpty()) {
      LOG.error("Null or empty SCM address list found.");
      return DatanodeStateMachine.DatanodeStates.SHUTDOWN;
    } else {
      for (InetSocketAddress addr : addresses) {
        connectionManager.addSCMServer(addr);
      }
    }

    // If datanode ID is set, persist it to the ID file.
    persistContainerDatanodeID();

    return this.context.getState().getNextState();
  }

  /**
   * Update Ozone container port to the datanode ID,
   * and persist the ID to a local file.
   */
  private void persistContainerDatanodeID() throws IOException {
    String dataNodeIDPath = HdslUtils.getDatanodeIDPath(conf);
    if (Strings.isNullOrEmpty(dataNodeIDPath)) {
      LOG.error("A valid file path is needed for config setting {}",
          ScmConfigKeys.OZONE_SCM_DATANODE_ID);
      this.context.setState(DatanodeStateMachine.DatanodeStates.SHUTDOWN);
      return;
    }
    File idPath = new File(dataNodeIDPath);
    int containerPort = this.context.getContainerPort();
    int ratisPort = this.context.getRatisPort();
    DatanodeID datanodeID = this.context.getParent().getDatanodeID();
    if (datanodeID != null) {
      datanodeID.setContainerPort(containerPort);
      datanodeID.setRatisPort(ratisPort);
      ContainerUtils.writeDatanodeIDTo(datanodeID, idPath);
      LOG.info("Datanode ID is persisted to {}", dataNodeIDPath);
    }
  }

  /**
   * Called before entering this state.
   */
  @Override
  public void onEnter() {
    LOG.trace("Entering init container state");
  }

  /**
   * Called After exiting this state.
   */
  @Override
  public void onExit() {
    LOG.trace("Exiting init container state");
  }

  /**
   * Executes one or more tasks that is needed by this state.
   *
   * @param executor -  ExecutorService
   */
  @Override
  public void execute(ExecutorService executor) {
    result = executor.submit(this);
  }

  /**
   * Wait for execute to finish.
   *
   * @param time     - Time
   * @param timeUnit - Unit of time.
   */
  @Override
  public DatanodeStateMachine.DatanodeStates await(long time,
      TimeUnit timeUnit) throws InterruptedException,
      ExecutionException, TimeoutException {
    return result.get(time, timeUnit);
  }
}
