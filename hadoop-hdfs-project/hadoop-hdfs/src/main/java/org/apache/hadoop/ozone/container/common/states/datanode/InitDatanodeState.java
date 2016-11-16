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

import com.google.common.base.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneClientUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.states.DatanodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
    String[] addresses = conf.getStrings(OzoneConfigKeys.OZONE_SCM_NAMES);
    final Optional<Integer> defaultPort =  Optional.of(OzoneConfigKeys
        .OZONE_SCM_DEFAULT_PORT);

    if (addresses == null || addresses.length <= 0) {
      LOG.error("SCM addresses need to be a set of valid DNS names " +
          "or IP addresses. Null or empty address list found. Aborting " +
          "containers.");
      return DatanodeStateMachine.DatanodeStates.SHUTDOWN;
    }
    for (String address : addresses) {
      Optional<String> hostname = OzoneClientUtils.getHostName(address);
      if (!hostname.isPresent()) {
        LOG.error("Invalid hostname for SCM.");
        return DatanodeStateMachine.DatanodeStates.SHUTDOWN;
      }
      Optional<Integer> port = OzoneClientUtils.getHostPort(address);
      InetSocketAddress addr = NetUtils.createSocketAddr(hostname.get(),
          port.or(defaultPort.get()));
      connectionManager.addSCMServer(addr);
    }
    return this.context.getState().getNextState();
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
