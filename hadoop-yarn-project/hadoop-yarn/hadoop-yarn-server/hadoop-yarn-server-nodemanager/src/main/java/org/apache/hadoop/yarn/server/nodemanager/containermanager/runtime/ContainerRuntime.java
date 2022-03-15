/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerExecContext;

/**
 * An abstraction for various container runtime implementations. Examples
 * include Process Tree, Docker, Appc runtimes etc. These implementations
 * are meant for low-level OS container support - dependencies on
 * higher-level node manager constructs should be avoided.
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface ContainerRuntime {
  /**
   * Prepare a container to be ready for launch.
   *
   * @param ctx the {@link ContainerRuntimeContext}
   * @throws ContainerExecutionException if an error occurs while preparing
   * the container
   */
  void prepareContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException;

  /**
   * Launch a container.
   *
   * @param ctx the {@link ContainerRuntimeContext}
   * @throws ContainerExecutionException if an error occurs while launching
   * the container
   */
  void launchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException;

  /**
   * Relaunch a container.
   *
   * @param ctx the {@link ContainerRuntimeContext}
   * @throws ContainerExecutionException if an error occurs while relaunching
   * the container
   */
  void relaunchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException;

  /**
   * Signal a container. Signals may be a request to terminate, a status check,
   * etc.
   *
   * @param ctx the {@link ContainerRuntimeContext}
   * @throws ContainerExecutionException if an error occurs while signaling
   * the container
   */
  void signalContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException;

  /**
   * Perform any container cleanup that may be required.
   *
   * @param ctx the {@link ContainerRuntimeContext}
   * @throws ContainerExecutionException if an error occurs while reaping
   * the container
   */
  void reapContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException;

  /**
   * Run a program in container.
   *
   * @param ctx the {@link ContainerExecContext}
   * @return stdin and stdout of container exec
   * @throws ContainerExecutionException
   */
  IOStreamPair execContainer(ContainerExecContext ctx)
      throws ContainerExecutionException;

  /**
   * Return the host and ip of the container.
   *
   * @param container the {@link Container}
   * @throws ContainerExecutionException if an error occurs while getting the ip
   * and hostname
   */
  String[] getIpAndHost(Container container) throws ContainerExecutionException;

  /**
   * Return the exposed ports of the container.
   * @param container the {@link Container}
   * @return List of exposed ports
   * @throws ContainerExecutionException if an error occurs while getting
   * the exposed ports
   */
  String getExposedPorts(Container container)
      throws ContainerExecutionException;
}
