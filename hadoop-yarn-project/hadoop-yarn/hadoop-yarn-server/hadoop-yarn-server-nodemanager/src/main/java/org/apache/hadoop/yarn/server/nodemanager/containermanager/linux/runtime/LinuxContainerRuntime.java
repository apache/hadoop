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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntime;

import java.util.Map;

/**
 * Linux-specific container runtime implementations must implement this
 * interface.
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface LinuxContainerRuntime extends ContainerRuntime {
  /**
   * Initialize the runtime.
   *
   * @param conf the {@link Configuration} to use
   * @param nmContext NMContext
   * @throws ContainerExecutionException if an error occurs while initializing
   * the runtime
   */
  void initialize(Configuration conf, Context nmContext) throws ContainerExecutionException;

  /**
   * Return whether the given environment variables indicate that the operation
   * is requesting this runtime.
   *
   * @param env the environment variable settings for the operation
   * @return whether this runtime is requested
   */
  boolean isRuntimeRequested(Map<String, String> env);
}

