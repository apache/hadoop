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

package org.apache.hadoop.test.system.process;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

/**
 * Interface to manage the remote processes in the cluster.
 */
public interface ClusterProcessManager {

  /**
   * Initialization method to pass the configuration object which is required 
   * by the ClusterProcessManager to manage the cluster.<br/>
   * Configuration object should typically contain all the parameters which are 
   * required by the implementations.<br/>
   *  
   * @param conf configuration containing values of the specific keys which 
   * are required by the implementation of the cluster process manger.
   * 
   * @throws IOException when initialization fails.
   */
  void init(Configuration conf) throws IOException;

  /**
   * Get the list of RemoteProcess handles of all the remote processes.
   */
  List<RemoteProcess> getAllProcesses();

  /**
   * Get all the roles this cluster's daemon processes have.
   */
  Set<Enum<?>> getRoles();

  /**
   * Method to start all the remote daemons.<br/>
   * 
   * @throws IOException if startup procedure fails.
   */
  void start() throws IOException;

  /**
   * Method to shutdown all the remote daemons.<br/>
   * 
   * @throws IOException if shutdown procedure fails.
   */
  void stop() throws IOException;

}
