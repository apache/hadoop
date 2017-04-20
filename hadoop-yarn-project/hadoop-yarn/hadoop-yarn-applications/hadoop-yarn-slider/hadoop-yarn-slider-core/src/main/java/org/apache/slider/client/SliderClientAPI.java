/*
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

package org.apache.slider.client;

import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.slider.api.resource.Application;
import org.apache.slider.api.types.NodeInformationList;
import org.apache.slider.common.params.AbstractClusterBuildingActionArgs;
import org.apache.slider.common.params.ActionAMSuicideArgs;
import org.apache.slider.common.params.ActionClientArgs;
import org.apache.slider.common.params.ActionDependencyArgs;
import org.apache.slider.common.params.ActionDiagnosticArgs;
import org.apache.slider.common.params.ActionFlexArgs;
import org.apache.slider.common.params.ActionFreezeArgs;
import org.apache.slider.common.params.ActionKeytabArgs;
import org.apache.slider.common.params.ActionNodesArgs;
import org.apache.slider.common.params.ActionKillContainerArgs;
import org.apache.slider.common.params.ActionListArgs;
import org.apache.slider.common.params.ActionRegistryArgs;
import org.apache.slider.common.params.ActionResolveArgs;
import org.apache.slider.common.params.ActionResourceArgs;
import org.apache.slider.common.params.ActionStatusArgs;
import org.apache.slider.common.params.ActionThawArgs;
import org.apache.slider.common.params.ActionUpgradeArgs;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.providers.AbstractClientProvider;

import java.io.IOException;

/**
 * Interface of those method calls in the slider API that are intended
 * for direct public invocation.
 * <p>
 * Stability: evolving
 */
public interface SliderClientAPI extends Service {

  int actionDestroy(String clustername) throws YarnException, IOException;

  /**
   * AM to commit an asynchronous suicide
   */
  int actionAmSuicide(String clustername,
      ActionAMSuicideArgs args) throws YarnException, IOException;

  /**
   * Get the provider for this cluster
   * @param provider the name of the provider
   * @return the provider instance
   * @throws SliderException problems building the provider
   */
  AbstractClientProvider createClientProvider(String provider)
    throws SliderException;

  /**
   * Manage keytabs leveraged by slider
   *
   * @param keytabInfo the arguments needed to manage the keytab
   * @throws YarnException Yarn problems
   * @throws IOException other problems
   * @throws BadCommandArgumentsException bad arguments.
   */
  int actionKeytab(ActionKeytabArgs keytabInfo)
      throws YarnException, IOException;

  /**
   * Manage file resources leveraged by slider
   *
   * @param resourceInfo the arguments needed to manage the resource
   * @throws YarnException Yarn problems
   * @throws IOException other problems
   * @throws BadCommandArgumentsException bad arguments.
   */
  int actionResource(ActionResourceArgs resourceInfo)
      throws YarnException, IOException;

  /**
   * Perform client operations such as install or configure
   *
   * @param clientInfo the arguments needed for client operations
   *
   * @throws SliderException bad arguments.
   * @throws IOException problems related to package and destination folders
   */
  int actionClient(ActionClientArgs clientInfo)
      throws IOException, YarnException;

  /**
   * Update the cluster specification
   *
   * @param clustername cluster name
   * @param buildInfo the arguments needed to update the cluster
   * @throws YarnException Yarn problems
   * @throws IOException other problems
   */
  int actionUpdate(String clustername,
      AbstractClusterBuildingActionArgs buildInfo)
      throws YarnException, IOException; 

  /**
   * Upgrade the cluster with a newer version of the application
   *
   * @param clustername cluster name
   * @param buildInfo the arguments needed to upgrade the cluster
   * @throws YarnException Yarn problems
   * @throws IOException other problems
   */
  int actionUpgrade(String clustername,
      ActionUpgradeArgs buildInfo)
      throws YarnException, IOException;

  /**
   * Implement the list action: list all nodes
   * @return exit code of 0 if a list was created
   */
  int actionList(String clustername, ActionListArgs args) throws IOException, YarnException;


  int actionFlex(String name, ActionFlexArgs args) throws YarnException,
      IOException;

  /**
   * Test for a cluster existing probe for a cluster of the given name existing
   * in the filesystem. If the live param is set, it must be a live cluster
   * @return exit code
   */
  int actionExists(String name, boolean checkLive) throws YarnException, IOException;

  /**
   * Kill a specific container of the cluster
   * @param name cluster name
   * @param args arguments
   * @return exit code
   * @throws YarnException
   * @throws IOException
   */
  int actionKillContainer(String name, ActionKillContainerArgs args)
      throws YarnException, IOException;

  /**
   * Status operation
   *
   * @param clustername cluster name
   * @param statusArgs status arguments
   * @return 0 -for success, else an exception is thrown
   * @throws YarnException
   * @throws IOException
   */
  int actionStatus(String clustername, ActionStatusArgs statusArgs)
      throws YarnException, IOException;

  /**
   * Status operation which returns the status object as a string instead of
   * printing it to the console or file.
   *
   * @param clustername cluster name
   * @return cluster status details
   * @throws YarnException
   * @throws IOException
   */
  Application actionStatus(String clustername) throws YarnException, IOException;

  /**
   * Version Details
   * @return exit code
   */
  int actionVersion();

  /**
   * Stop the cluster
   *
   * @param clustername cluster name
   * @param freezeArgs arguments to the stop
   * @return EXIT_SUCCESS if the cluster was not running by the end of the operation
   */
  int actionStop(String clustername, ActionFreezeArgs freezeArgs)
      throws YarnException, IOException;

  /**
   * Restore a cluster
   */
  int actionStart(String clustername, ActionThawArgs thaw) throws YarnException, IOException;

  /**
   * Registry operation
   *
   * @param args registry Arguments
   * @return 0 for success, -1 for some issues that aren't errors, just failures
   * to retrieve information (e.g. no configurations for that entry)
   * @throws YarnException YARN problems
   * @throws IOException Network or other problems
   */
  int actionResolve(ActionResolveArgs args)
      throws YarnException, IOException;

  /**
   * Registry operation
   *
   * @param registryArgs registry Arguments
   * @return 0 for success, -1 for some issues that aren't errors, just failures
   * to retrieve information (e.g. no configurations for that entry)
   * @throws YarnException YARN problems
   * @throws IOException Network or other problems
   */
  int actionRegistry(ActionRegistryArgs registryArgs)
      throws YarnException, IOException;

  /**
   * diagnostic operation
   *
   * @param diagnosticArgs diagnostic Arguments
   * @return 0 for success, -1 for some issues that aren't errors, just
   *         failures to retrieve information (e.g. no application name
   *         specified)
   * @throws YarnException YARN problems
   * @throws IOException Network or other problems
   */
  int actionDiagnostic(ActionDiagnosticArgs diagnosticArgs);

  /**
   * Get the registry binding. As this may start the registry, it can take time
   * and fail
   * @return the registry 
   */
  RegistryOperations getRegistryOperations()
      throws SliderException, IOException;

  /**
   * Upload all Slider AM and agent dependency libraries to HDFS, so that they
   * do not need to be uploaded with every create call. This operation is
   * Slider version specific. So it needs to be invoked for every single
   * version of slider/slider-client.
   * 
   * @throws SliderException
   * @throws IOException
   */
  int actionDependency(ActionDependencyArgs dependencyArgs) throws IOException,
      YarnException;

  /**
   * List the nodes
   * @param args
   * @return
   * @throws YarnException
   * @throws IOException
   */
  NodeInformationList listYarnClusterNodes(ActionNodesArgs args)
    throws YarnException, IOException;
}
