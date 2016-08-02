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
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.slider.api.types.NodeInformationList;
import org.apache.slider.api.types.SliderInstanceDescription;
import org.apache.slider.common.params.AbstractClusterBuildingActionArgs;
import org.apache.slider.common.params.ActionAMSuicideArgs;
import org.apache.slider.common.params.ActionClientArgs;
import org.apache.slider.common.params.ActionDependencyArgs;
import org.apache.slider.common.params.ActionDestroyArgs;
import org.apache.slider.common.params.ActionDiagnosticArgs;
import org.apache.slider.common.params.ActionEchoArgs;
import org.apache.slider.common.params.ActionFlexArgs;
import org.apache.slider.common.params.ActionFreezeArgs;
import org.apache.slider.common.params.ActionInstallKeytabArgs;
import org.apache.slider.common.params.ActionInstallPackageArgs;
import org.apache.slider.common.params.ActionKeytabArgs;
import org.apache.slider.common.params.ActionNodesArgs;
import org.apache.slider.common.params.ActionPackageArgs;
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
import java.util.Map;

/**
 * Interface of those method calls in the slider API that are intended
 * for direct public invocation.
 * <p>
 * Stability: evolving
 */
public interface SliderClientAPI extends Service {
  /**
   * Destroy a cluster. There's two race conditions here
   * #1 the cluster is started between verifying that there are no live
   * clusters of that name.
   */
  int actionDestroy(String clustername, ActionDestroyArgs destroyArgs)
      throws YarnException, IOException;

  int actionDestroy(String clustername) throws YarnException,
      IOException;

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
   * Build up the cluster specification/directory
   *
   * @param clustername cluster name
   * @param buildInfo the arguments needed to build the cluster
   * @throws YarnException Yarn problems
   * @throws IOException other problems
   * @throws BadCommandArgumentsException bad arguments.
   */
  int actionBuild(String clustername,
      AbstractClusterBuildingActionArgs buildInfo) throws YarnException, IOException;

  /**
   * Upload keytab to a designated sub-directory of the user home directory
   *
   * @param installKeytabInfo the arguments needed to upload the keytab
   * @throws YarnException Yarn problems
   * @throws IOException other problems
   * @throws BadCommandArgumentsException bad arguments.
   * @deprecated use #actionKeytab
   */
  int actionInstallKeytab(ActionInstallKeytabArgs installKeytabInfo)
      throws YarnException, IOException;

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
   * Upload application package to user home directory
   *
   * @param installPkgInfo the arguments needed to upload the package
   * @throws YarnException Yarn problems
   * @throws IOException other problems
   * @throws BadCommandArgumentsException bad arguments.
   */
  int actionInstallPkg(ActionInstallPackageArgs installPkgInfo)
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
   * Managing slider application package
   *
   * @param pkgInfo the arguments needed to upload, delete or list the package
   * @throws YarnException Yarn problems
   * @throws IOException other problems
   * @throws BadCommandArgumentsException bad arguments.
   */
  int actionPackage(ActionPackageArgs pkgInfo)
      throws YarnException, IOException;

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
   * Get the report of a this application
   * @return the app report or null if it could not be found.
   * @throws IOException
   * @throws YarnException
   */
  ApplicationReport getApplicationReport()
      throws IOException, YarnException;

  /**
   * Kill the submitted application via YARN
   * @throws YarnException
   * @throws IOException
   */
  boolean forceKillApplication(String reason)
    throws YarnException, IOException;

  /**
   * Implement the list action: list all nodes
   * @return exit code of 0 if a list was created
   */
  int actionList(String clustername, ActionListArgs args) throws IOException, YarnException;

  /**
   * Enumerate slider instances for the current user, and the
   * most recent app report, where available.
   * @param listOnlyInState boolean to indicate that the instances should
   * only include those in a YARN state
   * <code> minAppState &lt;= currentState &lt;= maxAppState </code>
   *
   * @param minAppState minimum application state to include in enumeration.
   * @param maxAppState maximum application state to include
   * @return a map of application instance name to description
   * @throws IOException Any IO problem
   * @throws YarnException YARN problems
   */
  Map<String, SliderInstanceDescription> enumSliderInstances(
      boolean listOnlyInState,
      YarnApplicationState minAppState,
      YarnApplicationState maxAppState)
      throws IOException, YarnException;

  /**
   * Implement the islive action: probe for a cluster of the given name existing
   * @return exit code
   */
  int actionFlex(String name, ActionFlexArgs args) throws YarnException, IOException;

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
   * Echo operation (not currently wired up to command line)
   * @param name cluster name
   * @param args arguments
   * @return the echoed text
   * @throws YarnException
   * @throws IOException
   */
  String actionEcho(String name, ActionEchoArgs args)
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
  int actionFreeze(String clustername, ActionFreezeArgs freezeArgs)
      throws YarnException, IOException;

  /**
   * Restore a cluster
   */
  int actionThaw(String clustername, ActionThawArgs thaw) throws YarnException, IOException;

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
