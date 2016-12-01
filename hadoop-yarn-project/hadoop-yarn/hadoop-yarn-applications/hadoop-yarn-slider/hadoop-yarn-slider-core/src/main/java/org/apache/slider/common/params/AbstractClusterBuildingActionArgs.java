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

package org.apache.slider.common.params;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.Path;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.providers.SliderProviderFactory;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Abstract Action to build things; shares args across build and
 * list
 */
public abstract class AbstractClusterBuildingActionArgs extends
    AbstractActionArgs {

  /**
   * Declare the image configuration directory to use when creating or
   * reconfiguring a slider cluster. The path must be on a filesystem visible
   * to all nodes in the YARN cluster. Only one configuration directory can
   * be specified.
   */
  @Parameter(names = ARG_CONFDIR,
      description = "Path to cluster configuration directory in HDFS",
      converter = PathArgumentConverter.class)
  public Path confdir;

  @Parameter(names = ARG_ZKPATH,
      description = "Zookeeper path for the application")
  public String appZKPath;

  @Parameter(names = ARG_ZKHOSTS,
      description = "comma separated list of the Zookeeper hosts")
  public String zkhosts;

  /**
   * --image path
   * the full path to a .tar or .tar.gz path containing an HBase image.
   */
  @Parameter(names = ARG_IMAGE,
      description = "The full path to a .tar or .tar.gz path containing the application",
      converter = PathArgumentConverter.class)
  public Path image;

  @Parameter(names = ARG_APP_HOME,
      description = "Home directory of a pre-installed application")
  public String appHomeDir;

  @Parameter(names = ARG_PROVIDER,
      description = "Provider of the specific cluster application")
  public String provider = SliderProviderFactory.DEFAULT_CLUSTER_TYPE;

  @Parameter(names = {ARG_PACKAGE},
      description = "URI to a slider package")
  public String packageURI;

  @Parameter(names = {ARG_RESOURCES},
      description = "File defining the resources of this instance")
  public File resources;

  @Parameter(names = {ARG_TEMPLATE},
      description = "Template application configuration")
  public File template;

  @Parameter(names = {ARG_METAINFO},
      description = "Application meta info file")
  public File appMetaInfo;

  @Parameter(names = {ARG_METAINFO_JSON},
      description = "Application meta info JSON blob")
  public String appMetaInfoJson;

  @Parameter(names = {ARG_APPDEF},
      description = "Application def (folder or a zip package)")
  public File appDef;

  @Parameter(names = {ARG_QUEUE},
             description = "Queue to submit the application")
  public String queue;

  @Parameter(names = {ARG_LIFETIME},
      description = "Lifetime of the application from the time of request")
  public long lifetime;

  @ParametersDelegate
  public ComponentArgsDelegate componentDelegate = new ComponentArgsDelegate();

  @ParametersDelegate
  public AddonArgsDelegate addonDelegate = new AddonArgsDelegate();


  @ParametersDelegate
  public AppAndResouceOptionArgsDelegate optionsDelegate =
      new AppAndResouceOptionArgsDelegate();


  public Map<String, String> getOptionsMap() throws
      BadCommandArgumentsException {
    return optionsDelegate.getOptionsMap();
  }

  /**
   * Get the role heap mapping (may be empty, but never null)
   * @return role heap mapping
   * @throws BadCommandArgumentsException parse problem
   */
  public Map<String, Map<String, String>> getCompOptionMap() throws
      BadCommandArgumentsException {
    return optionsDelegate.getCompOptionMap();
  }


  public Map<String, String> getResourceOptionsMap() throws
      BadCommandArgumentsException {
    return optionsDelegate.getResourceOptionsMap();
  }

  /**
   * Get the role heap mapping (may be empty, but never null)
   * @return role heap mapping
   * @throws BadCommandArgumentsException parse problem
   */
  public Map<String, Map<String, String>> getResourceCompOptionMap() throws
      BadCommandArgumentsException {
    return optionsDelegate.getResourceCompOptionMap();
  }

  @VisibleForTesting
  public List<String> getComponentTuples() {
    return componentDelegate.getComponentTuples();
  }

  /**
   * Get the role mapping (may be empty, but never null)
   * @return role mapping
   * @throws BadCommandArgumentsException parse problem
   */
  public Map<String, String> getComponentMap() throws
      BadCommandArgumentsException {
    return componentDelegate.getComponentMap();
  }

  @VisibleForTesting
  public List<String> getAddonTuples() {
    return addonDelegate.getAddonTuples();
  }

  /**
   * Get the list of addons (may be empty, but never null)
   */
  public Map<String, String> getAddonMap() throws
      BadCommandArgumentsException {
    return addonDelegate.getAddonMap();
  }

  public Path getConfdir() {
    return confdir;
  }

  public String getAppZKPath() {
    return appZKPath;
  }

  public String getZKhosts() {
    return zkhosts;
  }

  public Path getImage() {
    return image;
  }

  public String getAppHomeDir() {
    return appHomeDir;
  }

  public String getProvider() {
    return provider;
  }

  public ConfTree buildAppOptionsConfTree() throws
      BadCommandArgumentsException {
    return buildConfTree(getOptionsMap());
  }

  public ConfTree buildResourceOptionsConfTree() throws
      BadCommandArgumentsException {
    return buildConfTree(getResourceOptionsMap());
  }

  protected ConfTree buildConfTree(Map<String, String> optionsMap) throws
      BadCommandArgumentsException {
    ConfTree confTree = new ConfTree();
    ConfTreeOperations ops = new ConfTreeOperations(confTree);
    confTree.global.putAll(optionsMap);
    return confTree;
  }
}
