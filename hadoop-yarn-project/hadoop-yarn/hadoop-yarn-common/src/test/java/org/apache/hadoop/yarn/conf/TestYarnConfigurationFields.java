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

package org.apache.hadoop.yarn.conf;

import java.util.HashSet;

import org.apache.hadoop.conf.TestConfigurationFieldsBase;

/**
 * Unit test class to compare
 * {@link org.apache.hadoop.yarn.conf.YarnConfiguration} and
 * yarn-default.xml for missing properties.  Currently only throws an error
 * if the class is missing a property.
 * <p></p>
 * Refer to {@link org.apache.hadoop.conf.TestConfigurationFieldsBase}
 * for how this class works.
 */
public class TestYarnConfigurationFields extends TestConfigurationFieldsBase {

  @Override
  public void initializeMemberVariables() {
    xmlFilename = new String("yarn-default.xml");
    configurationClasses = new Class[] { YarnConfiguration.class };
        

    // Allocate for usage
    configurationPropsToSkipCompare = new HashSet<String>();

    // Set error modes
    errorIfMissingConfigProps = true;
    errorIfMissingXmlProps = false;

    // Specific properties to skip
    configurationPropsToSkipCompare
        .add(YarnConfiguration.DEFAULT_RM_CONFIGURATION_PROVIDER_CLASS);
    configurationPropsToSkipCompare
        .add(YarnConfiguration.DEFAULT_CLIENT_FAILOVER_PROXY_PROVIDER);
    configurationPropsToSkipCompare
        .add(YarnConfiguration.DEFAULT_IPC_RECORD_FACTORY_CLASS);
    configurationPropsToSkipCompare
        .add(YarnConfiguration.DEFAULT_IPC_CLIENT_FACTORY_CLASS);
    configurationPropsToSkipCompare
        .add(YarnConfiguration.DEFAULT_IPC_SERVER_FACTORY_CLASS);
    configurationPropsToSkipCompare
        .add(YarnConfiguration.DEFAULT_IPC_RPC_IMPL);
    configurationPropsToSkipCompare
        .add(YarnConfiguration.DEFAULT_RM_SCHEDULER);
    configurationPropsToSkipCompare
        .add(YarnConfiguration
	    .YARN_SECURITY_SERVICE_AUTHORIZATION_APPLICATIONCLIENT_PROTOCOL);
    configurationPropsToSkipCompare
        .add(YarnConfiguration
	    .YARN_SECURITY_SERVICE_AUTHORIZATION_APPLICATIONMASTER_PROTOCOL);
    configurationPropsToSkipCompare
        .add(YarnConfiguration
	    .YARN_SECURITY_SERVICE_AUTHORIZATION_CONTAINER_MANAGEMENT_PROTOCOL);
    configurationPropsToSkipCompare
        .add(YarnConfiguration
	    .YARN_SECURITY_SERVICE_AUTHORIZATION_RESOURCE_LOCALIZER);
    configurationPropsToSkipCompare
        .add(YarnConfiguration
	    .YARN_SECURITY_SERVICE_AUTHORIZATION_RESOURCEMANAGER_ADMINISTRATION_PROTOCOL);
    configurationPropsToSkipCompare
        .add(YarnConfiguration
	    .YARN_SECURITY_SERVICE_AUTHORIZATION_RESOURCETRACKER_PROTOCOL);

    // Allocate for usage
    xmlPropsToSkipCompare = new HashSet<String>();
    xmlPrefixToSkipCompare = new HashSet<String>();

    // Should probably be moved from yarn-default.xml to mapred-default.xml
    xmlPropsToSkipCompare.add("mapreduce.job.hdfs-servers");
    xmlPropsToSkipCompare.add("mapreduce.job.jar");

    // Possibly obsolete, but unable to verify 100%
    xmlPropsToSkipCompare.add("yarn.nodemanager.aux-services.mapreduce_shuffle.class");
    xmlPropsToSkipCompare.add("yarn.resourcemanager.container.liveness-monitor.interval-ms");

    // Used in the XML file as a variable reference internal to the XML file
    xmlPropsToSkipCompare.add("yarn.nodemanager.hostname");
    xmlPropsToSkipCompare.add("yarn.timeline-service.hostname");

    // Currently defined in TimelineAuthenticationFilterInitializer
    xmlPrefixToSkipCompare.add("yarn.timeline-service.http-authentication");

    // Currently defined in RegistryConstants
    xmlPrefixToSkipCompare.add("hadoop.registry");
  }

}
