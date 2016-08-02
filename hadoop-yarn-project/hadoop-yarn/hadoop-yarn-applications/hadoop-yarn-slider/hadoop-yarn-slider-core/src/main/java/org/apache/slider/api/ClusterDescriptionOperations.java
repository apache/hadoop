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

package org.apache.slider.api;

import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.providers.SliderProviderFactory;

import java.util.Map;

import static org.apache.slider.api.OptionKeys.ZOOKEEPER_PATH;
import static org.apache.slider.api.OptionKeys.ZOOKEEPER_QUORUM;

/**
 * Operations on Cluster Descriptions
 */
public class ClusterDescriptionOperations {


  public static ClusterDescription buildFromInstanceDefinition(AggregateConf aggregateConf) throws
                                                                                       BadConfigException {

    ClusterDescription cd = new ClusterDescription();
    
    aggregateConf.resolve();

    //options are a merge of all globals
    Map<String, String> options = cd.options;
    SliderUtils.mergeMapsIgnoreDuplicateKeys(options,
        aggregateConf.getInternal().global);
    SliderUtils.mergeMapsIgnoreDuplicateKeys(options,
        aggregateConf.getAppConf().global);
    SliderUtils.mergeMapsIgnoreDuplicateKeys(options,
        aggregateConf.getResources().global);

    //roles are the role values merged in the same order
    mergeInComponentMap(cd, aggregateConf.getInternal());
    mergeInComponentMap(cd, aggregateConf.getAppConf());
    mergeInComponentMap(cd, aggregateConf.getResources());

    //now add the extra bits
    cd.state = ClusterDescription.STATE_LIVE;
    MapOperations internalOptions =
      aggregateConf.getInternalOperations().getGlobalOptions();
    MapOperations appOptions =
      aggregateConf.getAppConfOperations().getGlobalOptions();

    cd.type = internalOptions.getOption(InternalKeys.INTERNAL_PROVIDER_NAME,
                                SliderProviderFactory.DEFAULT_CLUSTER_TYPE);

    cd.dataPath = internalOptions.get(InternalKeys.INTERNAL_DATA_DIR_PATH);
    cd.name = internalOptions.get(OptionKeys.APPLICATION_NAME);
    cd.originConfigurationPath = internalOptions.get(InternalKeys.INTERNAL_SNAPSHOT_CONF_PATH);
    cd.generatedConfigurationPath = internalOptions.get(InternalKeys.INTERNAL_GENERATED_CONF_PATH);
    cd.setImagePath(internalOptions.get(InternalKeys.INTERNAL_APPLICATION_IMAGE_PATH));
    cd.setApplicationHome(internalOptions.get(InternalKeys.INTERNAL_APPLICATION_HOME));
    cd.setZkPath(appOptions.get(ZOOKEEPER_PATH));
    cd.setZkHosts(appOptions.get(ZOOKEEPER_QUORUM));
    
    return cd;
  }

  private static void mergeInComponentMap(ClusterDescription cd,
                                          ConfTree confTree) {

    Map<String, Map<String, String>> components = confTree.components;
    for (Map.Entry<String, Map<String, String>> compEntry : components.entrySet()) {
      String name = compEntry.getKey();
      Map<String, String> destRole = cd.getOrAddRole(name);
      Map<String, String> sourceComponent = compEntry.getValue();
      SliderUtils.mergeMapsIgnoreDuplicateKeys(destRole, sourceComponent);
    }
  }
}
