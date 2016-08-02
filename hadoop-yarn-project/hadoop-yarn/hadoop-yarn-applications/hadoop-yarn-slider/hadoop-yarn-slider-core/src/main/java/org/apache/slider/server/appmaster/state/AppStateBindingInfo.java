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

package org.apache.slider.server.appmaster.state;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.providers.ProviderRole;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Binding information for application states; designed to be extensible
 * so that tests don't have to be massivley reworked when new arguments
 * are added.
 */
public class AppStateBindingInfo {
  public AggregateConf instanceDefinition;
  public Configuration serviceConfig = new Configuration();
  public Configuration publishedProviderConf = new Configuration(false);
  public List<ProviderRole> roles = new ArrayList<>();
  public FileSystem fs;
  public Path historyPath;
  public List<Container> liveContainers = new ArrayList<>(0);
  public Map<String, String> applicationInfo = new HashMap<>();
  public ContainerReleaseSelector releaseSelector = new SimpleReleaseSelector();
  /** node reports off the RM. */
  public List<NodeReport> nodeReports = new ArrayList<>(0);

  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(instanceDefinition != null, "null instanceDefinition");
    Preconditions.checkArgument(serviceConfig != null, "null appmasterConfig");
    Preconditions.checkArgument(publishedProviderConf != null, "null publishedProviderConf");
    Preconditions.checkArgument(releaseSelector != null, "null releaseSelector");
    Preconditions.checkArgument(roles != null, "null providerRoles");
    Preconditions.checkArgument(fs != null, "null fs");
    Preconditions.checkArgument(historyPath != null, "null historyDir");
    Preconditions.checkArgument(nodeReports != null, "null nodeReports");
  }
}
