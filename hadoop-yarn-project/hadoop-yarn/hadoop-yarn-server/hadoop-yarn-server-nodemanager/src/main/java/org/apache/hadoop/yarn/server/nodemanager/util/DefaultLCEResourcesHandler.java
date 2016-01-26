/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor;

@Deprecated
public class DefaultLCEResourcesHandler implements LCEResourcesHandler {

  final static Log LOG = LogFactory
      .getLog(DefaultLCEResourcesHandler.class);

  private Configuration conf;
  
  public DefaultLCEResourcesHandler() {
  }
  
  public void setConf(Configuration conf) {
        this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return  conf;
  }
  
  public void init(LinuxContainerExecutor lce) {
  }

  /*
   * LCE Resources Handler interface
   */
  
  public void preExecute(ContainerId containerId, Resource containerResource) {
  }
  
  public void postExecute(ContainerId containerId) {
  }
  
  public String getResourcesOption(ContainerId containerId) {
    return "cgroups=none";
  }


}