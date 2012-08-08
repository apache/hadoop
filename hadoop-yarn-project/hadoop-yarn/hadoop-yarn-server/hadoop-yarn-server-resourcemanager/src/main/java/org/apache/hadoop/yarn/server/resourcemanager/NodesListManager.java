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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.service.AbstractService;

public class NodesListManager extends AbstractService{

  private static final Log LOG = LogFactory.getLog(NodesListManager.class);

  private HostsFileReader hostsReader;
  private Configuration conf;

  public NodesListManager() {
    super(NodesListManager.class.getName());
  }

  @Override
  public void init(Configuration conf) {

    this.conf = conf;

    // Read the hosts/exclude files to restrict access to the RM
    try {
      this.hostsReader = 
        new HostsFileReader(
            conf.get(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH, 
                YarnConfiguration.DEFAULT_RM_NODES_INCLUDE_FILE_PATH),
            conf.get(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH, 
                YarnConfiguration.DEFAULT_RM_NODES_EXCLUDE_FILE_PATH)
                );
      printConfiguredHosts();
    } catch (IOException ioe) {
      LOG.warn("Failed to init hostsReader, disabling", ioe);
      try {
        this.hostsReader = 
          new HostsFileReader(YarnConfiguration.DEFAULT_RM_NODES_INCLUDE_FILE_PATH, 
              YarnConfiguration.DEFAULT_RM_NODES_EXCLUDE_FILE_PATH);
      } catch (IOException ioe2) {
        // Should *never* happen
        this.hostsReader = null;
        throw new YarnException(ioe2);
      }
    }
    super.init(conf);
  }

  private void printConfiguredHosts() {
    if (!LOG.isDebugEnabled()) {
      return;
    }
    
    LOG.debug("hostsReader: in=" + conf.get(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH, 
        YarnConfiguration.DEFAULT_RM_NODES_INCLUDE_FILE_PATH) + " out=" +
        conf.get(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH, 
            YarnConfiguration.DEFAULT_RM_NODES_EXCLUDE_FILE_PATH));
    for (String include : hostsReader.getHosts()) {
      LOG.debug("include: " + include);
    }
    for (String exclude : hostsReader.getExcludedHosts()) {
      LOG.debug("exclude: " + exclude);
    }
  }

  public void refreshNodes() throws IOException {
    synchronized (hostsReader) {
      hostsReader.refresh();
      printConfiguredHosts();
    }
  }

  public boolean isValidNode(String hostName) {
    synchronized (hostsReader) {
      Set<String> hostsList = hostsReader.getHosts();
      Set<String> excludeList = hostsReader.getExcludedHosts();
      return ((hostsList.isEmpty() || hostsList.contains(hostName)) && 
          !excludeList.contains(hostName));
    }
  }
}
