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

package org.apache.hadoop.yarn.server.resourcemanager.nodelabels;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.nodelabels.NodeLabelsStore;
import org.apache.hadoop.yarn.nodelabels.RMNodeLabel;

public class NullRMNodeLabelsManager extends RMNodeLabelsManager {
  Map<NodeId, Set<String>> lastNodeToLabels = null;
  Collection<String> lastAddedlabels = null;
  Collection<String> lastRemovedlabels = null;

  @Override
  public void initNodeLabelStore(Configuration conf) {
    this.store = new NodeLabelsStore() {

      @Override
      public void recover()
          throws IOException {
        // do nothing
      }

      @Override
      public void init(Configuration conf, CommonNodeLabelsManager mgr)
          throws Exception {

      }

      @Override
      public void removeClusterNodeLabels(Collection<String> labels)
          throws IOException {
        // do nothing
      }

      @Override
      public void updateNodeToLabelsMappings(
          Map<NodeId, Set<String>> nodeToLabels) throws IOException {
        // do nothing
      }

      @Override
      public void storeNewClusterNodeLabels(List<NodeLabel> label)
          throws IOException {
        // do nothing
      }

      @Override
      public void close() throws IOException {
        // do nothing
      }
    };
  }

  @Override
  protected void initDispatcher(Configuration conf) {
    super.dispatcher = null;
  }

  @Override
  protected void startDispatcher() {
    // do nothing
  }
  
  @Override
  protected void stopDispatcher() {
    // do nothing
  }
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // always enable node labels while using MemoryRMNodeLabelsManager
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    super.serviceInit(conf);
  }

  public void setResourceForLabel(String label, Resource resource) {
    if (label.equals(NO_LABEL)) {
      noNodeLabel = new FakeLabel(resource);
      return;
    }

    labelCollections.put(label, new FakeLabel(label, resource));
  }

  private static class FakeLabel extends RMNodeLabel {

    FakeLabel(String label, Resource resource) {
      super(label, resource, 1, false);
    }

    FakeLabel(Resource resource) {
      super(NO_LABEL, resource, 1, false);
    }
  }
}
