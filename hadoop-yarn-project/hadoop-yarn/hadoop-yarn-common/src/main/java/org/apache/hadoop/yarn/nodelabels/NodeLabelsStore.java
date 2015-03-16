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

package org.apache.hadoop.yarn.nodelabels;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;

public abstract class NodeLabelsStore implements Closeable {
  protected final CommonNodeLabelsManager mgr;

  public NodeLabelsStore(CommonNodeLabelsManager mgr) {
    this.mgr = mgr;
  }
  
  /**
   * Store node {@literal ->} label
   */
  public abstract void updateNodeToLabelsMappings(
      Map<NodeId, Set<String>> nodeToLabels) throws IOException;

  /**
   * Store new labels
   */
  public abstract void storeNewClusterNodeLabels(Set<String> label)
      throws IOException;

  /**
   * Remove labels
   */
  public abstract void removeClusterNodeLabels(Collection<String> labels)
      throws IOException;
  
  /**
   * Recover labels and node to labels mappings from store
   */
  public abstract void recover() throws IOException;
  
  public void init(Configuration conf) throws Exception {}
  
  public CommonNodeLabelsManager getNodeLabelsManager() {
    return mgr;
  }
}
