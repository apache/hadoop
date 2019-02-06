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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * Interface class for Node label store.
 */
public interface NodeLabelsStore extends Closeable {

  /**
   * Store node {@literal ->} label.
   */
  void updateNodeToLabelsMappings(
      Map<NodeId, Set<String>> nodeToLabels) throws IOException;

  /**
   * Store new labels.
   */
  void storeNewClusterNodeLabels(List<NodeLabel> label)
      throws IOException;

  /**
   * Remove labels.
   */
  void removeClusterNodeLabels(Collection<String> labels)
      throws IOException;

  /**
   * Recover labels and node to labels mappings from store, but if
   * ignoreNodeToLabelsMappings is true then node to labels mappings should not
   * be recovered. In case of Distributed NodeLabels setup
   * ignoreNodeToLabelsMappings will be set to true and recover will be invoked
   * as RM will collect the node labels from NM through registration/HB.
   *
   * @throws IOException
   * @throws YarnException
   */
  void recover() throws IOException, YarnException;

  void init(Configuration conf, CommonNodeLabelsManager mgr)
      throws Exception;

}
