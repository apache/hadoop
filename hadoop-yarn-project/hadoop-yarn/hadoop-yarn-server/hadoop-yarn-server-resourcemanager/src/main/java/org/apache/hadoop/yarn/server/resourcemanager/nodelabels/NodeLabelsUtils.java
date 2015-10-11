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
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.NodeLabel;

/**
 * Node labels utilities.
 */
public final class NodeLabelsUtils {
  private static final Log LOG = LogFactory.getLog(NodeLabelsUtils.class);

  private NodeLabelsUtils() { /* Hidden constructor */ }

  public static Set<String> convertToStringSet(Set<NodeLabel> nodeLabels) {
    if (null == nodeLabels) {
      return null;
    }
    Set<String> labels = new HashSet<String>();
    for (NodeLabel label : nodeLabels) {
      labels.add(label.getName());
    }
    return labels;
  }

  public static void verifyCentralizedNodeLabelConfEnabled(String operation,
      boolean isCentralizedNodeLabelConfiguration) throws IOException {
    if (!isCentralizedNodeLabelConfiguration) {
      String msg =
          String.format("Error when invoke method=%s because "
              + "centralized node label configuration is not enabled.",
              operation);
      LOG.error(msg);
      throw new IOException(msg);
    }
  }
}
