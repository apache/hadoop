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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeToAttributes;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Interface class for Node label store.
 */
public interface NodeAttributeStore extends Closeable {

  /**
   * Replace labels on node.
   *
   * @param nodeToAttribute node to attribute list.
   * @throws IOException
   */
  void replaceNodeAttributes(List<NodeToAttributes> nodeToAttribute)
      throws IOException;

  /**
   * Add attribute to node.
   *
   * @param nodeToAttribute node to attribute list.
   * @throws IOException
   */
  void addNodeAttributes(List<NodeToAttributes> nodeToAttribute)
      throws IOException;

  /**
   * Remove attribute from node.
   *
   * @param nodeToAttribute node to attribute list.
   * @throws IOException
   */
  void removeNodeAttributes(List<NodeToAttributes> nodeToAttribute)
      throws IOException;

  /**
   * Initialize based on configuration and NodeAttributesManager.
   *
   * @param configuration configuration instance.
   * @param mgr nodeattributemanager instance.
   * @throws Exception
   */
  void init(Configuration configuration, NodeAttributesManager mgr)
      throws Exception;

  /**
   * Recover store on resourcemanager startup.
   * @throws IOException
   * @throws YarnException
   */
  void recover() throws IOException, YarnException;
}
