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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * Plugins to handle resources on a node. This will be used by
 * {@link org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater}
 */
public abstract class NodeResourceUpdaterPlugin {
  /**
   * Update configured resource for the given component.
   * @param res resource passed in by external module (such as
   *            {@link org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater}
   * @throws YarnException when any issue happens.
   */
  public abstract void updateConfiguredResource(Resource res)
      throws YarnException;

}