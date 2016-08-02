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

package org.apache.slider.server.appmaster.web.rest.application.resources;

import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;

public class ResourceSnapshotRefresher implements ResourceRefresher<ConfTree> {

  private final StateAccessForProviders state;

  public ResourceSnapshotRefresher(StateAccessForProviders state) {
    this.state = state;
  }

  @Override
  public ConfTree refresh() throws Exception {

    // snapshot resources
    ConfTreeOperations resources = state.getResourcesSnapshot();
      return resources.getConfTree();
  }
}
