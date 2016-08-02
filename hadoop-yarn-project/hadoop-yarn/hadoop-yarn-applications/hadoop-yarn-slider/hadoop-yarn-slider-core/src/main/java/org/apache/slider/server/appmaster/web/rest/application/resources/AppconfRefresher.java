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

import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.persist.ConfTreeSerDeser;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;

/**
 * refresher for resources and application configuration
 */
public class AppconfRefresher
    implements ResourceRefresher<ConfTree> {

  private final StateAccessForProviders state;
  private final boolean unresolved;
  private final boolean resources;

  public AppconfRefresher(StateAccessForProviders state,
      boolean unresolved,
      boolean resources) {
    this.state = state;
    this.unresolved = unresolved;
    this.resources = resources;
  }


  @Override
  public ConfTree refresh() throws Exception {
    AggregateConf aggregateConf =
        unresolved ?
        state.getUnresolvedInstanceDefinition():
        state.getInstanceDefinitionSnapshot();
    ConfTree ct = resources ? aggregateConf.getResources() 
                            : aggregateConf.getAppConf();
    return new ConfTreeSerDeser().fromInstance(ct);
  }
}
