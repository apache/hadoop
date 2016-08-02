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

package org.apache.slider.server.appmaster.actions;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.slider.server.appmaster.SliderAppMaster;
import org.apache.slider.server.appmaster.state.AppState;
import org.apache.slider.server.appmaster.state.RoleInstance;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Start a container
 * @see SliderAppMaster#startContainer(Container, ContainerLaunchContext, RoleInstance) 
 */
public class ActionStartContainer extends AsyncAction {

  private final Container container;
  private final ContainerLaunchContext ctx;
  private final RoleInstance instance;

  public ActionStartContainer(String name,
      Container container,
      ContainerLaunchContext ctx,
      RoleInstance instance,
      long delay, TimeUnit timeUnit) {
    super(
        String.format(Locale.ENGLISH,
            "%s %s: /",
            name , container.getId().toString()), 
        delay, 
        timeUnit);
    this.container = container;
    this.ctx = ctx;
    this.instance = instance;
  }

  @Override
  public void execute(SliderAppMaster appMaster,
      QueueAccess queueService,
      AppState appState) throws Exception {
    appMaster.startContainer(container, ctx, instance);
  }
}
