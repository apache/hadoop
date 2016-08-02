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

import com.google.common.base.Preconditions;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.slider.server.appmaster.SliderAppMaster;
import org.apache.slider.server.appmaster.state.AppState;

import java.util.concurrent.TimeUnit;

/**
 * Notify the app master that it should register a component instance
 * in the registry
 * {@link SliderAppMaster#registerComponent(ContainerId)}
 */
public class RegisterComponentInstance extends AsyncAction {

  public final ContainerId containerId;
  public final String description;
  public final String type;

  public RegisterComponentInstance(ContainerId containerId,
      String description,
      String type,
      long delay,
      TimeUnit timeUnit) {
    super("RegisterComponentInstance :" + containerId,
        delay, timeUnit);
    this.description = description;
    this.type = type;
    Preconditions.checkArgument(containerId != null);
    this.containerId = containerId;
  }

  @Override
  public void execute(SliderAppMaster appMaster,
      QueueAccess queueService,
      AppState appState) throws Exception {

    appMaster.registerComponent(containerId, description, type);
  }
}
