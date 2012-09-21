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

package org.apache.hadoop.mapreduce.v2.app2.rm.container;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.jobhistory.ContainerHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.TaskAttemptListener;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;

public class AMContainerMap extends AbstractService implements
    EventHandler<AMContainerEvent> {

  private static final Log LOG = LogFactory.getLog(AMContainerMap.class);

  private final ContainerHeartbeatHandler chh;
  private final TaskAttemptListener tal;
  private final AppContext context;
  private final ConcurrentHashMap<ContainerId, AMContainer> containerMap;

  public AMContainerMap(ContainerHeartbeatHandler chh, TaskAttemptListener tal,
      AppContext context) {
    super("AMContainerMaps");
    this.chh = chh;
    this.tal = tal;
    this.context = context;
    this.containerMap = new ConcurrentHashMap<ContainerId, AMContainer>();
  }

  @Override
  public void handle(AMContainerEvent event) {
    ContainerId containerId = event.getContainerId();
    containerMap.get(containerId).handle(event);
  }

  public void addContainerIfNew(Container container) {
    AMContainer amc = new AMContainerImpl(container, chh, tal, context);
    containerMap.putIfAbsent(container.getId(), amc);
  }

  public AMContainer get(ContainerId containerId) {
    return containerMap.get(containerId);
  }

  public Collection<AMContainer> values() {
    return containerMap.values();
  }
}