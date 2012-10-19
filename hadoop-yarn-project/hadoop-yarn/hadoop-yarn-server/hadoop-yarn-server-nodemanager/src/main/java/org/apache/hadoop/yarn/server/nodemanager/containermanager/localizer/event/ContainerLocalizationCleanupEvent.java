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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event;

import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;

public class ContainerLocalizationCleanupEvent extends
    ContainerLocalizationEvent {

  private final Map<LocalResourceVisibility, Collection<LocalResourceRequest>> 
    rsrc;

  /**
   * Event requesting the cleanup of the rsrc.
   * @param c
   * @param rsrc
   */
  public ContainerLocalizationCleanupEvent(Container c,
      Map<LocalResourceVisibility, Collection<LocalResourceRequest>> rsrc) {
    super(LocalizationEventType.CLEANUP_CONTAINER_RESOURCES, c);
    this.rsrc = rsrc;
  }

  public
      Map<LocalResourceVisibility, Collection<LocalResourceRequest>>
      getResources() {
    return rsrc;
  }
}