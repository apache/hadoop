/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task;

import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.mockito.ArgumentMatcher;

/**
 * ArgumentMatcher to check the arguments of the
 * {@link DockerContainerDeletionTask}.
 */
public class DockerContainerDeletionMatcher
    implements ArgumentMatcher<DockerContainerDeletionTask> {

  private final DeletionService delService;
  private final String containerId;

  public DockerContainerDeletionMatcher(DeletionService delService,
      String containerId) {
    this.delService = delService;
    this.containerId = containerId;
  }

  @Override
  public boolean matches(DockerContainerDeletionTask task) {
    if (task.getContainerId() == null && containerId == null) {
      return true;
    }
    if (task.getContainerId() != null && containerId != null) {
      return task.getContainerId().equals(containerId);
    }
    return false;
  }
}