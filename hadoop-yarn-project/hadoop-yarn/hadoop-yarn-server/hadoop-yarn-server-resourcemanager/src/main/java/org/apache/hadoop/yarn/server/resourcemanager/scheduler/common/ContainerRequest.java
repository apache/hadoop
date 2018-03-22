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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common;

import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;

import java.util.List;

/**
 * ContainerRequest is a class to capture resource requests associated with a
 * Container, this will be used by scheduler to recover resource requests if the
 * container preempted or cancelled before AM acquire the container.
 *
 * It should include deducted resource requests when the container allocated.
 *
 * Lifecycle of the ContainerRequest is:
 *
 * <pre>
 * 1) It is instantiated when container created.
 * 2) It will be set to ContainerImpl by scheduler.
 * 3) When container preempted or cancelled because of whatever reason before
 *    container acquired by AM. ContainerRequest will be added back to pending
 *    request pool.
 * 4) It will be cleared from ContainerImpl if the container already acquired by
 *    AM.
 * </pre>
 */
public class ContainerRequest {
  private List<ResourceRequest> requests;
  private SchedulingRequest schedulingRequest;

  public ContainerRequest(List<ResourceRequest> requests) {
    this.requests = requests;
    schedulingRequest = null;
  }

  public ContainerRequest(SchedulingRequest schedulingRequest) {
    this.schedulingRequest = schedulingRequest;
    this.requests = null;
  }

  public List<ResourceRequest> getResourceRequests() {
    return requests;
  }

  public SchedulingRequest getSchedulingRequest() {
    return schedulingRequest;
  }
}
