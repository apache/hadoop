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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import org.apache.hadoop.yarn.api.records.Resource;

public class RMAppMetrics {
  final Resource resourcePreempted;
  final int numNonAMContainersPreempted;
  final int numAMContainersPreempted;
  final long memorySeconds;
  final long vcoreSeconds;

  public RMAppMetrics(Resource resourcePreempted,
      int numNonAMContainersPreempted, int numAMContainersPreempted,
      long memorySeconds, long vcoreSeconds) {
    this.resourcePreempted = resourcePreempted;
    this.numNonAMContainersPreempted = numNonAMContainersPreempted;
    this.numAMContainersPreempted = numAMContainersPreempted;
    this.memorySeconds = memorySeconds;
    this.vcoreSeconds = vcoreSeconds;
  }

  public Resource getResourcePreempted() {
    return resourcePreempted;
  }

  public int getNumNonAMContainersPreempted() {
    return numNonAMContainersPreempted;
  }

  public int getNumAMContainersPreempted() {
    return numAMContainersPreempted;
  }

  public long getMemorySeconds() {
    return memorySeconds;
  }

  public long getVcoreSeconds() {
    return vcoreSeconds;
  }
}
