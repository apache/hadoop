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

package org.apache.hadoop.yarn.submarine.common.api;

import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Container;
import org.apache.hadoop.yarn.service.api.records.ContainerState;

/**
 * Status of component of training job
 */
public class JobComponentStatus {
  private String compName;
  private long numReadyContainers = 0;
  private long numRunningButUnreadyContainers = 0;
  private long totalAskedContainers;

  public JobComponentStatus(String compName, long nReadyContainers,
      long nRunningButUnreadyContainers, long totalAskedContainers) {
    this.compName = compName;
    this.numReadyContainers = nReadyContainers;
    this.numRunningButUnreadyContainers = nRunningButUnreadyContainers;
    this.totalAskedContainers = totalAskedContainers;
  }

  public String getCompName() {
    return compName;
  }

  public void setCompName(String compName) {
    this.compName = compName;
  }

  public long getNumReadyContainers() {
    return numReadyContainers;
  }

  public void setNumReadyContainers(long numReadyContainers) {
    this.numReadyContainers = numReadyContainers;
  }

  public long getNumRunningButUnreadyContainers() {
    return numRunningButUnreadyContainers;
  }

  public void setNumRunningButUnreadyContainers(
      long numRunningButUnreadyContainers) {
    this.numRunningButUnreadyContainers = numRunningButUnreadyContainers;
  }

  public long getTotalAskedContainers() {
    return totalAskedContainers;
  }

  public void setTotalAskedContainers(long totalAskedContainers) {
    this.totalAskedContainers = totalAskedContainers;
  }
}
