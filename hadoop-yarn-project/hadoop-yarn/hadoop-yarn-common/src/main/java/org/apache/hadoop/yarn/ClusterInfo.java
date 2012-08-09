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

package org.apache.hadoop.yarn;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

public class ClusterInfo {
  private Resource minContainerCapability;
  private Resource maxContainerCapability;

  public ClusterInfo() {
    this.minContainerCapability = Records.newRecord(Resource.class);
    this.maxContainerCapability = Records.newRecord(Resource.class);
  }

  public ClusterInfo(Resource minCapability, Resource maxCapability) {
    this.minContainerCapability = minCapability;
    this.maxContainerCapability = maxCapability;
  }

  public Resource getMinContainerCapability() {
    return minContainerCapability;
  }

  public void setMinContainerCapability(Resource minContainerCapability) {
    this.minContainerCapability = minContainerCapability;
  }

  public Resource getMaxContainerCapability() {
    return maxContainerCapability;
  }

  public void setMaxContainerCapability(Resource maxContainerCapability) {
    this.maxContainerCapability = maxContainerCapability;
  }
}