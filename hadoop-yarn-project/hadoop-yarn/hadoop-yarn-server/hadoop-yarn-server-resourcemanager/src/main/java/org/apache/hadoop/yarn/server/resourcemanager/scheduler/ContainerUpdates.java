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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;

import java.util.ArrayList;
import java.util.List;

/**
 * Holder class that maintains list of container update requests
 */
public class ContainerUpdates {

  final List<UpdateContainerRequest> increaseRequests = new ArrayList<>();
  final List<UpdateContainerRequest> decreaseRequests = new ArrayList<>();
  final List<UpdateContainerRequest> promotionRequests = new ArrayList<>();
  final List<UpdateContainerRequest> demotionRequests = new ArrayList<>();

  /**
   * Returns Container Increase Requests.
   * @return Container Increase Requests.
   */
  public List<UpdateContainerRequest> getIncreaseRequests() {
    return increaseRequests;
  }

  /**
   * Returns Container Decrease Requests.
   * @return Container Decrease Requests.
   */
  public List<UpdateContainerRequest> getDecreaseRequests() {
    return decreaseRequests;
  }

  /**
   * Returns Container Promotion Requests.
   * @return Container Promotion Requests.
   */
  public List<UpdateContainerRequest> getPromotionRequests() {
    return promotionRequests;
  }

  /**
   * Returns Container Demotion Requests.
   * @return Container Demotion Requests.
   */
  public List<UpdateContainerRequest> getDemotionRequests() {
    return demotionRequests;
  }

}
