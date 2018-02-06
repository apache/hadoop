/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocationfile;

import com.google.common.collect.Lists;


/**
 * Queue builder that can build a subqueue with its properties.
 */
public class AllocationFileSubQueueBuilder extends AllocationFileQueueBuilder {
  private AllocationFileSimpleQueueBuilder parentQueueBuilder;

  AllocationFileSubQueueBuilder(
      AllocationFileSimpleQueueBuilder parentQueueBuilder, String queueName) {
    getqueuePropertiesBuilder().queueName(queueName);
    this.parentQueueBuilder = parentQueueBuilder;
  }

  @Override
  public AllocationFileWriter buildQueue() {
    throw new IllegalStateException(
        "BuildQueue is not supported in " + getClass());
  }

  public AllocationFileSimpleQueueBuilder buildSubQueue() {
    AllocationFileQueueProperties queueProperties =
            getqueuePropertiesBuilder().build();
    AllocationFileQueue queue =
        new AllocationFileQueue(queueProperties, Lists.newArrayList());

    if (parentQueueBuilder != null) {
      parentQueueBuilder.addSubQueue(queue);
      return parentQueueBuilder;
    } else {
      throw new IllegalStateException(
          "parentQueueBuilder field has to be set on a " + getClass());
    }
  }
}
