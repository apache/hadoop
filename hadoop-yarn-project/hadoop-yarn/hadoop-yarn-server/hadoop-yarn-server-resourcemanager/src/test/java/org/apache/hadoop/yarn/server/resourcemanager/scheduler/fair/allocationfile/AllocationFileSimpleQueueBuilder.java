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

import java.util.ArrayList;
import java.util.List;

/**
 * Queue builder that can build a simple queue with its properties.
 * Subqueues can be added with {@link #addSubQueue(AllocationFileQueue)}.
 */
public class AllocationFileSimpleQueueBuilder
    extends AllocationFileQueueBuilder {
  private final AllocationFileWriter allocationFileWriter;
  private final List<AllocationFileQueue> subQueues = new ArrayList<>();

  AllocationFileSimpleQueueBuilder(AllocationFileWriter allocationFileWriter,
      String queueName) {
    this.allocationFileWriter = allocationFileWriter;
    getqueuePropertiesBuilder().queueName(queueName);
  }

  void addSubQueue(AllocationFileQueue queue) {
    subQueues.add(queue);
  }

  @Override
  public AllocationFileWriter buildQueue() {
    AllocationFileQueueProperties queueProperties =
            getqueuePropertiesBuilder().build();
    AllocationFileQueue queue =
        new AllocationFileQueue(queueProperties, subQueues);

    if (allocationFileWriter != null) {
      allocationFileWriter.addQueue(queue);
    } else {
      throw new IllegalStateException(
          "allocationFileWriter field has to be set on a " + getClass());
    }

    return allocationFileWriter;
  }

  @Override
  public AllocationFileSimpleQueueBuilder buildSubQueue() {
    throw new IllegalStateException(
        "buildSubQueue is not supported in " + getClass());
  }

}
