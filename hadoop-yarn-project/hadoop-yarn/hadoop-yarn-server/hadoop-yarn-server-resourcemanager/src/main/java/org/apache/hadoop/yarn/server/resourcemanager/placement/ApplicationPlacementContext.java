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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

/**
 * Each placement rule when it successfully places an application onto a queue
 * returns a PlacementRuleContext which encapsulates the queue the
 * application was mapped to and any parent queue for the queue (if configured)
 */
public class ApplicationPlacementContext {

  private String queue;

  private String parentQueue;

  public ApplicationPlacementContext(String queue) {
    this(queue,null);
  }

  public ApplicationPlacementContext(String queue, String parentQueue) {
    this.queue = queue;
    this.parentQueue = parentQueue;
  }

  public String getQueue() {
    return queue;
  }

  public String getParentQueue() {
    return parentQueue;
  }

  public boolean hasParentQueue() {
    return parentQueue != null;
  }
}
