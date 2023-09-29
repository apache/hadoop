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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

/**
 * IteratorSelector contains information needed to tell an
 * {@link OrderingPolicy} what to return in an iterator.
 */
public class IteratorSelector {

  public static final IteratorSelector EMPTY_ITERATOR_SELECTOR =
      new IteratorSelector();

  private String partition;

  /**
   * The partition for this iterator selector.
   * @return partition
   */
  public String getPartition() {
    return this.partition;
  }

  /**
   * Set partition for this iterator selector.
   * @param p partition
   */
  public void setPartition(String p) {
    this.partition = p;
  }

}
