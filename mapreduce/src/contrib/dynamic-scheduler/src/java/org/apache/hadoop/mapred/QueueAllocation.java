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
package org.apache.hadoop.mapred;

/**
 * Class to hold queue share info to be
 * communicated between scheduler and 
 * queue share manager
 */
public class QueueAllocation {
  private String name;
  private float share;
  /**
   * @param name queue name
   * @param share queue share of total capacity (0..1)
   */
  public QueueAllocation(String name, float share) {
    this.name = name;
    this.share = share;
  }
  /**
   * Gets queue share
   * @return queue share of total capacity (0..1)
   */
  public float getShare() {
    return this.share;
  }
  /**
   * Gets queue name
   * @return queue name
   */
  public String getName() {
    return this.name;
  }
}
