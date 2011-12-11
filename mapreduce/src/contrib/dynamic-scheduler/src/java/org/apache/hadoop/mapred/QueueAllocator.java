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

import java.util.Map;
/**
 * This interface is intended for allowing schedulers to 
 * communicate with the queue share management implementation.
 * Schedulers can periodically poll this interface to
 * obtain the latest queue allocations.
 */
public interface QueueAllocator {
  /**
   * Used by schedulers to obtain queue allocations periodically
   * @return hashtable of queue names and their allocations (shares)
   */
  Map<String,QueueAllocation> getAllocation();
  /**
   * Used by schedulers to push queue usage info for
   * accounting purposes.
   * @param queue the queue name
   * @param used of slots currently used
   * @param pending number of tasks pending
   */
  void setUsage(String queue, int used, int pending);
}
