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
package org.apache.hadoop.hdfs.server.namenode.sps;

import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A class which holds the SPS invoked path ids.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SPSPathIds {

  // List of pending dir to satisfy the policy
  private final Queue<Long> spsDirsToBeTraveresed = new LinkedList<Long>();

  /**
   * Add the path id to queue.
   */
  public synchronized void add(long pathId) {
    spsDirsToBeTraveresed.add(pathId);
  }

  /**
   * Removes the path id.
   */
  public synchronized void remove(long pathId) {
    spsDirsToBeTraveresed.remove(pathId);
  }

  /**
   * Clears all path ids.
   */
  public synchronized void clear() {
    spsDirsToBeTraveresed.clear();
  }

  /**
   * @return next path id available in queue.
   */
  public synchronized Long pollNext() {
    return spsDirsToBeTraveresed.poll();
  }
}
