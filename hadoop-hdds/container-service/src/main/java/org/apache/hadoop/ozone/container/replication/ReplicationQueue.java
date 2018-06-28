/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.container.replication;

import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Priority queue to handle under-replicated and over replicated containers
 * in ozone. ReplicationManager will consume these messages and decide
 * accordingly.
 */
public class ReplicationQueue {

  private final Queue<ReplicationRequest> queue;

  ReplicationQueue() {
    queue = new PriorityQueue<>();
  }

  public synchronized boolean add(ReplicationRequest repObj) {
    if (this.queue.contains(repObj)) {
      // Remove the earlier message and insert this one
      this.queue.remove(repObj);
    }
    return this.queue.add(repObj);
  }

  public synchronized boolean remove(ReplicationRequest repObj) {
    return queue.remove(repObj);
  }

  /**
   * Retrieves, but does not remove, the head of this queue,
   * or returns {@code null} if this queue is empty.
   *
   * @return the head of this queue, or {@code null} if this queue is empty
   */
  public synchronized ReplicationRequest peek() {
    return queue.peek();
  }

  /**
   * Retrieves and removes the head of this queue,
   * or returns {@code null} if this queue is empty.
   *
   * @return the head of this queue, or {@code null} if this queue is empty
   */
  public synchronized ReplicationRequest poll() {
    return queue.poll();
  }

  public synchronized boolean removeAll(List<ReplicationRequest> repObjs) {
    return queue.removeAll(repObjs);
  }

  public int size() {
    return queue.size();
  }
}
