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

public class QueueMappingEntity {
  private String source;
  private String queue;
  private String parentQueue;

  public final static String DELIMITER = ":";

  public QueueMappingEntity(String source, String queue) {
    this.source = source;
    this.queue = queue;
    this.parentQueue = null;
  }
  public QueueMappingEntity(String source, String queue, String parentQueue) {
    this.source = source;
    this.queue = queue;
    this.parentQueue = parentQueue;
  }

  public String getQueue() {
    return queue;
  }

  public String getParentQueue() {
    return parentQueue;
  }

  public String getSource() {
    return source;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof QueueMappingEntity) {
      QueueMappingEntity other = (QueueMappingEntity) obj;
      return (other.source.equals(source) &&
          other.queue.equals(queue));
    } else {
      return false;
    }
  }

  public String toString() {
    return source + DELIMITER + (parentQueue != null ?
        parentQueue + "." + queue :
        queue);
  }
}
