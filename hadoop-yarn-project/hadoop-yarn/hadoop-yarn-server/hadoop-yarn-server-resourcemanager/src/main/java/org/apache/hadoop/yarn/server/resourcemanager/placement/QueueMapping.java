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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;

/**
 * Queue Mapping class to hold the queue mapping information.
 *
 */
@Private
public class QueueMapping {

  /**
   * Builder class for QueueMapping.
   *
   */
  public static class QueueMappingBuilder {

    private MappingType type;
    private String source;
    private String queue;
    private String parentQueue;

    public QueueMappingBuilder() {
    }

    public static QueueMappingBuilder create() {
      return new QueueMappingBuilder();
    }

    public QueueMappingBuilder type(MappingType mappingType) {
      this.type = mappingType;
      return this;
    }

    public QueueMappingBuilder source(String mappingSource) {
      this.source = mappingSource;
      return this;
    }

    public QueueMappingBuilder queue(String mappingQueue) {
      this.queue = mappingQueue;
      return this;
    }

    public QueueMappingBuilder parentQueue(String mappingParentQueue) {
      this.parentQueue = mappingParentQueue;
      return this;
    }

    public QueueMappingBuilder parsePathString(String queuePath) {
      int parentQueueNameEndIndex = queuePath.lastIndexOf(DOT);

      if (parentQueueNameEndIndex > -1) {
        final String parentQueue =
            queuePath.substring(0, parentQueueNameEndIndex).trim();
        final String leafQueue =
            queuePath.substring(parentQueueNameEndIndex + 1).trim();
        return this
            .parentQueue(parentQueue)
            .queue(leafQueue);
      }

      return this.queue(queuePath);
    }

    public QueueMapping build() {
      return new QueueMapping(this);
    }
  }

  private QueueMapping(QueueMappingBuilder builder) {
    this.type = builder.type;
    this.source = builder.source;
    this.queue = builder.queue;
    this.parentQueue = builder.parentQueue;
    this.fullPath = (parentQueue != null) ? (parentQueue + DOT + queue) : queue;
  }

  /**
   * Different types of mapping.
   *
   */
  public enum MappingType {
    USER("u"),
    GROUP("g"),
    APPLICATION("a");

    private final String type;

    MappingType(String type) {
      this.type = type;
    }

    public String toString() {
      return type;
    }

  };

  private MappingType type;
  private String source;
  private String queue;
  private String parentQueue;
  private String fullPath;

  private final static String DELIMITER = ":";

  public String getQueue() {
    return queue;
  }

  public String getParentQueue() {
    return parentQueue;
  }

  public boolean hasParentQueue() {
    return parentQueue != null;
  }

  public MappingType getType() {
    return type;
  }

  public String getSource() {
    return source;
  }

  public String getFullPath() {
    return fullPath;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result =
        prime * result + ((parentQueue == null) ? 0 : parentQueue.hashCode());
    result = prime * result + ((queue == null) ? 0 : queue.hashCode());
    result = prime * result + ((source == null) ? 0 : source.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    QueueMapping other = (QueueMapping) obj;
    if (parentQueue == null) {
      if (other.parentQueue != null) {
        return false;
      }
    } else if (!parentQueue.equals(other.parentQueue)) {
      return false;
    }
    if (queue == null) {
      if (other.queue != null) {
        return false;
      }
    } else if (!queue.equals(other.queue)) {
      return false;
    }
    if (source == null) {
      if (other.source != null) {
        return false;
      }
    } else if (!source.equals(other.source)) {
      return false;
    }
    if (type != other.type) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return type.toString() + DELIMITER + source + DELIMITER
        + (parentQueue != null ? parentQueue + "." + queue : queue);
  }

  public String toTypelessString() {
    return source + DELIMITER
        + (parentQueue != null ? parentQueue + "." + queue : queue);
  }

}