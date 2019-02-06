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
package org.apache.hadoop.hdds.scm.container.placement.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.web.utils.JsonUtils;

import java.io.IOException;

/**
 * This class represents the SCM container stat.
 */
public class ContainerStat {
  /**
   * The maximum container size.
   */
  @JsonProperty("Size")
  private LongMetric size;

  /**
   * The number of bytes used by the container.
   */
  @JsonProperty("Used")
  private LongMetric used;

  /**
   * The number of keys in the container.
   */
  @JsonProperty("KeyCount")
  private LongMetric keyCount;

  /**
   * The number of bytes read from the container.
   */
  @JsonProperty("ReadBytes")
  private LongMetric readBytes;

  /**
   * The number of bytes write into the container.
   */
  @JsonProperty("WriteBytes")
  private LongMetric writeBytes;

  /**
   * The number of times the container is read.
   */
  @JsonProperty("ReadCount")
  private LongMetric readCount;

  /**
   * The number of times the container is written into.
   */
  @JsonProperty("WriteCount")
  private LongMetric writeCount;

  public ContainerStat() {
    this(0L, 0L, 0L, 0L, 0L, 0L, 0L);
  }

  public ContainerStat(long size, long used, long keyCount, long readBytes,
      long writeBytes, long readCount, long writeCount) {
    Preconditions.checkArgument(size >= 0,
        "Container size cannot be " + "negative.");
    Preconditions.checkArgument(used >= 0,
        "Used space cannot be " + "negative.");
    Preconditions.checkArgument(keyCount >= 0,
        "Key count cannot be " + "negative");
    Preconditions.checkArgument(readBytes >= 0,
        "Read bytes read cannot be " + "negative.");
    Preconditions.checkArgument(readBytes >= 0,
        "Write bytes cannot be " + "negative.");
    Preconditions.checkArgument(readCount >= 0,
        "Read count cannot be " + "negative.");
    Preconditions.checkArgument(writeCount >= 0,
        "Write count cannot be " + "negative");

    this.size = new LongMetric(size);
    this.used = new LongMetric(used);
    this.keyCount = new LongMetric(keyCount);
    this.readBytes = new LongMetric(readBytes);
    this.writeBytes = new LongMetric(writeBytes);
    this.readCount = new LongMetric(readCount);
    this.writeCount = new LongMetric(writeCount);
  }

  public LongMetric getSize() {
    return size;
  }

  public LongMetric getUsed() {
    return used;
  }

  public LongMetric getKeyCount() {
    return keyCount;
  }

  public LongMetric getReadBytes() {
    return readBytes;
  }

  public LongMetric getWriteBytes() {
    return writeBytes;
  }

  public LongMetric getReadCount() {
    return readCount;
  }

  public LongMetric getWriteCount() {
    return writeCount;
  }

  public void add(ContainerStat stat) {
    if (stat == null) {
      return;
    }

    this.size.add(stat.getSize().get());
    this.used.add(stat.getUsed().get());
    this.keyCount.add(stat.getKeyCount().get());
    this.readBytes.add(stat.getReadBytes().get());
    this.writeBytes.add(stat.getWriteBytes().get());
    this.readCount.add(stat.getReadCount().get());
    this.writeCount.add(stat.getWriteCount().get());
  }

  public void subtract(ContainerStat stat) {
    if (stat == null) {
      return;
    }

    this.size.subtract(stat.getSize().get());
    this.used.subtract(stat.getUsed().get());
    this.keyCount.subtract(stat.getKeyCount().get());
    this.readBytes.subtract(stat.getReadBytes().get());
    this.writeBytes.subtract(stat.getWriteBytes().get());
    this.readCount.subtract(stat.getReadCount().get());
    this.writeCount.subtract(stat.getWriteCount().get());
  }

  public String toJsonString() {
    try {
      return JsonUtils.toJsonString(this);
    } catch (IOException ignored) {
      return null;
    }
  }
}