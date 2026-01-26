/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode.startupprogress;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A step performed by the namenode during a {@link Phase} of startup.
 */
@InterfaceAudience.Private
public class Step implements Comparable<Step> {
  private static final AtomicInteger SEQUENCE = new AtomicInteger();

  private final String file;
  private final int sequenceNumber;
  private final long size;
  private final StepType type;

  /**
   * Creates a new Step.
   * 
   * @param type StepType type of step
   */
  public Step(StepType type) {
    this(type, null, Long.MIN_VALUE);
  }

  /**
   * Creates a new Step.
   * 
   * @param file String file
   */
  public Step(String file) {
    this(null, file, Long.MIN_VALUE);
  }

  /**
   * Creates a new Step.
   * 
   * @param file String file
   * @param size long size in bytes
   */
  public Step(String file, long size) {
    this(null, file, size);
  }

  /**
   * Creates a new Step.
   * 
   * @param type StepType type of step
   * @param file String file
   */
  public Step(StepType type, String file) {
    this(type, file, Long.MIN_VALUE);
  }

  /**
   * Creates a new Step.
   * 
   * @param type StepType type of step
   * @param file String file
   * @param size long size in bytes
   */
  public Step(StepType type, String file, long size) {
    this.file = file;
    this.sequenceNumber = SEQUENCE.incrementAndGet();
    this.size = size;
    this.type = type;
  }

  @Override
  public int compareTo(Step other) {
    // Sort steps by file and then sequentially within the file to achieve the
    // desired order.  There is no concurrent map structure in the JDK that
    // maintains insertion order, so instead we attach a sequence number to each
    // step and sort on read.
    return new CompareToBuilder().append(file, other.file)
      .append(sequenceNumber, other.sequenceNumber).toComparison();
  }

  @Override
  public boolean equals(Object otherObj) {
    if (otherObj == null || otherObj.getClass() != getClass()) {
      return false;
    }
    Step other = (Step)otherObj;
    return new EqualsBuilder().append(this.file, other.file)
      .append(this.size, other.size).append(this.type, other.type).isEquals();
  }

  /**
   * Returns the optional file name, possibly null.
   * 
   * @return String optional file name, possibly null
   */
  public String getFile() {
    return file;
  }

  /**
   * Returns the optional size in bytes, possibly Long.MIN_VALUE if undefined.
   * 
   * @return long optional size in bytes, possibly Long.MIN_VALUE
   */
  public long getSize() {
    return size;
  }

  /**
   * Returns the optional step type, possibly null.
   * 
   * @return StepType optional step type, possibly null
   */
  public StepType getType() {
    return type;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(file).append(size).append(type)
      .toHashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("file", file)
        .append("sequenceNumber", sequenceNumber)
        .append("size", size)
        .append("type", type)
        .toString();
  }
}
