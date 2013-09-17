/*
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

package org.apache.hadoop.mapreduce.counters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.StringInterner;

/**
 * A generic counter implementation
 */
@InterfaceAudience.Private
public class GenericCounter extends AbstractCounter {

  private String name;
  private String displayName;
  private long value = 0;

  public GenericCounter() {
    // mostly for readFields
  }

  public GenericCounter(String name, String displayName) {
    this.name = name;
    this.displayName = displayName;
  }

  public GenericCounter(String name, String displayName, long value) {
    this.name = name;
    this.displayName = displayName;
    this.value = value;
  }

  @Override @Deprecated
  public synchronized void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  @Override
  public synchronized void readFields(DataInput in) throws IOException {
    name = StringInterner.weakIntern(Text.readString(in));
    displayName = in.readBoolean() ? 
        StringInterner.weakIntern(Text.readString(in)) : name;
    value = WritableUtils.readVLong(in);
  }

  /**
   * GenericCounter ::= keyName isDistinctDisplayName [displayName] value
   */
  @Override
  public synchronized void write(DataOutput out) throws IOException {
    Text.writeString(out, name);
    boolean distinctDisplayName = ! name.equals(displayName);
    out.writeBoolean(distinctDisplayName);
    if (distinctDisplayName) {
      Text.writeString(out, displayName);
    }
    WritableUtils.writeVLong(out, value);
  }

  @Override
  public synchronized String getName() {
    return name;
  }

  @Override
  public synchronized String getDisplayName() {
    return displayName;
  }

  @Override
  public synchronized long getValue() {
    return value;
  }

  @Override
  public synchronized void setValue(long value) {
    this.value = value;
  }

  @Override
  public synchronized void increment(long incr) {
    value += incr;
  }

  @Override
  public Counter getUnderlyingCounter() {
    return this;
  }
}
