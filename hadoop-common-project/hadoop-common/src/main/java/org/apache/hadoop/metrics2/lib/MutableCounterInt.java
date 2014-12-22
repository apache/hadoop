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

package org.apache.hadoop.metrics2.lib;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A mutable int counter for implementing metrics sources
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MutableCounterInt extends MutableCounter {
  private AtomicInteger value = new AtomicInteger();

  MutableCounterInt(MetricsInfo info, int initValue) {
    super(info);
    this.value.set(initValue);
  }

  @Override
  public void incr() {
    incr(1);
  }

  /**
   * Increment the value by a delta
   * @param delta of the increment
   */
  public synchronized void incr(int delta) {
    value.addAndGet(delta);
    setChanged();
  }

  public int value() {
    return value.get();
  }

  @Override
  public void snapshot(MetricsRecordBuilder builder, boolean all) {
    if (all || changed()) {
      builder.addCounter(info(), value());
      clearChanged();
    }
  }

}
