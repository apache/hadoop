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
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

/**
 * The mutable metric interface
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class MutableMetric {
  private volatile boolean changed = true;

  /**
   * Get a snapshot of the metric
   * @param builder the metrics record builder
   * @param all if true, snapshot unchanged metrics as well
   */
  public abstract void snapshot(MetricsRecordBuilder builder, boolean all);

  /**
   * Get a snapshot of metric if changed
   * @param builder the metrics record builder
   */
  public void snapshot(MetricsRecordBuilder builder) {
    snapshot(builder, false);
  }

  /**
   * Set the changed flag in mutable operations
   */
  protected void setChanged() { changed = true; }

  /**
   * Clear the changed flag in the snapshot operations
   */
  protected void clearChanged() { changed = false; }

  /**
   * @return  true if metric is changed since last snapshot/snapshot
   */
  public boolean changed() { return changed; }
}
