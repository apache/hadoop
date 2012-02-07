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

package org.apache.hadoop.mapreduce;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;

/**
 * A named counter that tracks the progress of a map/reduce job.
 *
 * <p><code>Counters</code> represent global counters, defined either by the
 * Map-Reduce framework or applications. Each <code>Counter</code> is named by
 * an {@link Enum} and has a long for the value.</p>
 *
 * <p><code>Counters</code> are bunched into Groups, each comprising of
 * counters from a particular <code>Enum</code> class.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Counter extends Writable {

  /**
   * Set the display name of the counter
   * @param displayName of the counter
   * @deprecated (and no-op by default)
   */
  @Deprecated
  void setDisplayName(String displayName);

  /**
   * @return the name of the counter
   */
  String getName();

  /**
   * Get the display name of the counter.
   * @return the user facing name of the counter
   */
  String getDisplayName();

  /**
   * What is the current value of this counter?
   * @return the current value
   */
  long getValue();

  /**
   * Set this counter by the given value
   * @param value the value to set
   */
  void setValue(long value);

  /**
   * Increment this counter by the given value
   * @param incr the value to increase this counter by
   */
  void increment(long incr);
 
  /**
   * Return the underlying object if this is a facade.
   * @return the undelying object.
   */
  Counter getUnderlyingCounter();
}
