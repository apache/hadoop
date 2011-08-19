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
import org.apache.hadoop.util.Progressable;

/**
 * The context for task attempts.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface TaskAttemptContext extends JobContext, Progressable {

  /**
   * Get the unique name for this task attempt.
   */
  public TaskAttemptID getTaskAttemptID();

  /**
   * Set the current status of the task to the given string.
   */
  public void setStatus(String msg);

  /**
   * Get the last set status message.
   * @return the current status message
   */
  public String getStatus();
  
  /**
   * The current progress of the task attempt.
   * @return a number between 0.0 and 1.0 (inclusive) indicating the attempt's
   * progress.
   */
  public abstract float getProgress();

  /**
   * Get the {@link Counter} for the given <code>counterName</code>.
   * @param counterName counter name
   * @return the <code>Counter</code> for the given <code>counterName</code>
   */
  public Counter getCounter(Enum<?> counterName);

  /**
   * Get the {@link Counter} for the given <code>groupName</code> and 
   * <code>counterName</code>.
   * @param counterName counter name
   * @return the <code>Counter</code> for the given <code>groupName</code> and 
   *         <code>counterName</code>
   */
  public Counter getCounter(String groupName, String counterName);

}