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
package org.apache.hadoop.mapreduce.lib.output;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Interface for an {@link org.apache.hadoop.mapreduce.OutputCommitter}
 * implementing partial commit of task output, as during preemption.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface PartialOutputCommitter {

  /**
   * Remove all previously committed outputs from prior executions of this task.
   * @param context Context for cleaning up previously promoted output.
   * @throws IOException If cleanup fails, then the state of the task my not be
   *                     well defined.
   */
  public void cleanUpPartialOutputForTask(TaskAttemptContext context)
    throws IOException;

}