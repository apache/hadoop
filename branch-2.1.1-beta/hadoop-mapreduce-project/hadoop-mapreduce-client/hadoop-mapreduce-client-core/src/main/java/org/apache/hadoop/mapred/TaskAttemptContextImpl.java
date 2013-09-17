/* Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.hadoop.mapred;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.Progressable;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskAttemptContextImpl
       extends org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl 
       implements TaskAttemptContext {
  private Reporter reporter;

  public TaskAttemptContextImpl(JobConf conf, TaskAttemptID taskid) {
    this(conf, taskid, Reporter.NULL);
  }
  
  TaskAttemptContextImpl(JobConf conf, TaskAttemptID taskid,
                         Reporter reporter) {
    super(conf, taskid);
    this.reporter = reporter;
  }
  
  /**
   * Get the taskAttemptID.
   *  
   * @return TaskAttemptID
   */
  public TaskAttemptID getTaskAttemptID() {
    return (TaskAttemptID) super.getTaskAttemptID();
  }
  
  public Progressable getProgressible() {
    return reporter;
  }
  
  public JobConf getJobConf() {
    return (JobConf) getConfiguration();
  }
  
  @Override
  public float getProgress() {
    return reporter.getProgress();
  }

  @Override
  public Counter getCounter(Enum<?> counterName) {
    return reporter.getCounter(counterName);
  }

  @Override
  public Counter getCounter(String groupName, String counterName) {
    return reporter.getCounter(groupName, counterName);
  }

  /**
   * Report progress.
   */
  @Override
  public void progress() {
    reporter.progress();
  }

  /**
   * Set the current status of the task to the given string.
   */
  @Override
  public void setStatus(String status) {
    setStatusString(status);
    reporter.setStatus(status);
  }


}
