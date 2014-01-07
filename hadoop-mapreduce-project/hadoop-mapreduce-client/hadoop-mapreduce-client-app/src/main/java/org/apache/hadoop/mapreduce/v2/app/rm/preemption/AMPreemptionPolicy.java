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
package org.apache.hadoop.mapreduce.v2.app.rm.preemption;

import java.util.List;

import org.apache.hadoop.mapreduce.checkpoint.TaskCheckpointID;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;

/**
 * Policy encoding the {@link org.apache.hadoop.mapreduce.v2.app.MRAppMaster}
 * response to preemption requests from the ResourceManager.
 * @see org.apache.hadoop.mapreduce.v2.app.rm.RMContainerAllocator
 */
public interface AMPreemptionPolicy {

  public abstract class Context {

    /**
     * @param container ID of container to preempt
     * @return Task associated with the running container or <code>null</code>
     * if no task is bound to that container.
     */
    public abstract TaskAttemptId getTaskAttempt(ContainerId container);

    /**
     * Method provides the complete list of containers running task of type t
     * for this AM.
     * @param t the type of containers
     * @return a map containing
     */
    public abstract List<Container> getContainers(TaskType t);

  }

  public void init(AppContext context);

  /**
   * Callback informing the policy of ResourceManager. requests for resources
   * to return to the cluster. The policy may take arbitrary action to satisfy
   * requests by checkpointing task state, returning containers, or ignoring
   * requests. The RM may elect to enforce these requests by forcibly killing
   * containers not returned after some duration.
   * @param context Handle to the current state of running containers
   * @param preemptionRequests Request from RM for resources to return.
   */
  public void preempt(Context context, PreemptionMessage preemptionRequests);

  /**
   * This method is invoked by components interested to learn whether a certain
   * task is being preempted.
   * @param attemptID Task attempt to query
   * @return true if this attempt is being preempted
   */
  public boolean isPreempted(TaskAttemptId attemptID);

  /**
   * This method is used to report to the policy that a certain task has been
   * successfully preempted (for bookeeping, counters, etc..)
   * @param attemptID Task attempt that preempted
   */
  public void reportSuccessfulPreemption(TaskAttemptId attemptID);

  /**
   * Callback informing the policy of containers exiting with a failure. This
   * allows the policy to implemnt cleanup/compensating actions.
   * @param attemptID Task attempt that failed
   */
  public void handleFailedContainer(TaskAttemptId attemptID);

  /**
   * Callback informing the policy of containers exiting cleanly. This is
   * reported to the policy for bookeeping purposes.
   * @param attemptID Task attempt that completed
   */
  public void handleCompletedContainer(TaskAttemptId attemptID);

  /**
   * Method to retrieve the latest checkpoint for a given {@link TaskId}
   * @param taskId TaskID
   * @return CheckpointID associated with this task or null
   */
  public TaskCheckpointID getCheckpointID(TaskId taskId);

  /**
   * Method to store the latest {@link
   * org.apache.hadoop.mapreduce.checkpoint.CheckpointID} for a given {@link
   * TaskId}. Assigning a null is akin to remove all previous checkpoints for
   * this task.
   * @param taskId TaskID
   * @param cid Checkpoint to assign or <tt>null</tt> to remove it.
   */
  public void setCheckpointID(TaskId taskId, TaskCheckpointID cid);

}
