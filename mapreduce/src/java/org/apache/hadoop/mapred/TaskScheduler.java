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
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

/**
 * Used by a {@link JobTracker} to schedule {@link Task}s on
 * {@link TaskTracker}s.
 * <p>
 * {@link TaskScheduler}s typically use one or more
 * {@link JobInProgressListener}s to receive notifications about jobs.
 * <p>
 * It is the responsibility of the {@link TaskScheduler}
 * to initialize tasks for a job, by calling {@link JobInProgress#initTasks()}
 * between the job being added (when
 * {@link JobInProgressListener#jobAdded(JobInProgress)} is called)
 * and tasks for that job being assigned (by
 * {@link #assignTasks(TaskTracker)}).
 * @see EagerTaskInitializationListener
 */
abstract class TaskScheduler implements Configurable {

  protected Configuration conf;
  protected TaskTrackerManager taskTrackerManager;
  
  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public synchronized void setTaskTrackerManager(
      TaskTrackerManager taskTrackerManager) {
    this.taskTrackerManager = taskTrackerManager;
  }
  
  /**
   * Lifecycle method to allow the scheduler to start any work in separate
   * threads.
   * @throws IOException
   */
  public void start() throws IOException {
    // do nothing
  }
  
  /**
   * Lifecycle method to allow the scheduler to stop any work it is doing.
   * @throws IOException
   */
  public void terminate() throws IOException {
    // do nothing
  }

  /**
   * Returns the tasks we'd like the TaskTracker to execute right now.
   * 
   * @param taskTracker The TaskTracker for which we're looking for tasks.
   * @return A list of tasks to run on that TaskTracker, possibly empty.
   */
  public abstract List<Task> assignTasks(TaskTracker taskTracker)
  throws IOException;

  /**
   * Returns a collection of jobs in an order which is specific to 
   * the particular scheduler.
   * @param queueName
   * @return
   */
  public abstract Collection<JobInProgress> getJobs(String queueName);

  /**
   * Abstract QueueRefresher class. Scheduler's can extend this and return an
   * instance of this in the {@link #getQueueRefresher()} method. The
   * {@link #refreshQueues(List)} method of this instance will be invoked by the
   * {@link QueueManager} whenever it gets a request from an administrator to
   * refresh its own queue-configuration. This method has a documented contract
   * between the {@link QueueManager} and the {@link TaskScheduler}.
   * 
   * Before calling QueueRefresher, the caller must hold the lock to the
   * corresponding {@link TaskScheduler} (generally in the {@link JobTracker}).
   */
  abstract class QueueRefresher {

    /**
     * Refresh the queue-configuration in the scheduler. This method has the
     * following contract.
     * <ol>
     * <li>Before this method, {@link QueueManager} does a validation of the new
     * queue-configuration. For e.g, currently addition of new queues, or
     * removal of queues at any level in the hierarchy is not supported by
     * {@link QueueManager} and so are not supported for schedulers too.</li>
     * <li>Schedulers will be passed a list of {@link JobQueueInfo}s of the root
     * queues i.e. the queues at the top level. All the descendants are properly
     * linked from these top-level queues.</li>
     * <li>Schedulers should use the scheduler specific queue properties from
     * the newRootQueues, validate the properties themselves and apply them
     * internally.</li>
     * <li>
     * Once the method returns successfully from the schedulers, it is assumed
     * that the refresh of queue properties is successful throughout and will be
     * 'committed' internally to {@link QueueManager} too. It is guaranteed that
     * at no point, after successful return from the scheduler, is the queue
     * refresh in QueueManager failed. If ever, such abnormalities happen, the
     * queue framework will be inconsistent and will need a JT restart.</li>
     * <li>If scheduler throws an exception during {@link #refreshQueues()},
     * {@link QueueManager} throws away the newly read configuration, retains
     * the old (consistent) configuration and informs the request issuer about
     * the error appropriately.</li>
     * </ol>
     * 
     * @param newRootQueues
     */
    abstract void refreshQueues(List<JobQueueInfo> newRootQueues)
        throws Throwable;
  }

  /**
   * Get the {@link QueueRefresher} for this scheduler. By default, no
   * {@link QueueRefresher} exists for a scheduler and is set to null.
   * Schedulers need to return an instance of {@link QueueRefresher} if they
   * wish to refresh their queue-configuration when {@link QueueManager}
   * refreshes its own queue-configuration via an administrator request.
   * 
   * @return
   */
  QueueRefresher getQueueRefresher() {
    return null;
  }
}
