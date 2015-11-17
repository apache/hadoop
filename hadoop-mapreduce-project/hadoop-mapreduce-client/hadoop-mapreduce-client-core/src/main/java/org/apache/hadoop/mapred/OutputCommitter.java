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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * <code>OutputCommitter</code> describes the commit of task output for a 
 * Map-Reduce job.
 *
 * <p>The Map-Reduce framework relies on the <code>OutputCommitter</code> of 
 * the job to:<p>
 * <ol>
 *   <li>
 *   Setup the job during initialization. For example, create the temporary 
 *   output directory for the job during the initialization of the job.
 *   </li>
 *   <li>
 *   Cleanup the job after the job completion. For example, remove the
 *   temporary output directory after the job completion. 
 *   </li>
 *   <li>
 *   Setup the task temporary output.
 *   </li> 
 *   <li>
 *   Check whether a task needs a commit. This is to avoid the commit
 *   procedure if a task does not need commit.
 *   </li>
 *   <li>
 *   Commit of the task output.
 *   </li>  
 *   <li>
 *   Discard the task commit.
 *   </li>
 * </ol>
 * The methods in this class can be called from several different processes and
 * from several different contexts.  It is important to know which process and
 * which context each is called from.  Each method should be marked accordingly
 * in its documentation.  It is also important to note that not all methods are
 * guaranteed to be called once and only once.  If a method is not guaranteed to
 * have this property the output committer needs to handle this appropriately. 
 * Also note it will only be in rare situations where they may be called 
 * multiple times for the same task.
 * 
 * @see FileOutputCommitter 
 * @see JobContext
 * @see TaskAttemptContext 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class OutputCommitter 
                extends org.apache.hadoop.mapreduce.OutputCommitter {
  /**
   * For the framework to setup the job output during initialization.  This is
   * called from the application master process for the entire job. This will be
   * called multiple times, once per job attempt.
   * 
   * @param jobContext Context of the job whose output is being written.
   * @throws IOException if temporary output could not be created
   */
  public abstract void setupJob(JobContext jobContext) throws IOException;

  /**
   * For cleaning up the job's output after job completion.  This is called
   * from the application master process for the entire job. This may be called
   * multiple times.
   * 
   * @param jobContext Context of the job whose output is being written.
   * @throws IOException
   * @deprecated Use {@link #commitJob(JobContext)} or 
   *                 {@link #abortJob(JobContext, int)} instead.
   */
  @Deprecated
  public void cleanupJob(JobContext jobContext) throws IOException { }

  /**
   * For committing job's output after successful job completion. Note that this
   * is invoked for jobs with final runstate as SUCCESSFUL.  This is called
   * from the application master process for the entire job. This is guaranteed
   * to only be called once.  If it throws an exception the entire job will
   * fail.
   * 
   * @param jobContext Context of the job whose output is being written.
   * @throws IOException 
   */
  public void commitJob(JobContext jobContext) throws IOException {
    cleanupJob(jobContext);
  }
  
  /**
   * For aborting an unsuccessful job's output. Note that this is invoked for 
   * jobs with final runstate as {@link JobStatus#FAILED} or 
   * {@link JobStatus#KILLED}. This is called from the application
   * master process for the entire job. This may be called multiple times.
   * 
   * @param jobContext Context of the job whose output is being written.
   * @param status final runstate of the job
   * @throws IOException
   */
  public void abortJob(JobContext jobContext, int status) 
  throws IOException {
    cleanupJob(jobContext);
  }
  
  /**
   * Sets up output for the task. This is called from each individual task's
   * process that will output to HDFS, and it is called just for that task. This
   * may be called multiple times for the same task, but for different task
   * attempts.
   * 
   * @param taskContext Context of the task whose output is being written.
   * @throws IOException
   */
  public abstract void setupTask(TaskAttemptContext taskContext)
  throws IOException;
  
  /**
   * Check whether task needs a commit.  This is called from each individual
   * task's process that will output to HDFS, and it is called just for that
   * task.
   * 
   * @param taskContext
   * @return true/false
   * @throws IOException
   */
  public abstract boolean needsTaskCommit(TaskAttemptContext taskContext)
  throws IOException;

  /**
   * To promote the task's temporary output to final output location.
   * If {@link #needsTaskCommit(TaskAttemptContext)} returns true and this
   * task is the task that the AM determines finished first, this method
   * is called to commit an individual task's output.  This is to mark
   * that tasks output as complete, as {@link #commitJob(JobContext)} will 
   * also be called later on if the entire job finished successfully. This
   * is called from a task's process. This may be called multiple times for the
   * same task, but different task attempts.  It should be very rare for this to
   * be called multiple times and requires odd networking failures to make this
   * happen. In the future the Hadoop framework may eliminate this race.
   * 
   * @param taskContext Context of the task whose output is being written.
   * @throws IOException if commit is not 
   */
  public abstract void commitTask(TaskAttemptContext taskContext)
  throws IOException;
  
  /**
   * Discard the task output. This is called from a task's process to clean 
   * up a single task's output that can not yet been committed. This may be
   * called multiple times for the same task, but for different task attempts.
   * 
   * @param taskContext
   * @throws IOException
   */
  public abstract void abortTask(TaskAttemptContext taskContext)
  throws IOException;

  /**
   * This method implements the new interface by calling the old method. Note
   * that the input types are different between the new and old apis and this is
   * a bridge between the two.
   * 
   * @deprecated Use {@link #isRecoverySupported(JobContext)} instead.
   */
  @Deprecated
  @Override
  public boolean isRecoverySupported() {
    return false;
  }

  /**
   * Is task output recovery supported for restarting jobs?
   * 
   * If task output recovery is supported, job restart can be done more
   * efficiently.
   *
   * @param jobContext
   *          Context of the job whose output is being written.
   * @return <code>true</code> if task output recovery is supported,
   *         <code>false</code> otherwise
   * @throws IOException
   * @see #recoverTask(TaskAttemptContext)
   */
  public boolean isRecoverySupported(JobContext jobContext) throws IOException {
    return isRecoverySupported();
  }

  /**
   * Returns true if an in-progress job commit can be retried. If the MR AM is
   * re-run then it will check this value to determine if it can retry an
   * in-progress commit that was started by a previous version.
   * Note that in rare scenarios, the previous AM version might still be running
   * at that time, due to system anomalies. Hence if this method returns true
   * then the retry commit operation should be able to run concurrently with
   * the previous operation.
   *
   * If repeatable job commit is supported, job restart can tolerate previous
   * AM failures during job commit.
   *
   * By default, it is not supported. Extended classes (like:
   * FileOutputCommitter) should explicitly override it if provide support.
   *
   * @param jobContext
   *          Context of the job whose output is being written.
   * @return <code>true</code> repeatable job commit is supported,
   *         <code>false</code> otherwise
   * @throws IOException
   */
  public boolean isCommitJobRepeatable(JobContext jobContext) throws
      IOException {
    return false;
  }

  @Override
  public boolean isCommitJobRepeatable(org.apache.hadoop.mapreduce.JobContext
      jobContext) throws IOException {
    return isCommitJobRepeatable((JobContext) jobContext);
  }

  /**
   * Recover the task output. 
   * 
   * The retry-count for the job will be passed via the 
   * {@link MRConstants#APPLICATION_ATTEMPT_ID} key in  
   * {@link TaskAttemptContext#getConfiguration()} for the 
   * <code>OutputCommitter</code>. This is called from the application master
   * process, but it is called individually for each task.
   * 
   * If an exception is thrown the task will be attempted again. 
   * 
   * @param taskContext Context of the task whose output is being recovered
   * @throws IOException
   */
  public void recoverTask(TaskAttemptContext taskContext) 
  throws IOException {
  }
  
  /**
   * This method implements the new interface by calling the old method. Note
   * that the input types are different between the new and old apis and this
   * is a bridge between the two.
   */
  @Override
  public final void setupJob(org.apache.hadoop.mapreduce.JobContext jobContext
                             ) throws IOException {
    setupJob((JobContext) jobContext);
  }

  /**
   * This method implements the new interface by calling the old method. Note
   * that the input types are different between the new and old apis and this
   * is a bridge between the two.
   * @deprecated Use {@link #commitJob(org.apache.hadoop.mapreduce.JobContext)}
   *             or {@link #abortJob(org.apache.hadoop.mapreduce.JobContext, org.apache.hadoop.mapreduce.JobStatus.State)}
   *             instead.
   */
  @Override
  @Deprecated
  public final void cleanupJob(org.apache.hadoop.mapreduce.JobContext context
                               ) throws IOException {
    cleanupJob((JobContext) context);
  }

  /**
   * This method implements the new interface by calling the old method. Note
   * that the input types are different between the new and old apis and this
   * is a bridge between the two.
   */
  @Override
  public final void commitJob(org.apache.hadoop.mapreduce.JobContext context
                             ) throws IOException {
    commitJob((JobContext) context);
  }
  
  /**
   * This method implements the new interface by calling the old method. Note
   * that the input types are different between the new and old apis and this
   * is a bridge between the two.
   */
  @Override
  public final void abortJob(org.apache.hadoop.mapreduce.JobContext context, 
		                   org.apache.hadoop.mapreduce.JobStatus.State runState) 
  throws IOException {
    int state = JobStatus.getOldNewJobRunState(runState);
    if (state != JobStatus.FAILED && state != JobStatus.KILLED) {
      throw new IOException ("Invalid job run state : " + runState.name());
    }
    abortJob((JobContext) context, state);
  }
  
  /**
   * This method implements the new interface by calling the old method. Note
   * that the input types are different between the new and old apis and this
   * is a bridge between the two.
   */
  @Override
  public final 
  void setupTask(org.apache.hadoop.mapreduce.TaskAttemptContext taskContext
                 ) throws IOException {
    setupTask((TaskAttemptContext) taskContext);
  }
  
  /**
   * This method implements the new interface by calling the old method. Note
   * that the input types are different between the new and old apis and this
   * is a bridge between the two.
   */
  @Override
  public final boolean 
    needsTaskCommit(org.apache.hadoop.mapreduce.TaskAttemptContext taskContext
                    ) throws IOException {
    return needsTaskCommit((TaskAttemptContext) taskContext);
  }

  /**
   * This method implements the new interface by calling the old method. Note
   * that the input types are different between the new and old apis and this
   * is a bridge between the two.
   */
  @Override
  public final 
  void commitTask(org.apache.hadoop.mapreduce.TaskAttemptContext taskContext
                  ) throws IOException {
    commitTask((TaskAttemptContext) taskContext);
  }
  
  /**
   * This method implements the new interface by calling the old method. Note
   * that the input types are different between the new and old apis and this
   * is a bridge between the two.
   */
  @Override
  public final 
  void abortTask(org.apache.hadoop.mapreduce.TaskAttemptContext taskContext
                 ) throws IOException {
    abortTask((TaskAttemptContext) taskContext);
  }
  
  /**
   * This method implements the new interface by calling the old method. Note
   * that the input types are different between the new and old apis and this
   * is a bridge between the two.
   */
  @Override
  public final 
  void recoverTask(org.apache.hadoop.mapreduce.TaskAttemptContext taskContext
      ) throws IOException {
    recoverTask((TaskAttemptContext) taskContext);
  }

  /**
   * This method implements the new interface by calling the old method. Note
   * that the input types are different between the new and old apis and this is
   * a bridge between the two.
   */
  @Override
  public final boolean isRecoverySupported(
      org.apache.hadoop.mapreduce.JobContext context) throws IOException {
    return isRecoverySupported((JobContext) context);
  }

}
