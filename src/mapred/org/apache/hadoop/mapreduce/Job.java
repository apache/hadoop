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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapred.TaskCompletionEvent;

/**
 * The job submitter's view of the Job. It allows the user to configure the
 * job, submit it, control its execution, and query the state.
 */
public class Job extends JobContext {  
  
  public Job() {
    this(new Configuration());
  }

  public Job(Configuration conf) {
    super(conf, null);
  }

  public Job(Configuration conf, String jobName) {
    this(conf);
    setJobName(jobName);
  }

  /**
   * Set the number of reduce tasks for the job.
   * @param tasks the number of reduce tasks
   */
  public void setNumReduceTasks(int tasks) {
    conf.setInt(NUM_REDUCES_ATTR, tasks);
  }

  /**
   * Set the current working directory for the default file system.
   * 
   * @param dir the new current working directory.
   */
  public void setWorkingDirectory(Path dir) throws IOException {
    dir = dir.makeQualified(FileSystem.get(conf));
    conf.set(WORKING_DIR_ATTR, dir.toString());
  }

  /**
   * Set the {@link InputFormat} for the job.
   * @param cls the <code>InputFormat</code> to use
   */
  public void setInputFormatClass(Class<? extends InputFormat<?,?>> cls) {
    conf.setClass(INPUT_FORMAT_CLASS_ATTR, cls, InputFormat.class);
  }

  /**
   * Set the {@link OutputFormat} for the job.
   * @param cls the <code>OutputFormat</code> to use
   */
  public void setOutputFormatClass(Class<? extends OutputFormat<?,?>> cls) {
    conf.setClass(OUTPUT_FORMAT_CLASS_ATTR, cls, OutputFormat.class);
  }

  /**
   * Set the {@link Mapper} for the job.
   * @param cls the <code>Mapper</code> to use
   */
  public void setMapperClass(Class<? extends Mapper<?,?,?,?>> cls) {
    conf.setClass(MAP_CLASS_ATTR, cls, Mapper.class);
  }

  /**
   * Set the combiner class for the job.
   * @param cls the combiner to use
   */
  public void setCombinerClass(Class<? extends Reducer<?,?,?,?>> cls) {
    conf.setClass(COMBINE_CLASS_ATTR, cls, Reducer.class);
  }

  /**
   * Set the {@link Reducer} for the job.
   * @param cls the <code>Reducer</code> to use
   */
  public void setReducerClass(Class<? extends Reducer<?,?,?,?>> cls) {
    conf.setClass(REDUCE_CLASS_ATTR, cls, Reducer.class);
  }

  /**
   * Set the {@link Partitioner} for the job.
   * @param cls the <code>Partitioner</code> to use
   */
  public void setPartitionerClass(Class<? extends Partitioner<?,?>> cls) {
    conf.setClass(PARTITIONER_CLASS_ATTR, cls, Partitioner.class);
  }

  /**
   * Set the key class for the map output data. This allows the user to
   * specify the map output key class to be different than the final output
   * value class.
   * 
   * @param theClass the map output key class.
   */
  public void setMapOutputKeyClass(Class<?> theClass) {
    conf.setClass(MAP_OUTPUT_KEY_CLASS_ATTR, theClass, Object.class);
  }

  /**
   * Set the value class for the map output data. This allows the user to
   * specify the map output value class to be different than the final output
   * value class.
   * 
   * @param theClass the map output value class.
   */
  public void setMapOutputValueClass(Class<?> theClass) {
    conf.setClass(MAP_OUTPUT_VALUE_CLASS_ATTR, theClass, Object.class);
  }

  /**
   * Set the key class for the job output data.
   * 
   * @param theClass the key class for the job output data.
   */
  public void setOutputKeyClass(Class<?> theClass) {
    conf.setClass(OUTPUT_KEY_CLASS_ATTR, theClass, Object.class);
  }

  /**
   * Set the value class for job outputs.
   * 
   * @param theClass the value class for job outputs.
   */
  public void setOutputValueClass(Class<?> theClass) {
    conf.setClass(OUTPUT_VALUE_CLASS_ATTR, theClass, Object.class);
  }

  /**
   * Define the comparator that controls how the keys are sorted before they
   * are passed to the {@link Reducer}.
   * @param cls the raw comparator
   */
  public void setSortComparatorClass(Class<? extends RawComparator<?>> cls) {
    conf.setClass(SORT_COMPARATOR_ATTR, cls, RawComparator.class);
  }

  /**
   * Define the comparator that controls which keys are grouped together
   * for a single call to 
   * {@link Reducer#reduce(Object, Iterable, org.apache.hadoop.mapreduce.Reducer.Context)}
   * @param cls the raw comparator to use
   */
  public void setGroupingComparatorClass(Class<? extends RawComparator<?>> cls){
    conf.setClass(GROUPING_COMPARATOR_ATTR, cls, RawComparator.class);
  }

  /**
   * Set the user-specified job name.
   * 
   * @param name the job's new name.
   */
  public void setJobName(String name) {
    conf.set(JOB_NAME_ATTR, name);
  }

  /**
   * Get the URL where some job progress information will be displayed.
   * 
   * @return the URL where some job progress information will be displayed.
   */
  public String getTrackingURL() {
    // TODO
    return null;
  }

  /**
   * Get the <i>progress</i> of the job's map-tasks, as a float between 0.0 
   * and 1.0.  When all map tasks have completed, the function returns 1.0.
   * 
   * @return the progress of the job's map-tasks.
   * @throws IOException
   */
  public float mapProgress() throws IOException {
    // TODO
    return 0.0f;
  }

  /**
   * Get the <i>progress</i> of the job's reduce-tasks, as a float between 0.0 
   * and 1.0.  When all reduce tasks have completed, the function returns 1.0.
   * 
   * @return the progress of the job's reduce-tasks.
   * @throws IOException
   */
  public float reduceProgress() throws IOException {
    // TODO
    return 0.0f;
  }

  /**
   * Check if the job is finished or not. 
   * This is a non-blocking call.
   * 
   * @return <code>true</code> if the job is complete, else <code>false</code>.
   * @throws IOException
   */
  public boolean isComplete() throws IOException {
    // TODO
    return false;
  }

  /**
   * Check if the job completed successfully. 
   * 
   * @return <code>true</code> if the job succeeded, else <code>false</code>.
   * @throws IOException
   */
  public boolean isSuccessful() throws IOException {
    // TODO
    return false;
  }

  /**
   * Kill the running job.  Blocks until all job tasks have been
   * killed as well.  If the job is no longer running, it simply returns.
   * 
   * @throws IOException
   */
  public void killJob() throws IOException {
    // TODO
  }
    
  /**
   * Get events indicating completion (success/failure) of component tasks.
   *  
   * @param startFrom index to start fetching events from
   * @return an array of {@link TaskCompletionEvent}s
   * @throws IOException
   */
  public TaskCompletionEvent[] getTaskCompletionEvents(int startFrom
                                                       ) throws IOException {
    // TODO
    return null;
  }
  
  /**
   * Kill indicated task attempt.
   * 
   * @param taskId the id of the task to be terminated.
   * @throws IOException
   */
  public void killTask(TaskAttemptID taskId) throws IOException {
    // TODO
  }

  /**
   * Fail indicated task attempt.
   * 
   * @param taskId the id of the task to be terminated.
   * @throws IOException
   */
  public void failTask(TaskAttemptID taskId) throws IOException {
    // TODO
  }

  /**
   * Gets the counters for this job.
   * 
   * @return the counters for this job.
   * @throws IOException
   */
  public Iterable<CounterGroup> getCounters() throws IOException {
    // TODO
    return null;
  }

  /**
   * Submit the job to the cluster and return immediately.
   * @throws IOException
   */
  public void submit() throws IOException {
    // TODO
  }
  
  /**
   * Submit the job to the cluster and wait for it to finish.
   * @return true if the job succeeded
   * @throws IOException thrown if the communication with the 
   *         <code>JobTracker</code> is lost
   */
  public boolean waitForCompletion() throws IOException {
    // TODO
    return false;
  }
}
