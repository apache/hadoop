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
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.security.Credentials;

/**
 * A read-only view of the job that is provided to the tasks while they
 * are running.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface JobContext extends MRJobConfig {
  /**
   * Return the configuration for the job.
   * @return the shared configuration object
   */
  public Configuration getConfiguration();

  /**
   * Get credentials for the job.
   * @return credentials for the job
   */
  public Credentials getCredentials();

  /**
   * Get the unique ID for the job.
   * @return the object with the job id
   */
  public JobID getJobID();
  
  /**
   * Get configured the number of reduce tasks for this job. Defaults to 
   * <code>1</code>.
   * @return the number of reduce tasks for this job.
   */
  public int getNumReduceTasks();
  
  /**
   * Get the current working directory for the default file system.
   * 
   * @return the directory name.
   */
  public Path getWorkingDirectory() throws IOException;

  /**
   * Get the key class for the job output data.
   * @return the key class for the job output data.
   */
  public Class<?> getOutputKeyClass();
  
  /**
   * Get the value class for job outputs.
   * @return the value class for job outputs.
   */
  public Class<?> getOutputValueClass();

  /**
   * Get the key class for the map output data. If it is not set, use the
   * (final) output key class. This allows the map output key class to be
   * different than the final output key class.
   * @return the map output key class.
   */
  public Class<?> getMapOutputKeyClass();

  /**
   * Get the value class for the map output data. If it is not set, use the
   * (final) output value class This allows the map output value class to be
   * different than the final output value class.
   *  
   * @return the map output value class.
   */
  public Class<?> getMapOutputValueClass();

  /**
   * Get the user-specified job name. This is only used to identify the 
   * job to the user.
   * 
   * @return the job's name, defaulting to "".
   */
  public String getJobName();

  /**
   * Get the {@link InputFormat} class for the job.
   * 
   * @return the {@link InputFormat} class for the job.
   */
  public Class<? extends InputFormat<?,?>> getInputFormatClass() 
     throws ClassNotFoundException;

  /**
   * Get the {@link Mapper} class for the job.
   * 
   * @return the {@link Mapper} class for the job.
   */
  public Class<? extends Mapper<?,?,?,?>> getMapperClass() 
     throws ClassNotFoundException;

  /**
   * Get the combiner class for the job.
   * 
   * @return the combiner class for the job.
   */
  public Class<? extends Reducer<?,?,?,?>> getCombinerClass() 
     throws ClassNotFoundException;

  /**
   * Get the {@link Reducer} class for the job.
   * 
   * @return the {@link Reducer} class for the job.
   */
  public Class<? extends Reducer<?,?,?,?>> getReducerClass() 
     throws ClassNotFoundException;

  /**
   * Get the {@link OutputFormat} class for the job.
   * 
   * @return the {@link OutputFormat} class for the job.
   */
  public Class<? extends OutputFormat<?,?>> getOutputFormatClass() 
     throws ClassNotFoundException;

  /**
   * Get the {@link Partitioner} class for the job.
   * 
   * @return the {@link Partitioner} class for the job.
   */
  public Class<? extends Partitioner<?,?>> getPartitionerClass() 
     throws ClassNotFoundException;

  /**
   * Get the {@link RawComparator} comparator used to compare keys.
   * 
   * @return the {@link RawComparator} comparator used to compare keys.
   */
  public RawComparator<?> getSortComparator();

  /**
   * Get the pathname of the job's jar.
   * @return the pathname
   */
  public String getJar();

  /**
   * Get the user defined {@link RawComparator} comparator for
   * grouping keys of inputs to the combiner.
   *
   * @return comparator set by the user for grouping values.
   * @see Job#setCombinerKeyGroupingComparatorClass(Class)
   */
  public RawComparator<?> getCombinerKeyGroupingComparator();

    /**
     * Get the user defined {@link RawComparator} comparator for
     * grouping keys of inputs to the reduce.
     *
     * @return comparator set by the user for grouping values.
     * @see Job#setGroupingComparatorClass(Class)
     * @see #getCombinerKeyGroupingComparator()
     */
  public RawComparator<?> getGroupingComparator();
  
  /**
   * Get whether job-setup and job-cleanup is needed for the job 
   * 
   * @return boolean 
   */
  public boolean getJobSetupCleanupNeeded();
  
  /**
   * Get whether task-cleanup is needed for the job 
   * 
   * @return boolean 
   */
  public boolean getTaskCleanupNeeded();

  /**
   * Get whether the task profiling is enabled.
   * @return true if some tasks will be profiled
   */
  public boolean getProfileEnabled();

  /**
   * Get the profiler configuration arguments.
   *
   * The default value for this property is
   * "-agentlib:hprof=cpu=samples,heap=sites,force=n,thread=y,verbose=n,file=%s"
   * 
   * @return the parameters to pass to the task child to configure profiling
   */
  public String getProfileParams();

  /**
   * Get the range of maps or reduces to profile.
   * @param isMap is the task a map?
   * @return the task ranges
   */
  public IntegerRanges getProfileTaskRange(boolean isMap);

  /**
   * Get the reported username for this job.
   * 
   * @return the username
   */
  public String getUser();
  
  /**
   * Originally intended to check if symlinks should be used, but currently
   * symlinks cannot be disabled.
   * @return true
   */
  @Deprecated
  public boolean getSymlink();
  
  /**
   * Get the archive entries in classpath as an array of Path
   */
  public Path[] getArchiveClassPaths();

  /**
   * Get cache archives set in the Configuration
   * @return A URI array of the caches set in the Configuration
   * @throws IOException
   */
  public URI[] getCacheArchives() throws IOException;

  /**
   * Get cache files set in the Configuration
   * @return A URI array of the files set in the Configuration
   * @throws IOException
   */

  public URI[] getCacheFiles() throws IOException;

  /**
   * Return the path array of the localized caches
   * @return A path array of localized caches
   * @throws IOException
   * @deprecated the array returned only includes the items the were 
   * downloaded. There is no way to map this to what is returned by
   * {@link #getCacheArchives()}.
   */
  @Deprecated
  public Path[] getLocalCacheArchives() throws IOException;

  /**
   * Return the path array of the localized files
   * @return A path array of localized files
   * @throws IOException
   * @deprecated the array returned only includes the items the were 
   * downloaded. There is no way to map this to what is returned by
   * {@link #getCacheFiles()}.
   */
  @Deprecated
  public Path[] getLocalCacheFiles() throws IOException;

  /**
   * Get the file entries in classpath as an array of Path
   */
  public Path[] getFileClassPaths();
  
  /**
   * Get the timestamps of the archives.  Used by internal
   * DistributedCache and MapReduce code.
   * @return a string array of timestamps 
   */
  public String[] getArchiveTimestamps();

  /**
   * Get the timestamps of the files.  Used by internal
   * DistributedCache and MapReduce code.
   * @return a string array of timestamps 
   */
  public String[] getFileTimestamps();

  /** 
   * Get the configured number of maximum attempts that will be made to run a
   * map task, as specified by the <code>mapred.map.max.attempts</code>
   * property. If this property is not already set, the default is 4 attempts.
   *  
   * @return the max number of attempts per map task.
   */
  public int getMaxMapAttempts();

  /** 
   * Get the configured number of maximum attempts  that will be made to run a
   * reduce task, as specified by the <code>mapred.reduce.max.attempts</code>
   * property. If this property is not already set, the default is 4 attempts.
   * 
   * @return the max number of attempts per reduce task.
   */
  public int getMaxReduceAttempts();

}
