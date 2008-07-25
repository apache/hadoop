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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A read-only view of the job that is provided to the tasks while they
 * are running.
 */
public class JobContext {
  // Put all of the attribute names in here so that Job and JobContext are
  // consistent.
  protected static final String INPUT_FORMAT_CLASS_ATTR = "mapreduce.map.class";
  protected static final String MAP_CLASS_ATTR = "mapreduce.map.class";
  protected static final String COMBINE_CLASS_ATTR = "mapreduce.combine.class";
  protected static final String REDUCE_CLASS_ATTR = "mapreduce.reduce.class";
  protected static final String OUTPUT_FORMAT_CLASS_ATTR = 
    "mapreduce.outputformat.class";
  protected static final String OUTPUT_KEY_CLASS_ATTR = 
    "mapreduce.out.key.class";
  protected static final String OUTPUT_VALUE_CLASS_ATTR = 
    "mapreduce.out.value.class";
  protected static final String MAP_OUTPUT_KEY_CLASS_ATTR = 
    "mapreduce.map.out.key.class";
  protected static final String MAP_OUTPUT_VALUE_CLASS_ATTR = 
    "mapreduce.map.out.value.class";
  protected static final String NUM_REDUCES_ATTR = "mapreduce.reduce.tasks";
  protected static final String WORKING_DIR_ATTR = "mapreduce.work.dir";
  protected static final String JOB_NAME_ATTR = "mapreduce.job.name";
  protected static final String SORT_COMPARATOR_ATTR = 
    "mapreduce.sort.comparator";
  protected static final String GROUPING_COMPARATOR_ATTR = 
    "mapreduce.grouping.comparator";
  protected static final String PARTITIONER_CLASS_ATTR = 
    "mapreduce.partitioner.class";

  protected final Configuration conf;
  private final JobID jobId;
  
  public JobContext(Configuration conf, JobID jobId) {
    this.conf = conf;
    this.jobId = jobId;
  }

  /**
   * Return the configuration for the job.
   * @return the shared configuration object
   */
  public Configuration getConfiguration() {
    return conf;
  }

  /**
   * Get the unique ID for the job.
   * @return the object with the job id
   */
  public JobID getJobID() {
    return jobId;
  }
  
  /**
   * Get configured the number of reduce tasks for this job. Defaults to 
   * <code>1</code>.
   * @return the number of reduce tasks for this job.
   */
  public int getNumReduceTasks() {
    return conf.getInt(NUM_REDUCES_ATTR, 1);
  }
  
  /**
   * Get the current working directory for the default file system.
   * 
   * @return the directory name.
   */
  public Path getWorkingDirectory() throws IOException {
    String name = conf.get(WORKING_DIR_ATTR);
    if (name != null) {
      return new Path(name);
    } else {
      Path dir = FileSystem.get(conf).getWorkingDirectory();
      conf.set(WORKING_DIR_ATTR, dir.toString());
      return dir;
    }
  }

  /**
   * Get the key class for the job output data.
   * @return the key class for the job output data.
   */
  public Class<?> getOutputKeyClass() {
    return conf.getClass(OUTPUT_KEY_CLASS_ATTR,
                         LongWritable.class, Object.class);
  }
  
  /**
   * Get the value class for job outputs.
   * @return the value class for job outputs.
   */
  public Class<?> getOutputValueClass() {
    return conf.getClass(OUTPUT_VALUE_CLASS_ATTR, Text.class, Object.class);
  }

  /**
   * Get the key class for the map output data. If it is not set, use the
   * (final) output key class. This allows the map output key class to be
   * different than the final output key class.
   * @return the map output key class.
   */
  public Class<?> getMapOutputKeyClass() {
    Class<?> retv = conf.getClass(MAP_OUTPUT_KEY_CLASS_ATTR, null, 
                                  Object.class);
    if (retv == null) {
      retv = getOutputKeyClass();
    }
    return retv;
  }

  /**
   * Get the value class for the map output data. If it is not set, use the
   * (final) output value class This allows the map output value class to be
   * different than the final output value class.
   *  
   * @return the map output value class.
   */
  public Class<?> getMapOutputValueClass() {
    Class<?> retv = conf.getClass(MAP_OUTPUT_VALUE_CLASS_ATTR, null,
        Object.class);
    if (retv == null) {
      retv = getOutputValueClass();
    }
    return retv;
  }

  /**
   * Get the user-specified job name. This is only used to identify the 
   * job to the user.
   * 
   * @return the job's name, defaulting to "".
   */
  public String getJobName() {
    return conf.get(JOB_NAME_ATTR, "");
  }

  /**
   * Get the {@link InputFormat} class for the job.
   * 
   * @return the {@link InputFormat} class for the job.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends InputFormat<?,?>> getInputFormatClass() 
     throws ClassNotFoundException {
    return (Class<? extends InputFormat<?,?>>) 
      conf.getClass(INPUT_FORMAT_CLASS_ATTR, InputFormat.class);
  }

  /**
   * Get the {@link Mapper} class for the job.
   * 
   * @return the {@link Mapper} class for the job.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Mapper<?,?,?,?>> getMapperClass() 
     throws ClassNotFoundException {
    return (Class<? extends Mapper<?,?,?,?>>) 
      conf.getClass(MAP_CLASS_ATTR, Mapper.class);
  }

  /**
   * Get the combiner class for the job.
   * 
   * @return the combiner class for the job.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Reducer<?,?,?,?>> getCombinerClass() 
     throws ClassNotFoundException {
    return (Class<? extends Reducer<?,?,?,?>>) 
      conf.getClass(COMBINE_CLASS_ATTR, Reducer.class);
  }

  /**
   * Get the {@link Reducer} class for the job.
   * 
   * @return the {@link Reducer} class for the job.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Reducer<?,?,?,?>> getReducerClass() 
     throws ClassNotFoundException {
    return (Class<? extends Reducer<?,?,?,?>>) 
      conf.getClass(REDUCE_CLASS_ATTR, Reducer.class);
  }

  /**
   * Get the {@link OutputFormat} class for the job.
   * 
   * @return the {@link OutputFormat} class for the job.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends OutputFormat<?,?>> getOutputFormatClass() 
     throws ClassNotFoundException {
    return (Class<? extends OutputFormat<?,?>>) 
      conf.getClass(OUTPUT_FORMAT_CLASS_ATTR, OutputFormat.class);
  }

  /**
   * Get the {@link Partitioner} class for the job.
   * 
   * @return the {@link Partitioner} class for the job.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Partitioner<?,?>> getPartitionerClass() 
     throws ClassNotFoundException {
    return (Class<? extends Partitioner<?,?>>) 
      conf.getClass(PARTITIONER_CLASS_ATTR, Partitioner.class);
  }

  /**
   * Get the {@link RawComparator} comparator used to compare keys.
   * 
   * @return the {@link RawComparator} comparator used to compare keys.
   */
  public RawComparator<?> getSortComparator() {
    Class<?> theClass = conf.getClass(SORT_COMPARATOR_ATTR, null,
                                   RawComparator.class);
    if (theClass != null)
      return (RawComparator<?>) ReflectionUtils.newInstance(theClass, conf);
    return WritableComparator.get(getMapOutputKeyClass());
  }

  /** 
   * Get the user defined {@link WritableComparable} comparator for 
   * grouping keys of inputs to the reduce.
   * 
   * @return comparator set by the user for grouping values.
   * @see Job#setGroupingComparatorClass(Class) for details.  
   */
  public RawComparator<?> getGroupingComparator() {
    Class<?> theClass = conf.getClass(GROUPING_COMPARATOR_ATTR, null,
                                   RawComparator.class);
    if (theClass == null) {
      return getSortComparator();
    }
    return (RawComparator<?>) ReflectionUtils.newInstance(theClass, conf);
  }

}
