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
package org.apache.hadoop.tools.dynamometer.workloadgenerator;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 * Represents the base class for a generic workload-generating mapper. By
 * default, it will expect to use {@link VirtualInputFormat} as its
 * {@link InputFormat}. Subclasses requiring a reducer or expecting a different
 * {@link InputFormat} should override the {@link #configureJob(Job)} method.
 */
public abstract class WorkloadMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends
    Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  /**
   * Get the description of the behavior of this mapper.
   * @return description string.
   */
  public abstract String getDescription();

  /**
   * Get a list of the description of each configuration that this mapper
   * accepts.
   * @return list of the description of each configuration.
   */
  public abstract List<String> getConfigDescriptions();

  /**
   * Verify that the provided configuration contains all configurations required
   * by this mapper.
   * @param conf configuration.
   * @return whether or not all configurations required are provided.
   */
  public abstract boolean verifyConfigurations(Configuration conf);

  /**
   * Setup input and output formats and optional reducer.
   */
  public void configureJob(Job job) {
    job.setInputFormatClass(VirtualInputFormat.class);

    job.setNumReduceTasks(0);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(NullOutputFormat.class);
  }

}
