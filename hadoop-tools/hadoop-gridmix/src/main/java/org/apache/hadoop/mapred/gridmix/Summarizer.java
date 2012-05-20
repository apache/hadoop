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
package org.apache.hadoop.mapred.gridmix;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.gridmix.GenerateData.DataStatistics;

/**
 * Summarizes various aspects of a {@link Gridmix} run.
 */
class Summarizer {
  private ExecutionSummarizer executionSummarizer;
  private ClusterSummarizer clusterSummarizer;
  protected static final String NA = "N/A";
  
  Summarizer() {
    this(new String[]{NA});
  }
  
  Summarizer(String[] args) {
    executionSummarizer = new ExecutionSummarizer(args);
    clusterSummarizer = new ClusterSummarizer();
  }
  
  ExecutionSummarizer getExecutionSummarizer() {
    return executionSummarizer;
  }
  
  ClusterSummarizer getClusterSummarizer() {
    return clusterSummarizer;
  }
  
  void start(Configuration conf) {
    executionSummarizer.start(conf);
    clusterSummarizer.start(conf);
  }
  
  /**
   * This finalizes the summarizer.
   */
  @SuppressWarnings("unchecked")
  void finalize(JobFactory factory, String path, long size, 
                UserResolver resolver, DataStatistics stats, Configuration conf)
  throws IOException {
    executionSummarizer.finalize(factory, path, size, resolver, stats, conf);
  }
  
  /**
   * Summarizes the current {@link Gridmix} run and the cluster used. 
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(executionSummarizer.toString());
    builder.append(clusterSummarizer.toString());
    return builder.toString();
  }
}