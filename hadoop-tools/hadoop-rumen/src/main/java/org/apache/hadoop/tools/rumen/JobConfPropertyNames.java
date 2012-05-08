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
package org.apache.hadoop.tools.rumen;

import java.util.Arrays;

import org.apache.hadoop.mapreduce.MRJobConfig;

public enum JobConfPropertyNames {
  QUEUE_NAMES("mapred.job.queue.name", MRJobConfig.QUEUE_NAME),
  JOB_NAMES("mapred.job.name", MRJobConfig.JOB_NAME),
  TASK_JAVA_OPTS_S("mapred.child.java.opts"),
  MAP_JAVA_OPTS_S("mapred.child.java.opts", MRJobConfig.MAP_JAVA_OPTS),
  REDUCE_JAVA_OPTS_S("mapred.child.java.opts", MRJobConfig.REDUCE_JAVA_OPTS);

  private String[] candidates;

  JobConfPropertyNames(String... candidates) {
    this.candidates = candidates;
  }

  public String[] getCandidates() {
    return Arrays.copyOf(candidates, candidates.length);
  }
}
