/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred.gridmix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.rumen.JobStory;

import java.io.IOException;

public enum JobCreator {

  LOADJOB("LOADJOB") {
    @Override
    public GridmixJob createGridmixJob(
      Configuration conf, long submissionMillis, JobStory jobdesc, Path outRoot,
      UserGroupInformation ugi, int seq) throws IOException {
      return new LoadJob(conf, submissionMillis, jobdesc, outRoot, ugi, seq);
    }},

  SLEEPJOB("SLEEPJOB") {
    @Override
    public GridmixJob createGridmixJob(
      Configuration conf, long submissionMillis, JobStory jobdesc, Path outRoot,
      UserGroupInformation ugi, int seq) throws IOException {
      return new SleepJob(conf, submissionMillis, jobdesc, outRoot, ugi, seq);
    }};

  public static final String GRIDMIX_JOB_TYPE = "gridmix.job.type";


  private final String name;

  JobCreator(String name) {
    this.name = name;
  }

  public abstract GridmixJob createGridmixJob(
    final Configuration conf, long submissionMillis, final JobStory jobdesc,
    Path outRoot, UserGroupInformation ugi, final int seq) throws IOException;

  public static JobCreator getPolicy(
    Configuration conf, JobCreator defaultPolicy) {
    return conf.getEnum(GRIDMIX_JOB_TYPE, defaultPolicy);
  }
}
