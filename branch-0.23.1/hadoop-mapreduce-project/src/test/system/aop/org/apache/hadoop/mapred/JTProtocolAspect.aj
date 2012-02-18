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
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.TTInfo;
import org.apache.hadoop.mapreduce.test.system.TaskInfo;

/**
 * Aspect which injects the basic protocol functionality which is to be
 * implemented by all the services which implement {@link ClientProtocol}
 * 
 * Aspect also injects default implementation for the {@link JTProtocol}
 */

public aspect JTProtocolAspect {

  // Make the ClientProtocl extend the JTprotocol
  declare parents : ClientProtocol extends JTProtocol;

  /*
   * Start of default implementation of the methods in JTProtocol
   */

  public Configuration JTProtocol.getDaemonConf() throws IOException {
    return null;
  }

  public JobInfo JTProtocol.getJobInfo(JobID jobID) throws IOException {
    return null;
  }

  public TaskInfo JTProtocol.getTaskInfo(TaskID taskID) throws IOException {
    return null;
  }

  public TTInfo JTProtocol.getTTInfo(String trackerName) throws IOException {
    return null;
  }

  public JobInfo[] JTProtocol.getAllJobInfo() throws IOException {
    return null;
  }

  public TaskInfo[] JTProtocol.getTaskInfo(JobID jobID) throws IOException {
    return null;
  }

  public TTInfo[] JTProtocol.getAllTTInfo() throws IOException {
    return null;
  }
  
  public boolean JTProtocol.isJobRetired(JobID jobID) throws IOException {
    return false;
  }
  
  public String JTProtocol.getJobHistoryLocationForRetiredJob(JobID jobID) throws IOException {
    return "";
  }
}
