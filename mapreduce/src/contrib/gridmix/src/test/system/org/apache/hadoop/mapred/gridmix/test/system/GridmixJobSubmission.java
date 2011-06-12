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
package org.apache.hadoop.mapred.gridmix.test.system;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.junit.Assert;

/**
 * Submit the gridmix jobs. 
 */
public class GridmixJobSubmission {
  private static final Log LOG = 
      LogFactory.getLog(GridmixJobSubmission.class);
  private int gridmixJobCount;
  private Configuration conf;
  private Path gridmixDir;
  private JTClient jtClient;

  public GridmixJobSubmission(Configuration conf, JTClient jtClient , 
                              Path gridmixDir) { 
    this.conf = conf;
    this.jtClient = jtClient;
    this.gridmixDir = gridmixDir;
  }
  
  /**
   * Submit the gridmix jobs.
   * @param runtimeArgs - gridmix common runtime arguments.
   * @param otherArgs - gridmix other runtime arguments.
   * @param traceInterval - trace time interval.
   * @throws Exception
   */
  public void submitJobs(String [] runtimeArgs, 
                         String [] otherArgs, int mode) throws Exception {
    int prvJobCount = jtClient.getClient().getAllJobs().length;
    int exitCode = -1;
    if (otherArgs == null) {
      exitCode = UtilsForGridmix.runGridmixJob(gridmixDir, conf, 
                                               mode, runtimeArgs);
    } else {
      exitCode = UtilsForGridmix.runGridmixJob(gridmixDir, conf, mode,
                                               runtimeArgs, otherArgs);
    }
    Assert.assertEquals("Gridmix jobs have failed.", 0 , exitCode);
    gridmixJobCount = jtClient.getClient().getAllJobs().length - prvJobCount;
  }

  /**
   * Get the submitted jobs count.
   * @return count of no. of jobs submitted for a trace.
   */
  public int getGridmixJobCount() {
     return gridmixJobCount;
  }

  /**
   * Get the job configuration.
   * @return Configuration of a submitted job.
   */
  public Configuration getJobConf() {
    return conf;
  }
}
