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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.gridmix.test.system.GridMixConfig;
import org.apache.hadoop.mapred.gridmix.test.system.GridMixRunMode;
import org.apache.hadoop.mapred.gridmix.test.system.UtilsForGridmix;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verify the emulation of local FS distributed cache files.
 *
 */
public class TestEmulationOfLocalFSDCFiles extends GridmixSystemTestCase { 
  private static final Log LOG = 
      LogFactory.getLog("TestEmulationOfLocalFSDCFiles.class");

  /**
   * Generate the input data and distributer cache files.Verify the 
   * gridmix emulation of local file system distributed cache files 
   * in RoundRobinUserResolver mode with REPLAY submission policy.
   * @throws Exception - if an error occurs.
   */
  @Test
  public void testGenerateInputAndEmulateLocalFSDCFile() 
     throws Exception { 
    final long inputSizeInMB = 1024 * 6;
    final String tracePath = getTraceFile("distcache_case7_trace");
    Assert.assertNotNull("Trace file has not found.", tracePath);
    final String [] runtimeValues = 
                     {"LOADJOB",
                      RoundRobinUserResolver.class.getName(),
                      "REPLAY",
                      inputSizeInMB + "m",
                      "file://" + UtilsForGridmix.getProxyUsersFile(conf),
                      tracePath};

    final String [] otherArgs = {
       "-D", JobContext.JOB_CANCEL_DELEGATION_TOKEN + "=false",
       "-D", GridMixConfig.GRIDMIX_DISTCACHE_ENABLE + "=true",
       "-D", GridmixJob.GRIDMIX_HIGHRAM_EMULATION_ENABLE + "=false",
       "-D", GridMixConfig.GRIDMIX_COMPRESSION_ENABLE + "=false"
    };
    runGridmixAndVerify(runtimeValues, otherArgs, tracePath, 
        GridMixRunMode.DATA_GENERATION_AND_RUN_GRIDMIX.getValue());
  }
  
  /**
   * Use existing input and local distributed cache files and  verify 
   * the gridmix emulation of local file system distributed cache 
   * files in SubmitterUserResolver mode with STRESS
   * Submission policy.
   * @throws Exception - if an error occurs.
   */
  @Test
  public void testEmulationOfLocalFSDCFile() 
     throws Exception  {
    final String tracePath = getTraceFile("distcache_case7_trace");
    Assert.assertNotNull("Trace file has not found.", tracePath);
    final String [] runtimeValues = {"LOADJOB",
                                     SubmitterUserResolver.class.getName(),
                                     "STRESS",
                                     tracePath};

    final String [] otherArgs = {
       "-D",JobContext.JOB_CANCEL_DELEGATION_TOKEN + "=false",
      "-D", GridMixConfig.GRIDMIX_DISTCACHE_ENABLE + "=true",
      "-D", GridmixJob.GRIDMIX_HIGHRAM_EMULATION_ENABLE + "=false",
      "-D", GridMixConfig.GRIDMIX_COMPRESSION_ENABLE + "=false"
    };
    runGridmixAndVerify(runtimeValues, otherArgs, tracePath, 
                        GridMixRunMode.RUN_GRIDMIX.getValue());
  }
}
