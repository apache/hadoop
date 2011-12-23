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
 *  Verify the Gridmix emulation of HDFS distributed cache files of 
 *  different visibilities. 
 */

public class TestEmulationOfHDFSDCFilesWithDifferentVisibilities 
    extends GridmixSystemTestCase {
  private static final Log LOG = 
     LogFactory.getLog(
         "TestEmulationOfHDFSDCFilesWithDifferentVisibilities.class");
  
  /**
   * Generate input data and HDFS distributed cache files of different
   * visibilities based on given input trace. Verify the Gridmix emulation 
   * of HDFS distributed cache files of different visibilities in 
   * RoundRobinUserResolver mode with SERIAL submission policy.
   * @throws Exception - if an error occurs.
   */
  @Test
  public void testGenerateAndEmulateOfHDFSDCFilesWithDiffVisibilities() 
     throws Exception {
    final long INPUT_SIZE = 1024 * 9;
    final String tracePath = getTraceFile("distcache_case5_trace");
    Assert.assertNotNull("Trace file was not found.", tracePath);
    final String [] runtimeValues = 
                     { "LOADJOB",
                       RoundRobinUserResolver.class.getName(),
                       "STRESS",
                       INPUT_SIZE+"m",
                       "file://" + UtilsForGridmix.getProxyUsersFile(conf),
                       tracePath};

    final String [] otherArgs = { 
        "-D", JobContext.JOB_CANCEL_DELEGATION_TOKEN + "=false",
        "-D", GridmixJob.GRIDMIX_HIGHRAM_EMULATION_ENABLE + "=false",
        "-D", GridMixConfig.GRIDMIX_DISTCACHE_ENABLE +  "=true"
    };
    runGridmixAndVerify(runtimeValues, otherArgs, tracePath, 
        GridMixRunMode.DATA_GENERATION_AND_RUN_GRIDMIX.getValue());
  }
  
  /**
   * Disable the distributed cache emulation and verify the Gridmix jobs
   * whether it emulates or not. 
   * @throws Exception
   */
  @Test
  public void testHDFSDCFilesWithoutEnableDCEmulation() 
     throws Exception {
    final String tracePath = getTraceFile("distcache_case6_trace");
    Assert.assertNotNull("Trace file was not found.", tracePath);
    final String [] runtimeValues ={ "LOADJOB",
                                     SubmitterUserResolver.class.getName(),
                                     "STRESS",
                                     tracePath};
    final String [] otherArgs = {
        "-D", GridmixJob.GRIDMIX_HIGHRAM_EMULATION_ENABLE + "=false",
        "-D", GridMixConfig.GRIDMIX_DISTCACHE_ENABLE + "=false"
    };
    runGridmixAndVerify(runtimeValues, otherArgs, tracePath, 
                        GridMixRunMode.RUN_GRIDMIX.getValue());
  }
}

