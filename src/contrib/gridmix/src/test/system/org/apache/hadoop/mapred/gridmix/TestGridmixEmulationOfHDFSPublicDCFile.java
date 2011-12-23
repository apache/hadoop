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
//import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verify the Gridmix emulation of HDFS public distributed cache file.
 */
public class TestGridmixEmulationOfHDFSPublicDCFile 
    extends GridmixSystemTestCase { 
  private static final Log LOG = 
      LogFactory.getLog("TestGridmixEmulationOfHDFSPublicDCFile.class");

  /**
   * Generate the input data and HDFS distributed cache file based 
   * on given input trace. Verify the Gridmix emulation of single HDFS
   * public distributed cache file in SubmitterUserResolver mode with 
   * STRESS submission policy.
   * @throws Exception - if an error occurs.
   */
  @Test
  public void testGenerateAndEmulationOfSingleHDFSDCFile() 
      throws Exception { 
    final long inputSizeInMB = 7168;
    final String tracePath = getTraceFile("distcache_case1_trace");
    Assert.assertNotNull("Trace file was not found.", tracePath);
    final String [] runtimeValues = {"LOADJOB",
                                     SubmitterUserResolver.class.getName(),
                                     "STRESS",
                                     inputSizeInMB + "m",
                                     tracePath};

    final String [] otherArgs = { 
      "-D", JobContext.JOB_CANCEL_DELEGATION_TOKEN + "=false",
      "-D", GridmixJob.GRIDMIX_HIGHRAM_EMULATION_ENABLE + "=false",
      "-D", GridMixConfig.GRIDMIX_DISTCACHE_ENABLE + "=true"
    };
    runGridmixAndVerify(runtimeValues, otherArgs, tracePath, 
        GridMixRunMode.DATA_GENERATION_AND_RUN_GRIDMIX.getValue());
  }
  
  /**
   * Verify the Gridmix emulation of Single HDFS public distributed cache
   * file in RoundRobinUserResolver mode with REPLAY submission policy 
   * by using the existing input data and HDFS public distributed cache file. 
   * @throws Exception - if an error occurs.
   */
  @Test
  public void testGridmixEmulationOfSingleHDFSPublicDCFile() 
      throws Exception {
    final String tracePath = getTraceFile("distcache_case1_trace");
    Assert.assertNotNull("Trace file was not found.", tracePath);
    final String [] runtimeValues = 
                     { "LOADJOB",
                       RoundRobinUserResolver.class.getName(),
                       "REPLAY",
                       "file://" + UtilsForGridmix.getProxyUsersFile(conf),
                       tracePath};

    final String [] otherArgs = {
       "-D", GridmixJob.GRIDMIX_HIGHRAM_EMULATION_ENABLE + "=false",
       "-D", GridMixConfig.GRIDMIX_DISTCACHE_ENABLE + "=true"
    };
    runGridmixAndVerify(runtimeValues, otherArgs, tracePath, 
                        GridMixRunMode.RUN_GRIDMIX.getValue());
  }
}

