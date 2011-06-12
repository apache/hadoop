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
import org.junit.Assert;
import org.junit.Test;

/**
 * Verify the Gridmix emulation of HDFS private distributed cache file.
 */
public class TestGridmixEmulationOfHDFSPrivateDCFile 
    extends GridmixSystemTestCase {
  private static final Log LOG = LogFactory.
      getLog("TestGridmixEmulationOfHDFSPrivateDCFile.class");
  /**
   * Generate input data and single HDFS private distributed cache 
   * file based on given input trace.Verify the Gridmix emulation of 
   * single private HDFS distributed cache file in RoundRobinUserResolver 
   * mode with STRESS submission policy.
   * @throws Exception - if an error occurs.
   */
  @Test
  public void testGenerateAndEmulateOfHDFSPrivateDCFile() 
      throws Exception {
    final long INPUT_SIZE = 8192;
    final String tracePath = getTraceFile("distcache_case3_trace");
    Assert.assertNotNull("Trace file was not found.", tracePath);
    final String [] runtimeValues ={"LOADJOB",
        RoundRobinUserResolver.class.getName(),
        "STRESS",
        INPUT_SIZE+"m",
        "file://" + UtilsForGridmix.getProxyUsersFile(conf),
        tracePath};
    final String [] otherArgs = {
        "-D",
        "mapreduce.job.complete.cancel.delegation.tokens=false",
        "-D", GridMixConfig.GRIDMIX_DISTCACHE_ENABLE +
        "=true"
    };
    runGridmixAndVerify(runtimeValues, otherArgs, tracePath, 
        GridMixRunMode.DATA_GENERATION_AND_RUN_GRIDMIX);
  }
  /**
   * Verify the Gridmix emulation of single HDFS private distributed 
   * cache file in SubmitterUserResolver mode with REPLAY submission 
   * policy by using the existing input data and HDFS private 
   * distributed cache file.
   * @throws Exception - if an error occurs.
   */
  @Test
  public void testGridmixEmulationOfHDFSPrivateDCFile() 
      throws Exception {
    final String tracePath = getTraceFile("distcache_case3_trace");
    Assert.assertNotNull("Trace file was not found.", tracePath);
    final String [] runtimeValues ={"LOADJOB",
       SubmitterUserResolver.class.getName(),
       "REPLAY",
       tracePath};
    final String [] otherArgs = {
        "-D", GridMixConfig.GRIDMIX_DISTCACHE_ENABLE +
        "=true"
    };
    runGridmixAndVerify(runtimeValues, otherArgs, tracePath, 
        GridMixRunMode.RUN_GRIDMIX);
  }
}

