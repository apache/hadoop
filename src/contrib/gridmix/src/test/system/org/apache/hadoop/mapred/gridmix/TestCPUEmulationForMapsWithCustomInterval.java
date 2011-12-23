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
import org.junit.Test;
import org.junit.Assert;

/**
 * Test the {@link Gridmix} cpu emulation with custom interval for 
 * gridmix jobs against different input data, submission policies and 
 * user resolvers. Verify the map phase cpu metrics of gridmix jobs 
 * against their original job in the trace. 
 */
public class TestCPUEmulationForMapsWithCustomInterval 
                                            extends GridmixSystemTestCase {
  private static final Log LOG = 
      LogFactory.getLog("TestCPUEmulationForMapsWithCustomInterval.class");
  int execMode = GridMixRunMode.DATA_GENERATION_AND_RUN_GRIDMIX.getValue();

  /**
   * Generate compressed input and run {@link Gridmix} by turning on 
   * cpu emulation feature with custom setting. The {@link Gridmix} should 
   * use the following runtime parameters while running gridmix jobs.
   * Submission Policy : STRESS, User Resolver Mode : SumitterUserResolver
   * Once {@link Gridmix} run is complete, verify maps phase cpu resource 
   * metrics of {@link Gridmix} jobs with their corresponding original
   * in the trace.
   * @throws Exception - if an error occurs.
   */
  @Test
  public void  testCPUEmulatonForMapsWithCompressedInputCase3() 
      throws Exception { 
    final long inputSizeInMB = 1024 * 7;
    String tracePath = getTraceFile("cpu_emul_case1");
    Assert.assertNotNull("Trace file not found!", tracePath);
    String [] runtimeValues = {"LOADJOB",
                               SubmitterUserResolver.class.getName(),
                               "STRESS",
                               inputSizeInMB + "m",
                               tracePath};

    String [] otherArgs = {
        "-D", GridMixConfig.GRIDMIX_CPU_EMULATION + "=" +
              GridMixConfig.GRIDMIX_CPU_EMULATION_PLUGIN,
        "-D", GridMixConfig.GRIDMIX_DISTCACHE_ENABLE + "=false",
        "-D", GridMixConfig.GRIDMIX_CPU_CUSTOM_INTERVAL + "=0.25F"};

    runGridmixAndVerify(runtimeValues, otherArgs, tracePath, execMode);
  }

  /**
   * Generate uncompressed input and run {@link Gridmix} by turning on 
   * cpu emulation feature with custom settings. The {@link Gridmix} 
   * should use the following runtime paramters while running gridmix jobs.
   * Submission Policy: REPLAY  User Resolver Mode: RoundRobinUserResolver
   * Once {@link Gridmix} run is complete, verify the map phase cpu resource 
   * metrics of {@link Gridmix} jobs with their corresponding jobs
   * in the original trace.
   * @throws Exception - if an error occurs.
   */
  @Test
  public void testCPUEmulatonForMapsUnCompressedInputCase4() 
      throws Exception { 
    final long inputSizeInMB = cSize * 200;
    String tracePath = getTraceFile("cpu_emul_case1");
    Assert.assertNotNull("Trace file not found!", tracePath);
    String [] runtimeValues = 
           {"LOADJOB",
            RoundRobinUserResolver.class.getName(),
            "REPLAY",
            inputSizeInMB + "m",
            "file://" + UtilsForGridmix.getProxyUsersFile(conf),
            tracePath};

    String [] otherArgs = {
        "-D", GridMixConfig.GRIDMIX_DISTCACHE_ENABLE + "=false",
        "-D", GridMixConfig.GRIDMIX_COMPRESSION_ENABLE + "=false",
        "-D", GridMixConfig.GRIDMIX_CPU_EMULATION + "=" + 
              GridMixConfig.GRIDMIX_CPU_EMULATION_PLUGIN,
        "-D", GridMixConfig.GRIDMIX_CPU_CUSTOM_INTERVAL + "=0.35F"};

    runGridmixAndVerify(runtimeValues, otherArgs, tracePath, execMode);
  }
}
