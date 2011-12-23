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
 * Test the {@link Gridmix} cpu emulation with default settings for 
 * gridmix jobs against different input data, submission policies and 
 * user resolvers. Verify the map phase cpu metrics of gridmix jobs 
 * against their original jobs in the trace. 
 */
public class TestCPUEmulationForMapsWithDefaultInterval 
    extends GridmixSystemTestCase { 
  private static final Log LOG = 
      LogFactory.getLog("TestCPUEmulationForMapsWithDefaultInterval.class");
  int execMode = GridMixRunMode.DATA_GENERATION_AND_RUN_GRIDMIX.getValue();

  /**
   * Generate compressed input and run {@link Gridmix} by turning on cpu 
   * emulation feature with default settings. The {@link Gridmix} should 
   * use the following runtime parameters while running the gridmix jobs.
   * Submission Policy: STRESS, UserResolver: SubmitterUserResolver. 
   * Once the {@link Gridmix} run is complete, verify map phase cpu metrics of 
   * {@link Gridmix} jobs with their corresponding original job in a trace.
   * @throws Exception - if an error occurs.
   */
  @Test
  public void testCPUEmulatonForMapsWithCompressedInputCase1() 
      throws Exception {
    final long inputSizeInMB = 1024 * 6;
    String tracePath = getTraceFile("cpu_emul_case1");
    Assert.assertNotNull("Trace file not found!", tracePath);
    String [] runtimeValues = { "LOADJOB", 
                                SubmitterUserResolver.class.getName(), 
                                "STRESS", 
                                inputSizeInMB + "m", 
                                tracePath};

    String [] otherArgs = { 
            "-D", GridMixConfig.GRIDMIX_CPU_EMULATION + "=" + 
                  GridMixConfig.GRIDMIX_CPU_EMULATION_PLUGIN,
            "-D", GridMixConfig.GRIDMIX_DISTCACHE_ENABLE + "=false",
            "-D", GridMixConfig.GRIDMIX_HIGH_RAM_JOB_ENABLE + "=false"};

    runGridmixAndVerify(runtimeValues, otherArgs, tracePath, execMode); 
  }
  
  /**
   * Generate uncompressed input and run {@link Gridmix} by turning on 
   * cpu emulation feature with default settings. The {@link Gridmix} 
   * should use the following runtime parameters while running Gridmix jobs.
   * Submission Policy: REPLAY, UserResolver: RoundRobinUserResolver
   * Once the Gridmix run is complete, verify cpu resource metrics of 
   * {@link Gridmix} jobs with their corresponding original job in a trace.
   * @throws Exception - if an error occurs.
   */
  @Test
  public void testCPUEmulatonForMapsWithUnCompressedInputCase2() 
      throws Exception { 
    final long inputSizeInMB = cSize * 200;
    String tracePath = getTraceFile("cpu_emul_case1");
    Assert.assertNotNull("Trace file not found!", tracePath);
    String [] runtimeValues = 
            { "LOADJOB",
              RoundRobinUserResolver.class.getName(),
              "REPLAY",
              inputSizeInMB + "m",
              "file://" + UtilsForGridmix.getProxyUsersFile(conf),
              tracePath};

    String [] otherArgs = { 
            "-D", GridMixConfig.GRIDMIX_DISTCACHE_ENABLE + "=false",
            "-D", GridMixConfig.GRIDMIX_COMPRESSION_ENABLE + "=false",
            "-D", GridMixConfig.GRIDMIX_HIGH_RAM_JOB_ENABLE + "=false",
            "-D", GridMixConfig.GRIDMIX_CPU_EMULATION + "=" + 
                  GridMixConfig.GRIDMIX_CPU_EMULATION_PLUGIN};

    runGridmixAndVerify(runtimeValues, otherArgs, tracePath, 
           GridMixRunMode.DATA_GENERATION_AND_RUN_GRIDMIX.getValue());
  }
}
