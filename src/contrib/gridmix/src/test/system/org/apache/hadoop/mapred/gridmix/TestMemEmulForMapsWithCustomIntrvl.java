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
import org.junit.Test;
import org.junit.Assert;

/**
 * Test the {@link Gridmix} memory emulation feature for {@link Gridmix} jobs 
 * with custom progress interval, different input data, submission policies 
 * and user resolver modes. Verify the total heap usage of map tasks of
 * the jobs with corresponding the original job in the trace. 
 */
public class TestMemEmulForMapsWithCustomIntrvl extends GridmixSystemTestCase { 
  private static final Log LOG = 
      LogFactory.getLog("TestMemEmulForMapsWithCustomIntrvl.class");

  /**
   * Generate compressed input and run {@link Gridmix} by turning on the
   * memory emulation with custom progress interval. The {@link Gridmix}
   * should use the following runtime parameters while running the jobs.
   * Submission Policy : STRESS, User Resolver Mode : SumitterUserResolver
   * Verify maps total heap memory usage of {@link Gridmix} jobs with 
   * corresponding original job in the trace. 
   * @throws Exception - if an error occurs.
   */
  @Test
  public void testMemoryEmulationForMapsWithCompressedInputCase3() 
     throws Exception {
    final long inputSizeInMB = 1024 * 7;
    String tracePath = getTraceFile("mem_emul_case1");
    Assert.assertNotNull("Trace file has not found.", tracePath);
    String [] runtimeValues = 
            { "LOADJOB",
              SubmitterUserResolver.class.getName(),
              "STRESS",
              inputSizeInMB + "m",
              tracePath};

    String [] otherArgs = { 
            "-D", GridMixConfig.GRIDMIX_MEMORY_EMULATION + "="  +
                  GridMixConfig.GRIDMIX_MEMORY_EMULATION_PLUGIN,
            "-D", GridMixConfig.GRIDMIX_HEAP_MEMORY_CUSTOM_INTRVL + "=0.2F",
            "-D", GridMixConfig.GRIDMIX_DISTCACHE_ENABLE + "=false",
            "-D", JobContext.JOB_CANCEL_DELEGATION_TOKEN + "=false"};

    runGridmixAndVerify(runtimeValues, otherArgs, tracePath, 
           GridMixRunMode.DATA_GENERATION_AND_RUN_GRIDMIX.getValue());
  }

  /**
   * Generate uncompressed input and run {@link Gridmix} by turning on the
   * memory emulation with custom progress interval. The {@link Gridmix} 
   * should use the following runtime parameters while running the jobs.
   *  Submission Policy : STRESS, User Resolver Mode : RoundRobinUserResolver
   * Verify maps total heap memory usage of {@link Gridmix} jobs with 
   * corresponding original job in the trace. 
   * @throws Exception - if an error occurs.
   */
  @Test
  public void testMemoryEmulationForMapsWithUncompressedInputCase4() 
      throws Exception {
    final long inputSizeInMB = cSize * 300;
    String tracePath = getTraceFile("mem_emul_case1");
    Assert.assertNotNull("Trace file has not found.", tracePath);
    String [] runtimeValues = 
              { "LOADJOB", 
                RoundRobinUserResolver.class.getName(), 
                "STRESS",
                inputSizeInMB + "m",
                "file://" + UtilsForGridmix.getProxyUsersFile(conf),
                tracePath};

    String [] otherArgs = {
            "-D", GridMixConfig.GRIDMIX_MEMORY_EMULATION + "=" +  
                  GridMixConfig.GRIDMIX_MEMORY_EMULATION_PLUGIN,
            "-D", GridMixConfig.GRIDMIX_HEAP_MEMORY_CUSTOM_INTRVL + "=0.3F",
            "-D", GridMixConfig.GRIDMIX_DISTCACHE_ENABLE + "=false",
            "-D", GridMixConfig.GRIDMIX_COMPRESSION_ENABLE + "=false",
            "-D", JobContext.JOB_CANCEL_DELEGATION_TOKEN + "=false"};

    runGridmixAndVerify(runtimeValues, otherArgs, tracePath, 
            GridMixRunMode.DATA_GENERATION_AND_RUN_GRIDMIX.getValue());
  }
}
