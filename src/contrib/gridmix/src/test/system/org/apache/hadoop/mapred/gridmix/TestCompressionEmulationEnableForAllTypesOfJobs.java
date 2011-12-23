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
import org.junit.Assert;
import org.junit.Test;

/**
 * Verify the compression emulation for all the jobs in the trace 
 * irrespective of compressed inputs.
 */
public class TestCompressionEmulationEnableForAllTypesOfJobs 
    extends GridmixSystemTestCase { 
  private static final Log LOG = 
      LogFactory.getLog(
          "TestCompressionEmulationEnableForAllTypesOfJobs.class");

  /**
   *  Generate compressed input data and verify the compression emulation
   *  for all the jobs in the trace irrespective of whether the original
   *  job uses the compressed input or not.Also use the custom compression
   *  ratios for map input, map output and reduce output.
   * @throws Exception - if an error occurs.
   */
  @Test
  public void testInputCompressionEmualtionEnableForAllJobsWithCustomRatios() 
      throws Exception { 
    final long inputSizeInMB = 1024 * 6;
    final String tracePath = getTraceFile("compression_case4_trace");
    Assert.assertNotNull("Trace file has not found.", tracePath);
    final String [] runtimeValues = {"LOADJOB",
                                     SubmitterUserResolver.class.getName(),
                                     "REPLAY",
                                     inputSizeInMB + "m",
                                     tracePath};

    final String [] otherArgs = {
        "-D", GridMixConfig.GRIDMIX_DISTCACHE_ENABLE + "=false",
        "-D", GridMixConfig.GRIDMIX_COMPRESSION_ENABLE + "=true",
        "-D", GridmixJob.GRIDMIX_HIGHRAM_EMULATION_ENABLE + "=false",
        "-D", GridMixConfig.GRIDMIX_INPUT_DECOMPRESS_ENABLE + "=true",
        "-D", GridMixConfig.GRIDMIX_INPUT_COMPRESS_RATIO + "=0.46",
        "-D", GridMixConfig.GRIDMIX_INTERMEDIATE_COMPRESSION_RATIO + "=0.35",
        "-D", GridMixConfig.GRIDMIX_OUTPUT_COMPRESSION_RATIO + "=0.36"
    };

    runGridmixAndVerify(runtimeValues, otherArgs, tracePath, 
        GridMixRunMode.DATA_GENERATION_AND_RUN_GRIDMIX.getValue());
  }
  
  /**
   *  Use existing compressed input data and turn off the compression 
   *  emulation. Verify the compression emulation whether it uses 
   *  by the jobs or not.
   * @throws Exception - if an error occurs.
   */
  @Test
  public void testInputCompressionEmulationDisableAllJobs() 
      throws Exception { 
     final String tracePath = getTraceFile("compression_case4_trace");
     Assert.assertNotNull("Trace file has not found.", tracePath);
     final String [] runtimeValues = {"LOADJOB",
                                      SubmitterUserResolver.class.getName(),
                                      "SERIAL",
                                      tracePath};

    final String [] otherArgs = {
        "-D", GridMixConfig.GRIDMIX_DISTCACHE_ENABLE + "=false",
        "-D", GridmixJob.GRIDMIX_HIGHRAM_EMULATION_ENABLE + "=false",
        "-D", GridMixConfig.GRIDMIX_COMPRESSION_ENABLE + "=false"
    };

    runGridmixAndVerify(runtimeValues, otherArgs, tracePath, 
                        GridMixRunMode.RUN_GRIDMIX.getValue());
  }  
}

