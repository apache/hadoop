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

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.mapred.gridmix.test.system.GridMixConfig;
import org.apache.hadoop.mapred.gridmix.test.system.GridMixRunMode;
import org.apache.hadoop.mapred.gridmix.test.system.UtilsForGridmix;
import org.junit.Test;
import org.junit.Assert;

/**
 * Run the {@link Gridmix} with combination of high ram and normal jobs of
 * trace and verify whether high ram jobs{@link Gridmix} are honoring or not.
 * Normal MR jobs should not honors the high ram emulation.
 */
public class TestEmulationOfHighRamAndNormalMRJobs
    extends GridmixSystemTestCase { 
  private static final Log LOG = 
      LogFactory.getLog("TestEmulationOfHighRamAndNormalMRJobs.class");

  /**
   * Generate input data and run the combination normal and high ram 
   * {@link Gridmix} jobs as load job and STRESS submission policy 
   * in a SubmitterUserResolver mode. Verify whether each {@link Gridmix} 
   * job honors the high ram or not after completion of execution. 
   * @throws Exception - if an error occurs.
   */
  @Test
  public void testEmulationOfHighRamForReducersOfMRJobs() 
      throws Exception { 
    final long inputSizeInMB = cSize * 250;
    String tracePath = getTraceFile("highram_mr_jobs_case4");
    Assert.assertNotNull("Trace file has not found.", tracePath);
    String [] runtimeArgs = {"LOADJOB",
                               SubmitterUserResolver.class.getName(),
                               "SERIAL",
                               inputSizeInMB + "m",
                               tracePath};
    String [] otherArgs = {
            "-D", GridMixConfig.GRIDMIX_DISTCACHE_ENABLE + "=false", 
            "-D", GridMixConfig.GRIDMIX_COMPRESSION_ENABLE + "=false", 
            "-D", GridMixConfig.GRIDMIX_HIGH_RAM_JOB_ENABLE + "=true"};

    validateTaskMemoryParamters(tracePath, true);
    runGridmixAndVerify(runtimeArgs, otherArgs, tracePath);
  }
}
