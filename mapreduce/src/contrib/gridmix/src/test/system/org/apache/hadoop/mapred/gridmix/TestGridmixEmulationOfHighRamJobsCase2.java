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
import org.apache.hadoop.mapred.gridmix.GridmixJob;
import org.apache.hadoop.mapred.gridmix.test.system.UtilsForGridmix;
import org.junit.Test;
import org.junit.Assert;

/**
 * Run the {@link Gridmix} with a high ram jobs trace and 
 * verify each {@link Gridmix} job whether it honors the high ram or not.
 * In the trace the jobs should use the high ram only for maps.
 */
public class TestGridmixEmulationOfHighRamJobsCase2 
    extends GridmixSystemTestCase { 
  private static final Log LOG = 
      LogFactory.getLog("TestGridmixEmulationOfHighRamJobsCase2.class");

 /**
   * Generate input data and run {@link Gridmix} with a high ram jobs trace 
   * as a load job and REPALY submission policy in a RoundRobinUserResolver 
   * mode. Verify each {@link Gridmix} job whether it honors the high ram or not
   * after completion of execution. 
   * @throws Exception - if an error occurs.
   */
  @Test
  public void testEmulationOfHighRamForMapsOfMRJobs() 
      throws Exception { 
    final long inputSizeInMB = cSize * 300;
    String tracePath = getTraceFile("highram_mr_jobs_case2");
    Assert.assertNotNull("Trace file has not found.", tracePath);
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
               "-D", GridmixJob.GRIDMIX_HIGHRAM_EMULATION_ENABLE + "=true"};

    validateTaskMemoryParamters(tracePath, true);
    runGridmixAndVerify(runtimeValues, otherArgs, tracePath);
  }
}
