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
import org.junit.Test;
import org.junit.Assert;

/**
 * Run the Gridmix with 5 minutes job trace which has been generated with 
 * streaming jobs histories and verify each job history against 
 * corresponding job story in a given trace file.
 */
public class TestGridmixWith5minStreamingJobTrace 
    extends GridmixSystemTestCase {
  private static final Log LOG = 
      LogFactory.getLog("TestGridmixWith5minStreamingJobTrace.class");

  /**
   * Generate input data and run gridmix by load job with SERIAL submission 
   * policy in a SubmitterUserResolver mode against 5 minutes job trace file 
   * of streaming job. Verify each gridmix job history with a corresponding 
   * job story in a trace file after completion of all the jobs execution.
   * @throws Exception - if an error occurs.
   */
  @Test
  public void testGridmixWith5minStreamJobTrace() throws Exception {
    String tracePath = getTraceFile("5m_stream");
    Assert.assertNotNull("Trace file has not found.", tracePath);
    final long inputSizeInMB = cSize * 200;
    final long bytesPerFile = 150 * 1024 * 1024;
    String [] runtimeValues = {"LOADJOB", 
                              SubmitterUserResolver.class.getName(), 
                              "SERIAL", 
                              inputSizeInMB + "m",
                              tracePath};

    String [] otherArgs = {
        "-D", GridMixConfig.GRIDMIX_KEY_FRC + "=0.5f",
        "-D", GridMixConfig.GRIDMIX_BYTES_PER_FILE + "=" + bytesPerFile,
        "-D", GridMixConfig.GRIDMIX_DISTCACHE_ENABLE + "=false",
        "-D", GridmixJob.GRIDMIX_HIGHRAM_EMULATION_ENABLE + "=false",
        "-D", GridMixConfig.GRIDMIX_COMPRESSION_ENABLE + "=false"
    };

    runGridmixAndVerify(runtimeValues, otherArgs, tracePath);
  }
}
