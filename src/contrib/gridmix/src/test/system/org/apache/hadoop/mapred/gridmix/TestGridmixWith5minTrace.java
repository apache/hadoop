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

/**
 * Run the Gridmix with 5 minutes MR jobs trace and 
 * verify each job history against the corresponding job story 
 * in a given trace file.
 */
public class TestGridmixWith5minTrace extends GridmixSystemTestCase {
  private static final Log LOG = 
      LogFactory.getLog(TestGridmixWith5minTrace.class);

  /**
   * Generate data and run gridmix by load job with SERIAL submission 
   * policy in a SubmitterUserResolver mode against 5 minutes trace file. 
   * Verify each Gridmix job history with a corresponding job story 
   * in a trace file after completion of all the jobs.  
   * @throws Exception - if an error occurs.
   */
  @Test
  public void testGridmixWith5minTrace() throws Exception {
    final long inputSizeInMB = cSize * 300;
    final long minFileSize = 100 * 1024 * 1024;
    String [] runtimeValues ={"LOADJOB", 
                              SubmitterUserResolver.class.getName(), 
                              "SERIAL", 
                              inputSizeInMB + "m", 
                              map.get("5m")};

    String [] otherArgs = {
        "-D", GridMixConfig.GRIDMIX_DISTCACHE_ENABLE + "=false",
        "-D", GridMixConfig.GRIDMIX_COMPRESSION_ENABLE + "=false",
        "-D", GridmixJob.GRIDMIX_HIGHRAM_EMULATION_ENABLE + "=false",
        "-D", GridMixConfig.GRIDMIX_MINIMUM_FILE_SIZE + "=" + minFileSize
    };

    String tracePath = map.get("5m");
    runGridmixAndVerify(runtimeValues, otherArgs, tracePath);
  }
}

