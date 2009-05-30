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

package org.apache.hadoop.mapred;

import org.apache.hadoop.fs.Path;

/**
 * Test a java-based mapred job with LinuxTaskController running the jobs as a
 * user different from the user running the cluster. See
 * {@link ClusterWithLinuxTaskController}
 */
public class TestJobExecutionAsDifferentUser extends
    ClusterWithLinuxTaskController {

  public void testJobExecution()
      throws Exception {
    if (!shouldRun()) {
      return;
    }
    startCluster();
    Path inDir = new Path("input");
    Path outDir = new Path("output");
    RunningJob job =
        UtilsForTests.runJobSucceed(getClusterConf(), inDir, outDir);
    assertTrue("Job failed", job.isSuccessful());
    assertOwnerShip(outDir);
  }
}
