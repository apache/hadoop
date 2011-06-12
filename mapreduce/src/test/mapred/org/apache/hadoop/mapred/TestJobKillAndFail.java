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

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;

/**
 * A JUnit test to test Kill Job & Fail Job functionality with local file
 * system.
 */
public class TestJobKillAndFail extends TestCase {

  private static String TEST_ROOT_DIR = new File(System.getProperty(
      "test.build.data", "/tmp")).toURI().toString().replace(' ', '+');

  public void testJobFailAndKill() throws IOException {
    MiniMRCluster mr = null;
    try {
      mr = new MiniMRCluster(2, "file:///", 3);

      // run the TCs
      JobConf conf = mr.createJobConf();

      Path inDir = new Path(TEST_ROOT_DIR + "/failkilljob/input");
      Path outDir = new Path(TEST_ROOT_DIR + "/failkilljob/output");
      RunningJob job = UtilsForTests.runJobFail(conf, inDir, outDir);
      // Checking that the Job got failed
      assertEquals(job.getJobState(), JobStatus.FAILED);

      job = UtilsForTests.runJobKill(conf, inDir, outDir);
      // Checking that the Job got killed
      assertTrue(job.isComplete());
      assertEquals(job.getJobState(), JobStatus.KILLED);
    } finally {
      if (mr != null) {
        mr.shutdown();
      }
    }
  }
}
