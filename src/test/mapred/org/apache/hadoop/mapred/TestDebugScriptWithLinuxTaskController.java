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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskType;
import org.junit.Test;

public class TestDebugScriptWithLinuxTaskController extends
    ClusterWithLinuxTaskController {

  @Test
  public void testDebugScriptExecutionAsDifferentUser() throws Exception {
    if (!super.shouldRun()) {
      return;
    }
    super.startCluster();
    TestDebugScript.setupDebugScriptDirs();
    Path inDir = new Path("input");
    Path outDir = new Path("output");
    JobConf conf = super.getClusterConf();
    FileSystem fs = inDir.getFileSystem(conf);
    fs.mkdirs(inDir);
    Path p = new Path(inDir, "1.txt");
    fs.createNewFile(p);
    JobID jobId = TestDebugScript.runFailingMapJob(super.getClusterConf(), 
        inDir, outDir);
    String ugi = System
        .getProperty(ClusterWithLinuxTaskController.TASKCONTROLLER_UGI);
    // construct the task id of first map task of failmap
    TaskAttemptID taskId = new TaskAttemptID(
        new TaskID(jobId,TaskType.MAP, 0), 0);
    TestDebugScript.verifyDebugScriptOutput(taskId, ugi.split(",")[0],
        "-rw-rw----");
    TestDebugScript.cleanupDebugScriptDirs();
  }
}
