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

package org.apache.hadoop.streaming;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterWithLinuxTaskController;
import org.apache.hadoop.mapred.JobConf;

/**
 * Test Streaming with LinuxTaskController running the jobs as a user different
 * from the user running the cluster. See {@link ClusterWithLinuxTaskController}
 */
public class TestStreamingAsDifferentUser extends
    ClusterWithLinuxTaskController {

  private Path inputPath = new Path("input");
  private Path outputPath = new Path("output");
  private String input = "roses.are.red\nviolets.are.blue\nbunnies.are.pink\n";
  private String map =
      StreamUtil.makeJavaCommand(TrApp.class, new String[] { ".", "\\n" });
  private String reduce =
      StreamUtil.makeJavaCommand(UniqApp.class, new String[] { "R" });

  public void testStreaming()
      throws Exception {
    if (!shouldRun()) {
      return;
    }
    startCluster();
    JobConf myConf = getClusterConf();
    FileSystem inFs = inputPath.getFileSystem(myConf);
    FileSystem outFs = outputPath.getFileSystem(myConf);
    outFs.delete(outputPath, true);
    if (!inFs.mkdirs(inputPath)) {
      throw new IOException("Mkdirs failed to create " + inFs.toString());
    }
    DataOutputStream file = inFs.create(new Path(inputPath, "part-0"));
    file.writeBytes(input);
    file.close();
    String[] args =
        new String[] { "-input", inputPath.makeQualified(inFs).toString(),
            "-output", outputPath.makeQualified(outFs).toString(), "-mapper",
            map, "-reducer", reduce, "-jobconf",
            "keep.failed.task.files=true", "-jobconf",
            "stream.tmpdir=" + System.getProperty("test.build.data", "/tmp") };
    StreamJob streamJob = new StreamJob(args, true);
    streamJob.setConf(myConf);
    streamJob.go();
    assertOwnerShip(outputPath);
  }
}
