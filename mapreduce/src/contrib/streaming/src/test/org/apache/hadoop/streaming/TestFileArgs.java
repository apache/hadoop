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
import java.net.URI;
import java.util.zip.GZIPOutputStream;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;

/**
 * This class tests that the '-file' argument to streaming results
 * in files being unpacked in the job working directory.
 */
public class TestFileArgs extends TestStreaming
{
  private MiniDFSCluster dfs = null;
  private MiniMRCluster mr = null;
  private FileSystem fileSys = null;
  private String strJobTracker = null;
  private String strNamenode = null;
  private String namenode = null;
  private Configuration conf = null;

  private static final String EXPECTED_OUTPUT =
    "job.jar\t\nsidefile\t\ntmp\t\n";

  private static final String LS_PATH = "/bin/ls";

  public TestFileArgs() throws IOException
  {
    // Set up mini cluster
    conf = new Configuration();
    dfs = new MiniDFSCluster(conf, 1, true, null);
    fileSys = dfs.getFileSystem();
    namenode = fileSys.getUri().getAuthority();
    mr  = new MiniMRCluster(1, namenode, 1);
    strJobTracker = JTConfig.JT_IPC_ADDRESS + "=localhost:" + mr.getJobTrackerPort();
    strNamenode = "fs.default.name=hdfs://" + namenode;

    FileSystem.setDefaultUri(conf, "hdfs://" + namenode);

    // Set up side file
    FileSystem localFs = FileSystem.getLocal(conf);
    DataOutputStream dos = localFs.create(new Path("sidefile"));
    dos.write("hello world\n".getBytes("UTF-8"));
    dos.close();
  }

  @Override
  protected String getExpectedOutput() {
    return EXPECTED_OUTPUT;
  }

  @Override
  protected Configuration getConf() {
    return conf;
  }

  @Override
  protected String[] genArgs() {
    return new String[] {
      "-input", INPUT_FILE.getAbsolutePath(),
      "-output", OUTPUT_DIR.getAbsolutePath(),
      "-file", new java.io.File("sidefile").getAbsolutePath(),
      "-mapper", LS_PATH,
      "-numReduceTasks", "0",
      "-jobconf", strNamenode,
      "-jobconf", strJobTracker,
      "-jobconf", "stream.tmpdir=" + System.getProperty("test.build.data","/tmp")
    };
  }


  public static void main(String[]args) throws Exception
  {
    new TestFileArgs().testCommandLine();
  }

}
