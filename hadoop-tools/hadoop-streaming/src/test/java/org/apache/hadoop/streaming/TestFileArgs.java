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
import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.util.Shell;
import org.junit.After;
import org.junit.Before;

/**
 * This class tests that the '-file' argument to streaming results
 * in files being unpacked in the job working directory.
 */
public class TestFileArgs extends TestStreaming
{
  private MiniDFSCluster dfs = null;
  private MiniMRCluster mr = null;
  private FileSystem fileSys = null;
  private String namenode = null;
  private Configuration conf = null;

  private static final String EXPECTED_OUTPUT =
    "job.jar\t\nsidefile\t\n";

  private static final String LS_PATH = Shell.WINDOWS ? "cmd /c dir /B" :
    "/bin/ls";

  public TestFileArgs() throws IOException
  {
    // Set up mini cluster
    conf = new Configuration();
    dfs = new MiniDFSCluster.Builder(conf).build();
    fileSys = dfs.getFileSystem();
    namenode = fileSys.getUri().getAuthority();
    mr  = new MiniMRCluster(1, namenode, 1);

    map = LS_PATH;
    FileSystem.setDefaultUri(conf, "hdfs://" + namenode);
    setTestDir(new File("/tmp/TestFileArgs"));
  }

  @Before
  @Override
  public void setUp() throws IOException {
    // Set up side file
    FileSystem localFs = FileSystem.getLocal(conf);
    DataOutputStream dos = localFs.create(new Path("target/sidefile"));
    dos.write("hello world\n".getBytes("UTF-8"));
    dos.close();

    // Since ls doesn't read stdin, we don't want to write anything
    // to it, or else we risk Broken Pipe exceptions.
    input = "";
  }

  @After
  @Override
  public void tearDown() {
    if (mr != null) {
      mr.shutdown();
    }
    if (dfs != null) {
      dfs.shutdown();
    }
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
    for (Map.Entry<String, String> entry : mr.createJobConf()) {
      args.add("-jobconf");
      args.add(entry.getKey() + "=" + entry.getValue());
    }
    args.add("-file");
    args.add(new java.io.File("target/sidefile").getAbsolutePath());
    args.add("-numReduceTasks");
    args.add("0");
    args.add("-jobconf");
    args.add("mapred.jar=" + STREAMING_JAR);
    args.add("-verbose");
    return super.genArgs();
  }
}
