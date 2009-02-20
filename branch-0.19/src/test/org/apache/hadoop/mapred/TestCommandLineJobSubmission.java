/* Licensed to the Apache Software Foundation (ASF) under one
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
import java.io.FileOutputStream;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hdfs.MiniDFSCluster;

/**
 * check for the job submission  options of 
 * -libjars -files -archives
 */

public class TestCommandLineJobSubmission extends TestCase {
  // Input output paths for this.. 
  // these are all dummy and does not test
  // much in map reduce except for the command line
  // params 
  static final Path input = new Path("/test/input/");
  static final Path output = new Path("/test/output");
  File buildDir = new File(System.getProperty("test.build.data", "/tmp"));
  public void testJobShell() throws Exception {
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fs = null;
    Path testFile = new Path(input, "testfile");
    try {
      Configuration conf = new Configuration();
      //start the mini mr and dfs cluster.
      dfs = new MiniDFSCluster(conf, 2 , true, null);
      fs = dfs.getFileSystem();
      FSDataOutputStream stream = fs.create(testFile);
      stream.write("teststring".getBytes());
      stream.close();
      mr = new MiniMRCluster(2, fs.getUri().toString(), 1);
      File thisbuildDir = new File(buildDir, "jobCommand");
      assertTrue("create build dir", thisbuildDir.mkdirs()); 
      File f = new File(thisbuildDir, "files_tmp");
      FileOutputStream fstream = new FileOutputStream(f);
      fstream.write("somestrings".getBytes());
      fstream.close();
      String[] args = new String[6];
      args[0] = "-files";
      args[1] = f.toString();
      args[2] = "-libjars";
      // the testjob.jar as a temporary jar file 
      // rather than creating its own
      args[3] = "build/test/testjar/testjob.jar";
      args[4] = input.toString();
      args[5] = output.toString();
      
      JobConf jobConf = mr.createJobConf();
      //before running the job, verify that libjar is not in client classpath
      assertTrue("libjar not in client classpath", loadLibJar(jobConf)==null);
      int ret = ToolRunner.run(jobConf,
                               new testshell.ExternalMapReduce(), args);
      //after running the job, verify that libjar is in the client classpath
      assertTrue("libjar added to client classpath", loadLibJar(jobConf)!=null);
      
      assertTrue("not failed ", ret != -1);
      f.delete();
      thisbuildDir.delete();
    } finally {
      if (dfs != null) {dfs.shutdown();};
      if (mr != null) {mr.shutdown();};
    }
  }
  
  @SuppressWarnings("unchecked")
  private Class loadLibJar(JobConf jobConf) {
    try {
      return jobConf.getClassByName("testjar.ClassWordCount");
    } catch (ClassNotFoundException e) {
      return null;
    }
  }
}
