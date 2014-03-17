/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce.v2;

import junit.framework.TestCase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;

import java.net.InetAddress;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.security.PrivilegedExceptionAction;

public class TestMiniMRProxyUser extends TestCase {

  private MiniDFSCluster dfsCluster = null;
  private MiniMRCluster mrCluster = null;
    
  protected void setUp() throws Exception {
    super.setUp();
    if (System.getProperty("hadoop.log.dir") == null) {
      System.setProperty("hadoop.log.dir", "/tmp");
    }
    int taskTrackers = 2;
    int dataNodes = 2;
    String proxyUser = System.getProperty("user.name");
    String proxyGroup = "g";
    StringBuilder sb = new StringBuilder();
    sb.append("127.0.0.1,localhost");
    for (InetAddress i : InetAddress.getAllByName(InetAddress.getLocalHost().getHostName())) {
      sb.append(",").append(i.getCanonicalHostName());
    }

    JobConf conf = new JobConf();
    conf.set("dfs.block.access.token.enable", "false");
    conf.set("dfs.permissions", "true");
    conf.set("hadoop.security.authentication", "simple");
    conf.set("hadoop.proxyuser." + proxyUser + ".hosts", sb.toString());
    conf.set("hadoop.proxyuser." + proxyUser + ".groups", proxyGroup);

    String[] userGroups = new String[]{proxyGroup};
    UserGroupInformation.createUserForTesting(proxyUser, userGroups);
    UserGroupInformation.createUserForTesting("u1", userGroups);
    UserGroupInformation.createUserForTesting("u2", new String[]{"gg"});

    dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(dataNodes)
        .build();
    FileSystem fileSystem = dfsCluster.getFileSystem();
    fileSystem.mkdirs(new Path("/tmp"));
    fileSystem.mkdirs(new Path("/user"));
    fileSystem.mkdirs(new Path("/hadoop/mapred/system"));
    fileSystem.setPermission(new Path("/tmp"), FsPermission.valueOf("-rwxrwxrwx"));
    fileSystem.setPermission(new Path("/user"), FsPermission.valueOf("-rwxrwxrwx"));
    fileSystem.setPermission(new Path("/hadoop/mapred/system"), FsPermission.valueOf("-rwx------"));
    String nnURI = fileSystem.getUri().toString();
    int numDirs = 1;
    String[] racks = null;
    String[] hosts = null;
    mrCluster = new MiniMRCluster(0, 0, taskTrackers, nnURI, numDirs, racks, hosts, null, conf);
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
  }

  protected JobConf getJobConf() {
    return mrCluster.createJobConf();
  }
  
  @Override
  protected void tearDown() throws Exception {
    if (mrCluster != null) {
      mrCluster.shutdown();
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
    super.tearDown();
  }

  private void mrRun() throws Exception {
    FileSystem fs = FileSystem.get(getJobConf());
    Path inputDir = new Path("input");
    fs.mkdirs(inputDir);
    Writer writer = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
    writer.write("hello");
    writer.close();

    Path outputDir = new Path("output", "output");

    JobConf jobConf = new JobConf(getJobConf());
    jobConf.setInt("mapred.map.tasks", 1);
    jobConf.setInt("mapred.map.max.attempts", 1);
    jobConf.setInt("mapred.reduce.max.attempts", 1);
    jobConf.set("mapred.input.dir", inputDir.toString());
    jobConf.set("mapred.output.dir", outputDir.toString());

    JobClient jobClient = new JobClient(jobConf);
    RunningJob runJob = jobClient.submitJob(jobConf);
    runJob.waitForCompletion();
    assertTrue(runJob.isComplete());
    assertTrue(runJob.isSuccessful());
  }
    
  public void __testCurrentUser() throws Exception {
   mrRun();
  }  

  public void testValidProxyUser() throws Exception {
    UserGroupInformation ugi = UserGroupInformation.createProxyUser("u1", UserGroupInformation.getLoginUser());
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
        public Void run() throws Exception {
          mrRun();
          return null;
        }

 
    });
  }

  public void ___testInvalidProxyUser() throws Exception {
    UserGroupInformation ugi = UserGroupInformation.createProxyUser("u2", UserGroupInformation.getLoginUser());
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
        public Void run() throws Exception {
          try {
            mrRun();
            fail();
          }
          catch (RemoteException ex) {
            //nop
          }
          catch (Exception ex) {
            fail();
          }
          return null;
        }
    });
  }
}

