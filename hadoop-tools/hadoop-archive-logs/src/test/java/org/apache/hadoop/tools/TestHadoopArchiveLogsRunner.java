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

package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class TestHadoopArchiveLogsRunner {

  private static final int FILE_SIZE_INCREMENT = 4096;
  private static final byte[] DUMMY_DATA = new byte[FILE_SIZE_INCREMENT];
  static {
    new Random().nextBytes(DUMMY_DATA);
  }

  @Test(timeout = 50000)
  public void testHadoopArchiveLogs() throws Exception {
    MiniYARNCluster yarnCluster = null;
    MiniDFSCluster dfsCluster = null;
    FileSystem fs = null;
    try {
      Configuration conf = new YarnConfiguration();
      conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
      conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
      yarnCluster =
          new MiniYARNCluster(TestHadoopArchiveLogsRunner.class.getSimpleName(),
              1, 2, 1, 1);
      yarnCluster.init(conf);
      yarnCluster.start();
      conf = yarnCluster.getConfig();
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      conf = new JobConf(conf);

      ApplicationId app1 =
          ApplicationId.newInstance(System.currentTimeMillis(), 1);
      fs = FileSystem.get(conf);
      Path remoteRootLogDir = new Path(conf.get(
          YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
          YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
      Path workingDir = new Path(remoteRootLogDir, "archive-logs-work");
      String suffix = "logs";
      Path logDir = new Path(remoteRootLogDir,
          new Path(System.getProperty("user.name"), suffix));
      fs.mkdirs(logDir);
      Path app1Path = new Path(logDir, app1.toString());
      fs.mkdirs(app1Path);
      createFile(fs, new Path(app1Path, "log1"), 3);
      createFile(fs, new Path(app1Path, "log2"), 4);
      createFile(fs, new Path(app1Path, "log3"), 2);
      FileStatus[] app1Files = fs.listStatus(app1Path);
      Assert.assertEquals(3, app1Files.length);

      String[] args = new String[]{
          "-appId", app1.toString(),
          "-user", System.getProperty("user.name"),
          "-workingDir", workingDir.toString(),
          "-remoteRootLogDir", remoteRootLogDir.toString(),
          "-suffix", suffix};
      final HadoopArchiveLogsRunner halr = new HadoopArchiveLogsRunner(conf);
      assertEquals(0, ToolRunner.run(halr, args));

      fs = FileSystem.get(conf);
      app1Files = fs.listStatus(app1Path);
      Assert.assertEquals(1, app1Files.length);
      FileStatus harFile = app1Files[0];
      Assert.assertEquals(app1.toString() + ".har", harFile.getPath().getName());
      Path harPath = new Path("har:///" + harFile.getPath().toUri().getRawPath());
      FileStatus[] harLogs = HarFs.get(harPath.toUri(), conf).listStatus(harPath);
      Assert.assertEquals(3, harLogs.length);
      Arrays.sort(harLogs, new Comparator<FileStatus>() {
        @Override
        public int compare(FileStatus o1, FileStatus o2) {
          return o1.getPath().getName().compareTo(o2.getPath().getName());
        }
      });
      Assert.assertEquals("log1", harLogs[0].getPath().getName());
      Assert.assertEquals(3 * FILE_SIZE_INCREMENT, harLogs[0].getLen());
      Assert.assertEquals(
          new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.NONE),
          harLogs[0].getPermission());
      Assert.assertEquals(System.getProperty("user.name"),
          harLogs[0].getOwner());
      Assert.assertEquals("log2", harLogs[1].getPath().getName());
      Assert.assertEquals(4 * FILE_SIZE_INCREMENT, harLogs[1].getLen());
      Assert.assertEquals(
          new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.NONE),
          harLogs[1].getPermission());
      Assert.assertEquals(System.getProperty("user.name"),
          harLogs[1].getOwner());
      Assert.assertEquals("log3", harLogs[2].getPath().getName());
      Assert.assertEquals(2 * FILE_SIZE_INCREMENT, harLogs[2].getLen());
      Assert.assertEquals(
          new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.NONE),
          harLogs[2].getPermission());
      Assert.assertEquals(System.getProperty("user.name"),
          harLogs[2].getOwner());
      Assert.assertEquals(0, fs.listStatus(workingDir).length);
    } finally {
      if (yarnCluster != null) {
        yarnCluster.stop();
      }
      if (fs != null) {
        fs.close();
      }
      if (dfsCluster != null) {
        dfsCluster.shutdown();
      }
    }
  }

  private static void createFile(FileSystem fs, Path p, long sizeMultiple)
      throws IOException {
    FSDataOutputStream out = null;
    try {
      out = fs.create(p);
      for (int i = 0 ; i < sizeMultiple; i++) {
        out.write(DUMMY_DATA);
      }
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }
}
