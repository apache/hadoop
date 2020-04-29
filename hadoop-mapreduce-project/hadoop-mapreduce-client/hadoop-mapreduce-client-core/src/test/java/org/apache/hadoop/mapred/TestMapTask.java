/*
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

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.MapTask.MapOutputBuffer;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.Progress;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class TestMapTask {
  private static File TEST_ROOT_DIR = new File(
      System.getProperty("test.build.data",
          System.getProperty("java.io.tmpdir", "/tmp")),
      TestMapTask.class.getName());

  @After
  public void cleanup() throws Exception {
    FileUtil.fullyDelete(TEST_ROOT_DIR);
  }

  // Verify output files for shuffle have group read permission even when
  // the configured umask normally would prevent it.
  @Test
  public void testShufflePermissions() throws Exception {
    JobConf conf = new JobConf();
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "077");
    conf.set(MRConfig.LOCAL_DIR, TEST_ROOT_DIR.getAbsolutePath());
    MapOutputFile mof = new MROutputFiles();
    mof.setConf(conf);
    TaskAttemptID attemptId = new TaskAttemptID("12345", 1, TaskType.MAP, 1, 1);
    MapTask mockTask = mock(MapTask.class);
    doReturn(mof).when(mockTask).getMapOutputFile();
    doReturn(attemptId).when(mockTask).getTaskID();
    doReturn(new Progress()).when(mockTask).getSortPhase();
    TaskReporter mockReporter = mock(TaskReporter.class);
    doReturn(new Counter()).when(mockReporter).getCounter(
        any(TaskCounter.class));
    MapOutputCollector.Context ctx = new MapOutputCollector.Context(mockTask,
        conf, mockReporter);
    MapOutputBuffer<Object, Object> mob = new MapOutputBuffer<>();
    mob.init(ctx);
    mob.flush();
    mob.close();
    Path outputFile = mof.getOutputFile();
    FileSystem lfs = FileSystem.getLocal(conf);
    FsPermission perms = lfs.getFileStatus(outputFile).getPermission();
    Assert.assertEquals("Incorrect output file perms",
        (short)0640, perms.toShort());
    Path indexFile = mof.getOutputIndexFile();
    perms = lfs.getFileStatus(indexFile).getPermission();
    Assert.assertEquals("Incorrect index file perms",
        (short)0640, perms.toShort());
  }
}
