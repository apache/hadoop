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

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherEvent;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerRemoteLaunchEvent;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLocalContainerLauncher {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestLocalContainerLauncher.class);
  private static File testWorkDir;
  private static final String[] localDirs = new String[2];

  private static void delete(File dir) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    Path p = fs.makeQualified(new Path(dir.getAbsolutePath()));
    fs.delete(p, true);
  }

  @BeforeClass
  public static void setupTestDirs() throws IOException {
    testWorkDir = new File("target",
        TestLocalContainerLauncher.class.getCanonicalName());
    testWorkDir.delete();
    testWorkDir.mkdirs();
    testWorkDir = testWorkDir.getAbsoluteFile();
    for (int i = 0; i < localDirs.length; i++) {
      final File dir = new File(testWorkDir, "local-" + i);
      dir.mkdirs();
      localDirs[i] = dir.toString();
    }
  }

  @AfterClass
  public static void cleanupTestDirs() throws IOException {
    if (testWorkDir != null) {
      delete(testWorkDir);
    }
  }

  @SuppressWarnings("rawtypes")
  @Test(timeout=10000)
  public void testKillJob() throws Exception {
    JobConf conf = new JobConf();
    AppContext context = mock(AppContext.class);
    // a simple event handler solely to detect the container cleaned event
    final CountDownLatch isDone = new CountDownLatch(1);
    EventHandler<Event> handler = new EventHandler<Event>() {
      @Override
      public void handle(Event event) {
        LOG.info("handling event " + event.getClass() +
            " with type " + event.getType());
        if (event instanceof TaskAttemptEvent) {
          if (event.getType() == TaskAttemptEventType.TA_CONTAINER_CLEANED) {
            isDone.countDown();
          }
        }
      }
    };
    when(context.getEventHandler()).thenReturn(handler);

    // create and start the launcher
    LocalContainerLauncher launcher =
        new LocalContainerLauncher(context, mock(TaskUmbilicalProtocol.class));
    launcher.init(conf);
    launcher.start();

    // create mocked job, task, and task attempt
    // a single-mapper job
    JobId jobId = MRBuilderUtils.newJobId(System.currentTimeMillis(), 1, 1);
    TaskId taskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.MAP);
    TaskAttemptId taId = MRBuilderUtils.newTaskAttemptId(taskId, 0);

    Job job = mock(Job.class);
    when(job.getTotalMaps()).thenReturn(1);
    when(job.getTotalReduces()).thenReturn(0);
    Map<JobId,Job> jobs = new HashMap<JobId,Job>();
    jobs.put(jobId, job);
    // app context returns the one and only job
    when(context.getAllJobs()).thenReturn(jobs);

    org.apache.hadoop.mapreduce.v2.app.job.Task ytask =
        mock(org.apache.hadoop.mapreduce.v2.app.job.Task.class);
    when(ytask.getType()).thenReturn(TaskType.MAP);
    when(job.getTask(taskId)).thenReturn(ytask);

    // create a sleeping mapper that runs beyond the test timeout
    MapTask mapTask = mock(MapTask.class);
    when(mapTask.isMapOrReduce()).thenReturn(true);
    when(mapTask.isMapTask()).thenReturn(true);
    TaskAttemptID taskID = TypeConverter.fromYarn(taId);
    when(mapTask.getTaskID()).thenReturn(taskID);
    when(mapTask.getJobID()).thenReturn(taskID.getJobID());
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        // sleep for a long time
        LOG.info("sleeping for 5 minutes...");
        Thread.sleep(5*60*1000);
        return null;
      }
    }).when(mapTask).run(isA(JobConf.class), isA(TaskUmbilicalProtocol.class));

    // pump in a task attempt launch event
    ContainerLauncherEvent launchEvent =
        new ContainerRemoteLaunchEvent(taId, null, createMockContainer(), mapTask);
    launcher.handle(launchEvent);

    Thread.sleep(200);
    // now pump in a container clean-up event
    ContainerLauncherEvent cleanupEvent =
        new ContainerLauncherEvent(taId, null, null, null,
            ContainerLauncher.EventType.CONTAINER_REMOTE_CLEANUP);
    launcher.handle(cleanupEvent);

    // wait for the event to fire: this should be received promptly
    isDone.await();

    launcher.close();
  }

  private static Container createMockContainer() {
    Container container = mock(Container.class);
    NodeId nodeId = NodeId.newInstance("foo.bar.org", 1234);
    when(container.getNodeId()).thenReturn(nodeId);
    return container;
  }


  @Test
  public void testRenameMapOutputForReduce() throws Exception {
    final JobConf conf = new JobConf();

    final MROutputFiles mrOutputFiles = new MROutputFiles();
    mrOutputFiles.setConf(conf);

    // make sure both dirs are distinct
    //
    conf.set(MRConfig.LOCAL_DIR, localDirs[0].toString());
    final Path mapOut = mrOutputFiles.getOutputFileForWrite(1);
    conf.set(MRConfig.LOCAL_DIR, localDirs[1].toString());
    final Path mapOutIdx = mrOutputFiles.getOutputIndexFileForWrite(1);
    Assert.assertNotEquals("Paths must be different!",
        mapOut.getParent(), mapOutIdx.getParent());

    // make both dirs part of LOCAL_DIR
    conf.setStrings(MRConfig.LOCAL_DIR, localDirs);

    final FileContext lfc = FileContext.getLocalFSFileContext(conf);
    lfc.create(mapOut, EnumSet.of(CREATE)).close();
    lfc.create(mapOutIdx, EnumSet.of(CREATE)).close();

    final JobId jobId = MRBuilderUtils.newJobId(12345L, 1, 2);
    final TaskId tid = MRBuilderUtils.newTaskId(jobId, 0, TaskType.MAP);
    final TaskAttemptId taid = MRBuilderUtils.newTaskAttemptId(tid, 0);

    LocalContainerLauncher.renameMapOutputForReduce(conf, taid, mrOutputFiles);
  }
}
