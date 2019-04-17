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

package org.apache.hadoop.tools.mapred;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.tools.CopyListing;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpContext;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;
import org.apache.hadoop.tools.GlobbedCopyListing;
import org.apache.hadoop.tools.util.TestDistCpUtils;
import org.apache.hadoop.security.Credentials;
import org.junit.*;

import java.io.IOException;
import java.util.*;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.tools.util.TestDistCpUtils.*;

public class TestCopyCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(TestCopyCommitter.class);

  private static final Random rand = new Random();

  private static final Credentials CREDENTIALS = new Credentials();
  public static final int PORT = 39737;


  private static Configuration config;
  private static MiniDFSCluster cluster;

  private static Job getJobForClient() throws IOException {
    Job job = Job.getInstance(new Configuration());
    job.getConfiguration().set("mapred.job.tracker", "localhost:" + PORT);
    job.setInputFormatClass(NullInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setNumReduceTasks(0);
    return job;
  }

  @BeforeClass
  public static void create() throws IOException {
    config = getJobForClient().getConfiguration();
    config.setLong(DistCpConstants.CONF_LABEL_TOTAL_BYTES_TO_BE_COPIED, 0);
    cluster = new MiniDFSCluster.Builder(config).numDataNodes(1).format(true)
                      .build();
  }

  @AfterClass
  public static void destroy() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void createMetaFolder() throws IOException {
    config.set(DistCpConstants.CONF_LABEL_META_FOLDER, "/meta");
    Path meta = new Path("/meta");
    cluster.getFileSystem().mkdirs(meta);
  }

  @After
  public void cleanupMetaFolder() throws IOException {
    Path meta = new Path("/meta");
    if (cluster.getFileSystem().exists(meta)) {
      cluster.getFileSystem().delete(meta, true);
      Assert.fail("Expected meta folder to be deleted");
    }
  }

  @Test
  public void testNoCommitAction() throws IOException {
    TaskAttemptContext taskAttemptContext = getTaskAttemptContext(config);
    JobContext jobContext = new JobContextImpl(
        taskAttemptContext.getConfiguration(),
        taskAttemptContext.getTaskAttemptID().getJobID());
    OutputCommitter committer = new CopyCommitter(null, taskAttemptContext);
    committer.commitJob(jobContext);
    Assert.assertEquals("Commit Successful", taskAttemptContext.getStatus());

    //Test for idempotent commit
    committer.commitJob(jobContext);
    Assert.assertEquals("Commit Successful", taskAttemptContext.getStatus());
  }

  @Test
  public void testPreserveStatus() throws IOException {
    TaskAttemptContext taskAttemptContext = getTaskAttemptContext(config);
    JobContext jobContext = new JobContextImpl(taskAttemptContext.getConfiguration(),
        taskAttemptContext.getTaskAttemptID().getJobID());
    Configuration conf = jobContext.getConfiguration();


    String sourceBase;
    String targetBase;
    FileSystem fs = null;
    try {
      OutputCommitter committer = new CopyCommitter(null, taskAttemptContext);
      fs = FileSystem.get(conf);
      FsPermission sourcePerm = new FsPermission((short) 511);
      FsPermission initialPerm = new FsPermission((short) 448);
      sourceBase = TestDistCpUtils.createTestSetup(fs, sourcePerm);
      targetBase = TestDistCpUtils.createTestSetup(fs, initialPerm);

      final DistCpOptions options = new DistCpOptions.Builder(
          Collections.singletonList(new Path(sourceBase)), new Path("/out"))
          .preserve(FileAttribute.PERMISSION).build();
      options.appendToConf(conf);
      final DistCpContext context = new DistCpContext(options);
      context.setTargetPathExists(false);

      CopyListing listing = new GlobbedCopyListing(conf, CREDENTIALS);
      Path listingFile = new Path("/tmp1/" + String.valueOf(rand.nextLong()));
      listing.buildListing(listingFile, context);

      conf.set(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH, targetBase);

      committer.commitJob(jobContext);
      checkDirectoryPermissions(fs, targetBase, sourcePerm);

      //Test for idempotent commit
      committer.commitJob(jobContext);
      checkDirectoryPermissions(fs, targetBase, sourcePerm);

    } finally {
      TestDistCpUtils.delete(fs, "/tmp1");
      conf.unset(DistCpConstants.CONF_LABEL_PRESERVE_STATUS);
    }

  }

  @Test
  public void testDeleteMissing() throws IOException {
    TaskAttemptContext taskAttemptContext = getTaskAttemptContext(config);
    JobContext jobContext = new JobContextImpl(taskAttemptContext.getConfiguration(),
        taskAttemptContext.getTaskAttemptID().getJobID());
    Configuration conf = jobContext.getConfiguration();

    String sourceBase;
    String targetBase;
    FileSystem fs = null;
    try {
      OutputCommitter committer = new CopyCommitter(null, taskAttemptContext);
      fs = FileSystem.get(conf);
      sourceBase = TestDistCpUtils.createTestSetup(fs, FsPermission.getDefault());
      targetBase = TestDistCpUtils.createTestSetup(fs, FsPermission.getDefault());
      String targetBaseAdd = TestDistCpUtils.createTestSetup(fs, FsPermission.getDefault());
      fs.rename(new Path(targetBaseAdd), new Path(targetBase));

      final DistCpOptions options = new DistCpOptions.Builder(
          Collections.singletonList(new Path(sourceBase)), new Path("/out"))
          .withSyncFolder(true).withDeleteMissing(true).build();
      options.appendToConf(conf);
      final DistCpContext context = new DistCpContext(options);

      CopyListing listing = new GlobbedCopyListing(conf, CREDENTIALS);
      Path listingFile = new Path("/tmp1/" + String.valueOf(rand.nextLong()));
      listing.buildListing(listingFile, context);

      conf.set(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH, targetBase);
      conf.set(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH, targetBase);

      committer.commitJob(jobContext);
      verifyFoldersAreInSync(fs, targetBase, sourceBase);
      verifyFoldersAreInSync(fs, sourceBase, targetBase);

      //Test for idempotent commit
      committer.commitJob(jobContext);
      verifyFoldersAreInSync(fs, targetBase, sourceBase);
      verifyFoldersAreInSync(fs, sourceBase, targetBase);
    } finally {
      TestDistCpUtils.delete(fs, "/tmp1");
      conf.set(DistCpConstants.CONF_LABEL_DELETE_MISSING, "false");
    }
  }

  @Test
  public void testDeleteMissingFlatInterleavedFiles() throws IOException {
    TaskAttemptContext taskAttemptContext = getTaskAttemptContext(config);
    JobContext jobContext = new JobContextImpl(taskAttemptContext.getConfiguration(),
        taskAttemptContext.getTaskAttemptID().getJobID());
    Configuration conf = jobContext.getConfiguration();


    String sourceBase;
    String targetBase;
    FileSystem fs = null;
    try {
      OutputCommitter committer = new CopyCommitter(null, taskAttemptContext);
      fs = FileSystem.get(conf);
      sourceBase = "/tmp1/" + String.valueOf(rand.nextLong());
      targetBase = "/tmp1/" + String.valueOf(rand.nextLong());
      createFile(fs, sourceBase + "/1");
      createFile(fs, sourceBase + "/3");
      createFile(fs, sourceBase + "/4");
      createFile(fs, sourceBase + "/5");
      createFile(fs, sourceBase + "/7");
      createFile(fs, sourceBase + "/8");
      createFile(fs, sourceBase + "/9");

      createFile(fs, targetBase + "/2");
      createFile(fs, targetBase + "/4");
      createFile(fs, targetBase + "/5");
      createFile(fs, targetBase + "/7");
      createFile(fs, targetBase + "/9");
      createFile(fs, targetBase + "/A");

      final DistCpOptions options = new DistCpOptions.Builder(
          Collections.singletonList(new Path(sourceBase)), new Path("/out"))
          .withSyncFolder(true).withDeleteMissing(true).build();
      options.appendToConf(conf);
      final DistCpContext context = new DistCpContext(options);

      CopyListing listing = new GlobbedCopyListing(conf, CREDENTIALS);
      Path listingFile = new Path("/tmp1/" + String.valueOf(rand.nextLong()));
      listing.buildListing(listingFile, context);

      conf.set(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH, targetBase);
      conf.set(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH, targetBase);

      committer.commitJob(jobContext);
      verifyFoldersAreInSync(fs, targetBase, sourceBase);
      Assert.assertEquals(4, fs.listStatus(new Path(targetBase)).length);

      //Test for idempotent commit
      committer.commitJob(jobContext);
      verifyFoldersAreInSync(fs, targetBase, sourceBase);
      Assert.assertEquals(4, fs.listStatus(new Path(targetBase)).length);
    } finally {
      TestDistCpUtils.delete(fs, "/tmp1");
      conf.set(DistCpConstants.CONF_LABEL_DELETE_MISSING, "false");
    }

  }

  @Test
  public void testAtomicCommitMissingFinal() throws IOException {
    TaskAttemptContext taskAttemptContext = getTaskAttemptContext(config);
    JobContext jobContext = new JobContextImpl(taskAttemptContext.getConfiguration(),
        taskAttemptContext.getTaskAttemptID().getJobID());
    Configuration conf = jobContext.getConfiguration();

    String workPath = "/tmp1/" + String.valueOf(rand.nextLong());
    String finalPath = "/tmp1/" + String.valueOf(rand.nextLong());
    FileSystem fs = null;
    try {
      OutputCommitter committer = new CopyCommitter(null, taskAttemptContext);
      fs = FileSystem.get(conf);
      fs.mkdirs(new Path(workPath));

      conf.set(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH, workPath);
      conf.set(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH, finalPath);
      conf.setBoolean(DistCpConstants.CONF_LABEL_ATOMIC_COPY, true);

      assertPathExists(fs, "Work path", new Path(workPath));
      assertPathDoesNotExist(fs, "Final path", new Path(finalPath));
      committer.commitJob(jobContext);
      assertPathDoesNotExist(fs, "Work path", new Path(workPath));
      assertPathExists(fs, "Final path", new Path(finalPath));

      //Test for idempotent commit
      committer.commitJob(jobContext);
      assertPathDoesNotExist(fs, "Work path", new Path(workPath));
      assertPathExists(fs, "Final path", new Path(finalPath));
    } finally {
      TestDistCpUtils.delete(fs, workPath);
      TestDistCpUtils.delete(fs, finalPath);
      conf.setBoolean(DistCpConstants.CONF_LABEL_ATOMIC_COPY, false);
    }
  }

  @Test
  public void testAtomicCommitExistingFinal() throws IOException {
    TaskAttemptContext taskAttemptContext = getTaskAttemptContext(config);
    JobContext jobContext = new JobContextImpl(taskAttemptContext.getConfiguration(),
        taskAttemptContext.getTaskAttemptID().getJobID());
    Configuration conf = jobContext.getConfiguration();


    String workPath = "/tmp1/" + String.valueOf(rand.nextLong());
    String finalPath = "/tmp1/" + String.valueOf(rand.nextLong());
    FileSystem fs = null;
    try {
      OutputCommitter committer = new CopyCommitter(null, taskAttemptContext);
      fs = FileSystem.get(conf);
      fs.mkdirs(new Path(workPath));
      fs.mkdirs(new Path(finalPath));

      conf.set(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH, workPath);
      conf.set(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH, finalPath);
      conf.setBoolean(DistCpConstants.CONF_LABEL_ATOMIC_COPY, true);

      assertPathExists(fs, "Work path", new Path(workPath));
      assertPathExists(fs, "Final path", new Path(finalPath));
      try {
        committer.commitJob(jobContext);
        Assert.fail("Should not be able to atomic-commit to pre-existing path.");
      } catch(Exception exception) {
        assertPathExists(fs, "Work path", new Path(workPath));
        assertPathExists(fs, "Final path", new Path(finalPath));
        LOG.info("Atomic-commit Test pass.");
      }

    } finally {
      TestDistCpUtils.delete(fs, workPath);
      TestDistCpUtils.delete(fs, finalPath);
      conf.setBoolean(DistCpConstants.CONF_LABEL_ATOMIC_COPY, false);
    }
  }

  private TaskAttemptContext getTaskAttemptContext(Configuration conf) {
    return new TaskAttemptContextImpl(conf,
        new TaskAttemptID("200707121733", 1, TaskType.MAP, 1, 1));
  }

  private void checkDirectoryPermissions(FileSystem fs, String targetBase,
      FsPermission sourcePerm) throws IOException {
    Path base = new Path(targetBase);

    Stack<Path> stack = new Stack<>();
    stack.push(base);
    while (!stack.isEmpty()) {
      Path file = stack.pop();
      if (!fs.exists(file)) continue;
      FileStatus[] fStatus = fs.listStatus(file);
      if (fStatus == null || fStatus.length == 0) continue;

      for (FileStatus status : fStatus) {
        if (status.isDirectory()) {
          stack.push(status.getPath());
          Assert.assertEquals(sourcePerm, status.getPermission());
        }
      }
    }
  }

  private static class NullInputFormat extends InputFormat {
    @Override
    public List getSplits(JobContext context)
        throws IOException, InterruptedException {
      return Collections.emptyList();
    }

    @Override
    public RecordReader createRecordReader(InputSplit split,
                                           TaskAttemptContext context)
        throws IOException, InterruptedException {
      return null;
    }
  }
}
