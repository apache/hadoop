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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

public class TestCopyCommitter {
  private static final Log LOG = LogFactory.getLog(TestCopyCommitter.class);

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
  public void createMetaFolder() {
    config.set(DistCpConstants.CONF_LABEL_META_FOLDER, "/meta");
    // Unset listing file path since the config is shared by
    // multiple tests, and some test doesn't set it, such as
    // testNoCommitAction, but the distcp code will check it.
    config.set(DistCpConstants.CONF_LABEL_LISTING_FILE_PATH, "");
    Path meta = new Path("/meta");
    try {
      cluster.getFileSystem().mkdirs(meta);
    } catch (IOException e) {
      LOG.error("Exception encountered while creating meta folder", e);
      Assert.fail("Unable to create meta folder");
    }
  }

  @After
  public void cleanupMetaFolder() {
    Path meta = new Path("/meta");
    try {
      if (cluster.getFileSystem().exists(meta)) {
        cluster.getFileSystem().delete(meta, true);
        Assert.fail("Expected meta folder to be deleted");
      }
    } catch (IOException e) {
      LOG.error("Exception encountered while cleaning up folder", e);
      Assert.fail("Unable to clean up meta folder");
    }
  }

  @Test
  public void testNoCommitAction() {
    TaskAttemptContext taskAttemptContext = getTaskAttemptContext(config);
    JobContext jobContext = new JobContextImpl(taskAttemptContext.getConfiguration(),
        taskAttemptContext.getTaskAttemptID().getJobID());
    try {
      OutputCommitter committer = new CopyCommitter(null, taskAttemptContext);
      committer.commitJob(jobContext);
      Assert.assertEquals(taskAttemptContext.getStatus(), "Commit Successful");

      //Test for idempotent commit
      committer.commitJob(jobContext);
      Assert.assertEquals(taskAttemptContext.getStatus(), "Commit Successful");
    } catch (IOException e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("Commit failed");
    }
  }

  @Test
  public void testPreserveStatus() {
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
      if (!checkDirectoryPermissions(fs, targetBase, sourcePerm)) {
        Assert.fail("Permission don't match");
      }

      //Test for idempotent commit
      committer.commitJob(jobContext);
      if (!checkDirectoryPermissions(fs, targetBase, sourcePerm)) {
        Assert.fail("Permission don't match");
      }

    } catch (IOException e) {
      LOG.error("Exception encountered while testing for preserve status", e);
      Assert.fail("Preserve status failure");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp1");
      conf.unset(DistCpConstants.CONF_LABEL_PRESERVE_STATUS);
    }

  }

  @Test
  public void testDeleteMissing() {
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
      if (!TestDistCpUtils.checkIfFoldersAreInSync(fs, targetBase, sourceBase)) {
        Assert.fail("Source and target folders are not in sync");
      }
      if (!TestDistCpUtils.checkIfFoldersAreInSync(fs, sourceBase, targetBase)) {
        Assert.fail("Source and target folders are not in sync");
      }

      //Test for idempotent commit
      committer.commitJob(jobContext);
      if (!TestDistCpUtils.checkIfFoldersAreInSync(fs, targetBase, sourceBase)) {
        Assert.fail("Source and target folders are not in sync");
      }
      if (!TestDistCpUtils.checkIfFoldersAreInSync(fs, sourceBase, targetBase)) {
        Assert.fail("Source and target folders are not in sync");
      }
    } catch (Throwable e) {
      LOG.error("Exception encountered while testing for delete missing", e);
      Assert.fail("Delete missing failure");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp1");
      conf.set(DistCpConstants.CONF_LABEL_DELETE_MISSING, "false");
    }
  }

  @Test
  public void testDeleteMissingFlatInterleavedFiles() {
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
      TestDistCpUtils.createFile(fs, sourceBase + "/1");
      TestDistCpUtils.createFile(fs, sourceBase + "/3");
      TestDistCpUtils.createFile(fs, sourceBase + "/4");
      TestDistCpUtils.createFile(fs, sourceBase + "/5");
      TestDistCpUtils.createFile(fs, sourceBase + "/7");
      TestDistCpUtils.createFile(fs, sourceBase + "/8");
      TestDistCpUtils.createFile(fs, sourceBase + "/9");

      TestDistCpUtils.createFile(fs, targetBase + "/2");
      TestDistCpUtils.createFile(fs, targetBase + "/4");
      TestDistCpUtils.createFile(fs, targetBase + "/5");
      TestDistCpUtils.createFile(fs, targetBase + "/7");
      TestDistCpUtils.createFile(fs, targetBase + "/9");
      TestDistCpUtils.createFile(fs, targetBase + "/A");

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
      if (!TestDistCpUtils.checkIfFoldersAreInSync(fs, targetBase, sourceBase)) {
        Assert.fail("Source and target folders are not in sync");
      }
      Assert.assertEquals(fs.listStatus(new Path(targetBase)).length, 4);

      //Test for idempotent commit
      committer.commitJob(jobContext);
      if (!TestDistCpUtils.checkIfFoldersAreInSync(fs, targetBase, sourceBase)) {
        Assert.fail("Source and target folders are not in sync");
      }
      Assert.assertEquals(fs.listStatus(new Path(targetBase)).length, 4);
    } catch (IOException e) {
      LOG.error("Exception encountered while testing for delete missing", e);
      Assert.fail("Delete missing failure");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp1");
      conf.set(DistCpConstants.CONF_LABEL_DELETE_MISSING, "false");
    }

  }

  @Test
  public void testAtomicCommitMissingFinal() {
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

      Assert.assertTrue(fs.exists(new Path(workPath)));
      Assert.assertFalse(fs.exists(new Path(finalPath)));
      committer.commitJob(jobContext);
      Assert.assertFalse(fs.exists(new Path(workPath)));
      Assert.assertTrue(fs.exists(new Path(finalPath)));

      //Test for idempotent commit
      committer.commitJob(jobContext);
      Assert.assertFalse(fs.exists(new Path(workPath)));
      Assert.assertTrue(fs.exists(new Path(finalPath)));
    } catch (IOException e) {
      LOG.error("Exception encountered while testing for preserve status", e);
      Assert.fail("Atomic commit failure");
    } finally {
      TestDistCpUtils.delete(fs, workPath);
      TestDistCpUtils.delete(fs, finalPath);
      conf.setBoolean(DistCpConstants.CONF_LABEL_ATOMIC_COPY, false);
    }
  }

  @Test
  public void testAtomicCommitExistingFinal() {
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

      Assert.assertTrue(fs.exists(new Path(workPath)));
      Assert.assertTrue(fs.exists(new Path(finalPath)));
      try {
        committer.commitJob(jobContext);
        Assert.fail("Should not be able to atomic-commit to pre-existing path.");
      } catch(Exception exception) {
        Assert.assertTrue(fs.exists(new Path(workPath)));
        Assert.assertTrue(fs.exists(new Path(finalPath)));
        LOG.info("Atomic-commit Test pass.");
      }

    } catch (IOException e) {
      LOG.error("Exception encountered while testing for atomic commit.", e);
      Assert.fail("Atomic commit failure");
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

  private boolean checkDirectoryPermissions(FileSystem fs, String targetBase,
                                            FsPermission sourcePerm) throws IOException {
    Path base = new Path(targetBase);

    Stack<Path> stack = new Stack<Path>();
    stack.push(base);
    while (!stack.isEmpty()) {
      Path file = stack.pop();
      if (!fs.exists(file)) continue;
      FileStatus[] fStatus = fs.listStatus(file);
      if (fStatus == null || fStatus.length == 0) continue;

      for (FileStatus status : fStatus) {
        if (status.isDirectory()) {
          stack.push(status.getPath());
          Assert.assertEquals(status.getPermission(), sourcePerm);
        }
      }
    }
    return true;
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
