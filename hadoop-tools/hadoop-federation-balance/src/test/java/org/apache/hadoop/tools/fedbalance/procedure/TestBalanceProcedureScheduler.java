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
package org.apache.hadoop.tools.fedbalance.procedure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.SCHEDULER_JOURNAL_URI;
import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.WORK_THREAD_NUM;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.mockito.ArgumentMatchers.any;

/**
 * Test BalanceProcedureScheduler.
 */
public class TestBalanceProcedureScheduler {

  private static MiniDFSCluster cluster;
  private static final Configuration CONF = new Configuration();
  private static DistributedFileSystem fs;
  private static final int DEFAULT_BLOCK_SIZE = 512;

  @BeforeClass
  public static void setup() throws IOException {
    CONF.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
        true);
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "hdfs:///");
    CONF.setBoolean(DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    CONF.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    CONF.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);
    CONF.setInt(WORK_THREAD_NUM, 1);

    cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(3).build();
    cluster.waitClusterUp();
    cluster.waitActive();

    fs = cluster.getFileSystem();
    String workPath =
        "hdfs://" + cluster.getNameNode().getHostAndPort() + "/procedure";
    CONF.set(SCHEDULER_JOURNAL_URI, workPath);
    fs.mkdirs(new Path(workPath));
  }

  @AfterClass
  public static void close() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Test the scheduler could be shutdown correctly.
   */
  @Test(timeout = 60000)
  public void testShutdownScheduler() throws Exception {
    BalanceProcedureScheduler scheduler = new BalanceProcedureScheduler(CONF);
    scheduler.init(true);
    // construct job
    BalanceJob.Builder builder = new BalanceJob.Builder<>();
    builder.nextProcedure(new WaitProcedure("wait", 1000, 5 * 1000));
    BalanceJob job = builder.build();

    scheduler.submit(job);
    Thread.sleep(1000); // wait job to be scheduled.
    scheduler.shutDownAndWait(30 * 1000);

    BalanceJournal journal =
        ReflectionUtils.newInstance(BalanceJournalInfoHDFS.class, CONF);
    journal.clear(job);
  }

  /**
   * Test a successful job.
   */
  @Test(timeout = 60000)
  public void testSuccessfulJob() throws Exception {
    BalanceProcedureScheduler scheduler = new BalanceProcedureScheduler(CONF);
    scheduler.init(true);
    try {
      // construct job
      List<RecordProcedure> procedures = new ArrayList<>();
      BalanceJob.Builder builder = new BalanceJob.Builder<RecordProcedure>();
      for (int i = 0; i < 5; i++) {
        RecordProcedure r = new RecordProcedure("record-" + i, 1000L);
        builder.nextProcedure(r);
        procedures.add(r);
      }
      BalanceJob<RecordProcedure> job = builder.build();

      scheduler.submit(job);
      scheduler.waitUntilDone(job);
      assertNull(job.getError());
      // verify finish list.
      assertEquals(5, RecordProcedure.getFinishList().size());
      for (int i = 0; i < RecordProcedure.getFinishList().size(); i++) {
        assertEquals(procedures.get(i), RecordProcedure.getFinishList().get(i));
      }
    } finally {
      scheduler.shutDownAndWait(2);
    }
  }

  /**
   * Test a job fails and the error can be got.
   */
  @Test(timeout = 60000)
  public void testFailedJob() throws Exception {
    BalanceProcedureScheduler scheduler = new BalanceProcedureScheduler(CONF);
    scheduler.init(true);
    try {
      // Mock bad procedure.
      BalanceProcedure badProcedure = Mockito.mock(BalanceProcedure.class);
      Mockito.doThrow(new IOException("Job failed exception."))
          .when(badProcedure).execute();
      Mockito.doReturn("bad-procedure").when(badProcedure).name();

      BalanceJob.Builder builder = new BalanceJob.Builder<>();
      builder.nextProcedure(badProcedure);
      BalanceJob job = builder.build();
      scheduler.submit(job);
      scheduler.waitUntilDone(job);
      GenericTestUtils
          .assertExceptionContains("Job failed exception", job.getError());
    } finally {
      scheduler.shutDownAndWait(2);
    }
  }

  /**
   * Test recover a job. After the job is recovered, the job should start from
   * the last unfinished procedure, which is the first procedure without
   * journal.
   */
  @Test(timeout = 60000)
  public void testGetJobAfterRecover() throws Exception {
    BalanceProcedureScheduler scheduler = new BalanceProcedureScheduler(CONF);
    scheduler.init(true);
    try {
      // Construct job.
      BalanceJob.Builder builder = new BalanceJob.Builder<>();
      String firstProcedure = "wait0";
      WaitProcedure[] procedures = new WaitProcedure[5];
      for (int i = 0; i < 5; i++) {
        WaitProcedure procedure = new WaitProcedure("wait" + i, 1000, 1000);
        builder.nextProcedure(procedure).removeAfterDone(false);
        procedures[i] = procedure;
      }
      BalanceJob job = builder.build();
      scheduler.submit(job);

      // Sleep a random time then shut down.
      long randomSleepTime = Math.abs(new Random().nextInt()) % 5 * 1000 + 1000;
      Thread.sleep(randomSleepTime);
      scheduler.shutDownAndWait(2);

      // Current procedure is the last unfinished procedure. It is also the
      // first procedure without journal.
      WaitProcedure recoverProcedure = (WaitProcedure) job.getCurProcedure();
      int recoverIndex = -1;
      for (int i = 0; i < procedures.length; i++) {
        if (procedures[i].name().equals(recoverProcedure.name())) {
          recoverIndex = i;
          break;
        }
      }

      // Restart scheduler and recover the job.
      scheduler = new BalanceProcedureScheduler(CONF);
      scheduler.init(true);
      scheduler.waitUntilDone(job);

      // The job should be done successfully and the recoverJob should be equal
      // to the original job.
      BalanceJob recoverJob = scheduler.findJob(job);
      assertNull(recoverJob.getError());
      assertNotSame(job, recoverJob);
      assertEquals(job, recoverJob);
      // Verify whether the recovered job starts from the recoverProcedure.
      Map<String, WaitProcedure> pTable = recoverJob.getProcedureTable();
      List<WaitProcedure> recoveredProcedures =
          procedureTableToList(pTable, firstProcedure);
      for (int i = 0; i < recoverIndex; i++) {
        // All procedures before recoverProcedure shouldn't be executed.
        assertFalse(recoveredProcedures.get(i).getExecuted());
      }
      for (int i = recoverIndex; i < procedures.length; i++) {
        // All procedures start from recoverProcedure should be executed.
        assertTrue(recoveredProcedures.get(i).getExecuted());
      }
    } finally {
      scheduler.shutDownAndWait(2);
    }
  }

  /**
   * Test RetryException is handled correctly.
   */
  @Test(timeout = 60000)
  public void testRetry() throws Exception {
    BalanceProcedureScheduler scheduler = new BalanceProcedureScheduler(CONF);
    scheduler.init(true);
    try {
      // construct job
      BalanceJob.Builder builder = new BalanceJob.Builder<>();
      RetryProcedure retryProcedure = new RetryProcedure("retry", 1000, 3);
      builder.nextProcedure(retryProcedure);
      BalanceJob job = builder.build();

      long start = Time.monotonicNow();
      scheduler.submit(job);
      scheduler.waitUntilDone(job);
      assertNull(job.getError());

      long duration = Time.monotonicNow() - start;
      assertEquals(true, duration > 1000 * 3);
      assertEquals(3, retryProcedure.getTotalRetry());
    } finally {
      scheduler.shutDownAndWait(2);
    }
  }

  /**
   * Test schedule an empty job.
   */
  @Test(timeout = 60000)
  public void testEmptyJob() throws Exception {
    BalanceProcedureScheduler scheduler = new BalanceProcedureScheduler(CONF);
    scheduler.init(true);
    try {
      BalanceJob job = new BalanceJob.Builder<>().build();
      scheduler.submit(job);
      scheduler.waitUntilDone(job);
    } finally {
      scheduler.shutDownAndWait(2);
    }
  }

  /**
   * Test serialization and deserialization of Job.
   */
  @Test(timeout = 60000)
  public void testJobSerializeAndDeserialize() throws Exception {
    BalanceJob.Builder builder = new BalanceJob.Builder<RecordProcedure>();
    for (int i = 0; i < 5; i++) {
      RecordProcedure r = new RecordProcedure("record-" + i, 1000L);
      builder.nextProcedure(r);
    }
    builder.nextProcedure(new RetryProcedure("retry", 1000, 3));
    BalanceJob<RecordProcedure> job = builder.build();
    job.setId(BalanceProcedureScheduler.allocateJobId());
    // Serialize.
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    job.write(new DataOutputStream(bao));
    bao.flush();
    ByteArrayInputStream bai = new ByteArrayInputStream(bao.toByteArray());
    // Deserialize.
    BalanceJob newJob = new BalanceJob.Builder<>().build();
    newJob.readFields(new DataInputStream(bai));
    assertEquals(job, newJob);
  }

  /**
   * Test scheduler crashes and recovers.
   */
  @Test(timeout = 180000)
  public void testSchedulerDownAndRecoverJob() throws Exception {
    BalanceProcedureScheduler scheduler = new BalanceProcedureScheduler(CONF);
    scheduler.init(true);
    Path parent = new Path("/testSchedulerDownAndRecoverJob");
    try {
      // construct job
      BalanceJob.Builder builder = new BalanceJob.Builder<>();
      MultiPhaseProcedure multiPhaseProcedure =
          new MultiPhaseProcedure("retry", 1000, 10, CONF, parent.toString());
      builder.nextProcedure(multiPhaseProcedure);
      BalanceJob job = builder.build();

      scheduler.submit(job);
      Thread.sleep(500); // wait procedure to be scheduled.
      scheduler.shutDownAndWait(2);

      assertFalse(job.isJobDone());
      int len = fs.listStatus(parent).length;
      assertTrue(len > 0 && len < 10);
      // restart scheduler, test recovering the job.
      scheduler = new BalanceProcedureScheduler(CONF);
      scheduler.init(true);
      scheduler.waitUntilDone(job);

      assertEquals(10, fs.listStatus(parent).length);
      for (int i = 0; i < 10; i++) {
        assertTrue(fs.exists(new Path(parent, "phase-" + i)));
      }

      BalanceJob recoverJob = scheduler.findJob(job);
      assertNull(recoverJob.getError());
      assertNotSame(job, recoverJob);
      assertEquals(job, recoverJob);
    } finally {
      if (fs.exists(parent)) {
        fs.delete(parent, true);
      }
      scheduler.shutDownAndWait(2);
    }
  }

  @Test(timeout = 60000)
  public void testRecoverJobFromJournal() throws Exception {
    BalanceJournal journal =
        ReflectionUtils.newInstance(BalanceJournalInfoHDFS.class, CONF);
    BalanceJob.Builder builder = new BalanceJob.Builder<RecordProcedure>();
    BalanceProcedure wait0 = new WaitProcedure("wait0", 1000, 5000);
    BalanceProcedure wait1 = new WaitProcedure("wait1", 1000, 1000);
    builder.nextProcedure(wait0).nextProcedure(wait1);

    BalanceJob job = builder.build();
    job.setId(BalanceProcedureScheduler.allocateJobId());
    job.setCurrentProcedure(wait1);
    job.setLastProcedure(null);
    journal.saveJob(job);

    long start = Time.monotonicNow();
    BalanceProcedureScheduler scheduler = new BalanceProcedureScheduler(CONF);
    scheduler.init(true);
    try {
      scheduler.waitUntilDone(job);
      long duration = Time.monotonicNow() - start;
      assertTrue(duration >= 1000 && duration < 5000);
    } finally {
      scheduler.shutDownAndWait(2);
    }
  }

  @Test(timeout = 60000)
  public void testClearJournalFail() throws Exception {
    BalanceProcedureScheduler scheduler = new BalanceProcedureScheduler(CONF);
    scheduler.init(true);

    BalanceJournal journal = Mockito.mock(BalanceJournal.class);
    AtomicInteger count = new AtomicInteger(0);
    Mockito.doAnswer(invocation -> {
      if (count.incrementAndGet() == 1) {
        throw new IOException("Mock clear failure");
      }
      return null;
    }).when(journal).clear(any(BalanceJob.class));
    scheduler.setJournal(journal);

    try {
      BalanceJob.Builder builder = new BalanceJob.Builder<>();
      builder.nextProcedure(new WaitProcedure("wait", 1000, 1000));
      BalanceJob job = builder.build();
      scheduler.submit(job);
      scheduler.waitUntilDone(job);
      assertEquals(2, count.get());
    } finally {
      scheduler.shutDownAndWait(2);
    }
  }

  /**
   * Test the job will be recovered if writing journal fails.
   */
  @Test(timeout = 60000)
  public void testJobRecoveryWhenWriteJournalFail() throws Exception {
    BalanceProcedureScheduler scheduler = new BalanceProcedureScheduler(CONF);
    scheduler.init(true);

    try {
      // construct job
      AtomicBoolean recoverFlag = new AtomicBoolean(true);
      BalanceJob.Builder builder = new BalanceJob.Builder<>();
      builder.nextProcedure(new WaitProcedure("wait", 1000, 1000))
          .nextProcedure(
              new UnrecoverableProcedure("shutdown", 1000, () -> {
                cluster.restartNameNode(false);
                return true;
              })).nextProcedure(
          new UnrecoverableProcedure("recoverFlag", 1000, () -> {
            recoverFlag.set(false);
            return true;
          })).nextProcedure(new WaitProcedure("wait", 1000, 1000));

      BalanceJob job = builder.build();
      scheduler.submit(job);
      scheduler.waitUntilDone(job);
      assertTrue(job.isJobDone());
      assertNull(job.getError());
      assertTrue(recoverFlag.get());
    } finally {
      scheduler.shutDownAndWait(2);
    }
  }

  /**
   * Transform the procedure map into an ordered list based on the relations
   * specified by the map.
   */
  <T extends BalanceProcedure> List<T> procedureTableToList(
      Map<String, T> pTable, String first) {
    List<T> procedures = new ArrayList<>();
    T cur = pTable.get(first);
    while (cur != null) {
      procedures.add(cur);
      cur = pTable.get(cur.nextProcedure());
    }
    return procedures;
  }
}