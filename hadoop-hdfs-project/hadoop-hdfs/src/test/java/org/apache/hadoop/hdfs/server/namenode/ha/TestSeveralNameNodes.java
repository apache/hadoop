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
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.test.MultithreadedTestUtil.RepeatingTestThread;
import org.apache.hadoop.test.MultithreadedTestUtil.TestContext;
import org.junit.Test;

/**
 * Test that we can start several and run with namenodes on the same minicluster
 */
public class TestSeveralNameNodes {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestSeveralNameNodes.class);

  /** ms between failovers between NNs */
  private static final int TIME_BETWEEN_FAILOVERS = 1000;
  private static final int NUM_NAMENODES = 3;
  private static final int NUM_THREADS = 3;
  private static final int LIST_LENGTH = 50;
  /** ms for length of test */
  private static final long RUNTIME = 100000;

  @Test
  public void testCircularLinkedListWrites() throws Exception {
    HAStressTestHarness harness = new HAStressTestHarness();
    // setup the harness
    harness.setNumberOfNameNodes(NUM_NAMENODES);
    harness.addFailoverThread(TIME_BETWEEN_FAILOVERS);
    harness.conf.setInt(HdfsClientConfigKeys.Failover.SLEEPTIME_MAX_KEY, 1000);
    harness.conf.setInt(HdfsClientConfigKeys.Failover.MAX_ATTEMPTS_KEY, 128);

    final MiniDFSCluster cluster = harness.startCluster();
    try {
      cluster.waitActive();
      cluster.transitionToActive(0);

      // setup the a circular writer
      FileSystem fs = harness.getFailoverFs();
      TestContext context = harness.testCtx;
      List<CircularWriter> writers = new ArrayList<CircularWriter>();
      for (int i = 0; i < NUM_THREADS; i++) {
        Path p = new Path("/test-" + i);
        fs.mkdirs(p);
        CircularWriter writer = new CircularWriter(context, LIST_LENGTH, fs, p);
        writers.add(writer);
        context.addThread(writer);
      }
      harness.startThreads();

      // wait for all the writer threads to finish, or that we exceed the time
      long start = System.currentTimeMillis();
      while ((System.currentTimeMillis() - start) < RUNTIME &&
          writers.size() > 0) {
        for (int i = 0; i < writers.size(); i++) {
          CircularWriter writer = writers.get(i);
          // remove the writer from the ones to check
          if (writer.done.await(100, TimeUnit.MILLISECONDS)) {
            writers.remove(i--);
          }
        }
      }
      assertEquals(
          "Some writers didn't complete in expected runtime! Current writer state:"
              + writers, 0,
          writers.size());

      harness.stopThreads();
    } finally {
      System.err.println("===========================\n\n\n\n");
      harness.shutdown();
    }
  }

  private static class CircularWriter extends RepeatingTestThread {

    private final int maxLength;
    private final Path dir;
    private final FileSystem fs;
    private int currentListIndex = 0;
    private CountDownLatch done = new CountDownLatch(1);

    public CircularWriter(TestContext context, int listLength, FileSystem fs,
        Path parentDir) {
      super(context);
      this.fs = fs;
      this.maxLength = listLength;
      this.dir = parentDir;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder("Circular Writer:\n");
      builder.append("\t directory: " + dir + "\n");
      builder.append("\t target length: " + maxLength + "\n");
      // might be a little racy, but we just want a close count
      builder.append("\t current item: " + currentListIndex + "\n");
      builder.append("\t done: " + (done.getCount() == 0) + "\n");
      return builder.toString();
    }

    @Override
    public void doAnAction() throws Exception {
      if (currentListIndex == maxLength) {
        checkList();
        this.stopTestThread();
        done.countDown();
      } else {
        writeList();
      }
    }

    /**
     * Make sure we can traverse the entire linked list
     */
    private void checkList() throws IOException {
      for (int i = 0; i < maxLength; i++) {
        Path nextFile = getNextFile(i);
        if (!fs.exists(nextFile)) {
          throw new RuntimeException("Next file " + nextFile
              + " for list does not exist!");
        }
        // read the next file name
        FSDataInputStream in = fs.open(nextFile);
        nextFile = getNextFile(in.read());
        in.close();
      }

    }

    private void cleanup() throws IOException {
      if (!fs.delete(dir, true)) {
        throw new RuntimeException("Didn't correctly delete " + dir);
      }
      if (!fs.mkdirs(dir)) {
        throw new RuntimeException("Didn't correctly make directory " + dir);
      }
    }

    private void writeList() throws IOException {
      Path nextPath = getNextFile(currentListIndex++);
      LOG.info("Writing next file: " + nextPath);
      FSDataOutputStream file = fs.create(nextPath);
      file.write(currentListIndex);
      file.close();
    }

    private Path getNextFile(int i) {
      return new Path(dir, Integer.toString(i));
    }
  }
}
