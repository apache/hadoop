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
package org.apache.hadoop.hdfs;

import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.log4j.Level;

/**
 * This class tests hflushing concurrently from many threads.
 */
public class TestMultiThreadedHflush {
  static final int blockSize = 1024*1024;
  static final int numBlocks = 10;
  static final int fileSize = numBlocks * blockSize + 1;

  private static final int NUM_THREADS = 10;
  private static final int WRITE_SIZE = 517;
  private static final int NUM_WRITES_PER_THREAD = 1000;
  
  private byte[] toWrite = null;

  {
    ((Log4JLogger)NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LogFactory.getLog(FSNamesystem.class)).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)InterDatanodeProtocol.LOG).getLogger().setLevel(Level.ALL);
  }

  /*
   * creates a file but does not close it
   */ 
  private FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true, fileSys.getConf()
        .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
        (short) repl, blockSize);
    return stm;
  }
  
  private void initBuffer(int size) {
    long seed = AppendTestUtil.nextLong();
    toWrite = AppendTestUtil.randomBytes(seed, size);
  }

  private class WriterThread extends Thread {
    private final FSDataOutputStream stm;
    private final AtomicReference<Throwable> thrown;
    private final int numWrites;
    private final CountDownLatch countdown;

    public WriterThread(FSDataOutputStream stm,
      AtomicReference<Throwable> thrown,
      CountDownLatch countdown, int numWrites) {
      this.stm = stm;
      this.thrown = thrown;
      this.numWrites = numWrites;
      this.countdown = countdown;
    }

    public void run() {
      try {
        countdown.await();
        for (int i = 0; i < numWrites && thrown.get() == null; i++) {
          doAWrite();
        }
      } catch (Throwable t) {
        thrown.compareAndSet(null, t);
      }
    }

    private void doAWrite() throws IOException {
      stm.write(toWrite);
      stm.hflush();
    }
  }


  /**
   * Test case where a bunch of threads are both appending and flushing.
   * They all finish before the file is closed.
   */
  @Test
  public void testMultipleHflushers() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();

    FileSystem fs = cluster.getFileSystem();
    Path p = new Path("/multiple-hflushers.dat");
    try {
      doMultithreadedWrites(conf, p, NUM_THREADS, WRITE_SIZE, NUM_WRITES_PER_THREAD);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  /**
   * Test case where a bunch of threads are continuously calling hflush() while another
   * thread appends some data and then closes the file.
   *
   * The hflushing threads should eventually catch an IOException stating that the stream
   * was closed -- and not an NPE or anything like that.
   */
  @Test
  public void testHflushWhileClosing() throws Throwable {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    FileSystem fs = cluster.getFileSystem();
    Path p = new Path("/hflush-and-close.dat");

    final FSDataOutputStream stm = createFile(fs, p, 1);


    ArrayList<Thread> flushers = new ArrayList<Thread>();
    final AtomicReference<Throwable> thrown = new AtomicReference<Throwable>();
    try {
      for (int i = 0; i < 10; i++) {
        Thread flusher = new Thread() {
            public void run() {
              try {
                while (true) {
                  try {
                    stm.hflush();
                  } catch (IOException ioe) {
                    if (!ioe.toString().contains("DFSOutputStream is closed")) {
                      throw ioe;
                    } else {
                      return;
                    }
                  }
                }
              } catch (Throwable t) {
                thrown.set(t);
              }
            }
          };
        flusher.start();
        flushers.add(flusher);
      }

      // Write some data
      for (int i = 0; i < 10000; i++) {
        stm.write(1);
      }

      // Close it while the flushing threads are still flushing
      stm.close();

      // Wait for the flushers to all die.
      for (Thread t : flushers) {
        t.join();
      }

      // They should have all gotten the expected exception, not anything
      // else.
      if (thrown.get() != null) {
        throw thrown.get();
      }

    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  public void doMultithreadedWrites(
    Configuration conf, Path p, int numThreads, int bufferSize, int numWrites)
    throws Exception {
    initBuffer(bufferSize);

    // create a new file.
    FileSystem fs = p.getFileSystem(conf);
    FSDataOutputStream stm = createFile(fs, p, 1);
    System.out.println("Created file simpleFlush.dat");

    // There have been a couple issues with flushing empty buffers, so do
    // some empty flushes first.
    stm.hflush();
    stm.hflush();
    stm.write(1);
    stm.hflush();
    stm.hflush();

    CountDownLatch countdown = new CountDownLatch(1);
    ArrayList<Thread> threads = new ArrayList<Thread>();
    AtomicReference<Throwable> thrown = new AtomicReference<Throwable>();
    for (int i = 0; i < numThreads; i++) {
      Thread t = new WriterThread(stm, thrown, countdown, numWrites);
      threads.add(t);
      t.start();
    }

    // Start all the threads at the same time for maximum raciness!
    countdown.countDown();

    for (Thread t : threads) {
      t.join();
    }
    if (thrown.get() != null) {
      throw new RuntimeException("Deferred", thrown.get());
    }
    stm.close();
    System.out.println("Closed file.");
  }

  public static void main(String args[]) throws Exception {
    if (args.length != 1) {
      System.err.println(
        "usage: " + TestMultiThreadedHflush.class.getSimpleName() +
        " <path to test file> ");
      System.exit(1);
    }
    TestMultiThreadedHflush test = new TestMultiThreadedHflush();
    Configuration conf = new Configuration();
    Path p = new Path(args[0]);
    long st = System.nanoTime();
    test.doMultithreadedWrites(conf, p, 10, 511, 50000);
    long et = System.nanoTime();

    System.out.println("Finished in " + ((et - st) / 1000000) + "ms");
  }

}
