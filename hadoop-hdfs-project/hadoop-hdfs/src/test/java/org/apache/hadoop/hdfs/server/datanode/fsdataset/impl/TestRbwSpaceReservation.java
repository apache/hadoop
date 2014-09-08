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

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.fs.DU;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Random;

/**
 * Ensure that the DN reserves disk space equivalent to a full block for
 * replica being written (RBW).
 */
public class TestRbwSpaceReservation {
  static final Log LOG = LogFactory.getLog(TestRbwSpaceReservation.class);

  private static final short REPL_FACTOR = 1;
  private static final int DU_REFRESH_INTERVAL_MSEC = 500;
  private static final int STORAGES_PER_DATANODE = 1;
  private static final int BLOCK_SIZE = 1024 * 1024;
  private static final int SMALL_BLOCK_SIZE = 1024;

  protected MiniDFSCluster cluster;
  private Configuration conf;
  private DistributedFileSystem fs = null;
  private DFSClient client = null;
  FsVolumeImpl singletonVolume = null;

  private static Random rand = new Random();

  private void initConfig(int blockSize) {
    conf = new HdfsConfiguration();

    // Refresh disk usage information frequently.
    conf.setInt(FS_DU_INTERVAL_KEY, DU_REFRESH_INTERVAL_MSEC);
    conf.setLong(DFS_BLOCK_SIZE_KEY, blockSize);

    // Disable the scanner
    conf.setInt(DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, -1);
  }

  static {
    ((Log4JLogger) FsDatasetImpl.LOG).getLogger().setLevel(Level.ALL);
  }

  private void startCluster(int blockSize, long perVolumeCapacity) throws IOException {
    initConfig(blockSize);

    cluster = new MiniDFSCluster
        .Builder(conf)
        .storagesPerDatanode(STORAGES_PER_DATANODE)
        .numDataNodes(REPL_FACTOR)
        .build();
    fs = cluster.getFileSystem();
    client = fs.getClient();
    cluster.waitActive();

    if (perVolumeCapacity >= 0) {
      List<? extends FsVolumeSpi> volumes =
          cluster.getDataNodes().get(0).getFSDataset().getVolumes();

      assertThat(volumes.size(), is(1));
      singletonVolume = ((FsVolumeImpl) volumes.get(0));
      singletonVolume.setCapacityForTesting(perVolumeCapacity);
    }
  }

  @After
  public void shutdownCluster() throws IOException {
    if (client != null) {
      client.close();
      client = null;
    }

    if (fs != null) {
      fs.close();
      fs = null;
    }

    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private void createFileAndTestSpaceReservation(
      final String fileNamePrefix, final int fileBlockSize)
      throws IOException, InterruptedException {
    // Enough for 1 block + meta files + some delta.
    final long configuredCapacity = fileBlockSize * 2 - 1;
    startCluster(BLOCK_SIZE, configuredCapacity);
    FSDataOutputStream out = null;
    Path path = new Path("/" + fileNamePrefix + ".dat");

    try {
      out = fs.create(path, false, 4096, (short) 1, fileBlockSize);

      byte[] buffer = new byte[rand.nextInt(fileBlockSize / 4)];
      out.write(buffer);
      out.hsync();
      int bytesWritten = buffer.length;

      // Check that space was reserved for a full block minus the bytesWritten.
      assertThat(singletonVolume.getReservedForRbw(),
                 is((long) fileBlockSize - bytesWritten));
      out.close();
      out = null;

      // Check that the reserved space has been released since we closed the
      // file.
      assertThat(singletonVolume.getReservedForRbw(), is(0L));

      // Reopen the file for appends and write 1 more byte.
      out = fs.append(path);
      out.write(buffer);
      out.hsync();
      bytesWritten += buffer.length;

      // Check that space was again reserved for a full block minus the
      // bytesWritten so far.
      assertThat(singletonVolume.getReservedForRbw(),
                 is((long) fileBlockSize - bytesWritten));

      // Write once again and again verify the available space. This ensures
      // that the reserved space is progressively adjusted to account for bytes
      // written to disk.
      out.write(buffer);
      out.hsync();
      bytesWritten += buffer.length;
      assertThat(singletonVolume.getReservedForRbw(),
                 is((long) fileBlockSize - bytesWritten));
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  @Test (timeout=300000)
  public void testWithDefaultBlockSize()
      throws IOException, InterruptedException {
    createFileAndTestSpaceReservation(GenericTestUtils.getMethodName(), BLOCK_SIZE);
  }

  @Test (timeout=300000)
  public void testWithNonDefaultBlockSize()
      throws IOException, InterruptedException {
    // Same test as previous one, but with a non-default block size.
    createFileAndTestSpaceReservation(GenericTestUtils.getMethodName(), BLOCK_SIZE * 2);
  }

  /**
   * Stress test to ensure we are not leaking reserved space.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout=600000)
  public void stressTest() throws IOException, InterruptedException {
    final int numWriters = 5;
    startCluster(SMALL_BLOCK_SIZE, SMALL_BLOCK_SIZE * numWriters * 10);
    Writer[] writers = new Writer[numWriters];

    // Start a few writers and let them run for a while.
    for (int i = 0; i < numWriters; ++i) {
      writers[i] = new Writer(client, SMALL_BLOCK_SIZE);
      writers[i].start();
    }

    Thread.sleep(60000);

    // Stop the writers.
    for (Writer w : writers) {
      w.stopWriter();
    }
    int filesCreated = 0;
    int numFailures = 0;
    for (Writer w : writers) {
      w.join();
      filesCreated += w.getFilesCreated();
      numFailures += w.getNumFailures();
    }

    LOG.info("Stress test created " + filesCreated +
             " files and hit " + numFailures + " failures");

    // Check no space was leaked.
    assertThat(singletonVolume.getReservedForRbw(), is(0L));
  }

  private static class Writer extends Daemon {
    private volatile boolean keepRunning;
    private final DFSClient localClient;
    private int filesCreated = 0;
    private int numFailures = 0;
    byte[] data;

    Writer(DFSClient client, int blockSize) throws IOException {
      localClient = client;
      keepRunning = true;
      filesCreated = 0;
      numFailures = 0;

      // At least some of the files should span a block boundary.
      data = new byte[blockSize * 2];
    }

    @Override
    public void run() {
      /**
       * Create a file, write up to 3 blocks of data and close the file.
       * Do this in a loop until we are told to stop.
       */
      while (keepRunning) {
        OutputStream os = null;
        try {
          String filename = "/file-" + rand.nextLong();
          os = localClient.create(filename, false);
          os.write(data, 0, rand.nextInt(data.length));
          IOUtils.closeQuietly(os);
          os = null;
          localClient.delete(filename, false);
          Thread.sleep(50);     // Sleep for a bit to avoid killing the system.
          ++filesCreated;
        } catch (IOException ioe) {
          // Just ignore the exception and keep going.
          ++numFailures;
        } catch (InterruptedException ie) {
          return;
        } finally {
          if (os != null) {
            IOUtils.closeQuietly(os);
          }
        }
      }
    }

    public void stopWriter() {
      keepRunning = false;
    }

    public int getFilesCreated() {
      return filesCreated;
    }

    public int getNumFailures() {
      return numFailures;
    }
  }
}
