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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.event.Level;


/**
 * This class tests the cases of a concurrent reads/writes to a file;
 * ie, one writer and one or more readers can see unfinsihed blocks
 */
public class TestFileConcurrentReader {

  private enum SyncType {
    SYNC,
    APPEND,
  }


  private static final Logger LOG =
    Logger.getLogger(TestFileConcurrentReader.class);

  {
    GenericTestUtils.setLogLevel(LeaseManager.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(FSNamesystem.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(DFSClient.LOG, Level.TRACE);
  }

  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  private static final int DEFAULT_WRITE_SIZE = 1024 + 1;
  private static final int SMALL_WRITE_SIZE = 61;
  private Configuration conf;
  private MiniDFSCluster cluster;
  private FileSystem fileSystem;


  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    init(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private void init(Configuration conf) throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitClusterUp();
    fileSystem = cluster.getFileSystem();
  }

  private void writeFileAndSync(FSDataOutputStream stm, int size)
    throws IOException {
    byte[] buffer = DFSTestUtil.generateSequentialBytes(0, size);
    stm.write(buffer, 0, size);
    stm.hflush();
  }

  private void checkCanRead(FileSystem fileSys, Path path, int numBytes)
    throws IOException {
    waitForBlocks(fileSys, path);
    assertBytesAvailable(fileSys, path, numBytes);
  }

  // make sure bytes are available and match expected
  private void assertBytesAvailable(
    FileSystem fileSystem,
    Path path,
    int numBytes
  ) throws IOException {
    byte[] buffer = new byte[numBytes];
    FSDataInputStream inputStream = fileSystem.open(path);
    IOUtils.readFully(inputStream, buffer, 0, numBytes);
    inputStream.close();

    assertTrue(
      "unable to validate bytes",
      validateSequentialBytes(buffer, 0, numBytes)
    );
  }

  private void waitForBlocks(FileSystem fileSys, Path name)
    throws IOException {
    // wait until we have at least one block in the file to read.
    boolean done = false;

    while (!done) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
      done = true;
      BlockLocation[] locations = fileSys.getFileBlockLocations(
        fileSys.getFileStatus(name), 0, blockSize);
      if (locations.length < 1) {
        done = false;
        continue;
      }
    }
  }

  /**
   * Test that that writes to an incomplete block are available to a reader
   */
  @Test (timeout = 30000)
  public void testUnfinishedBlockRead()
    throws IOException {
    // create a new file in the root, write data, do no close
    Path file1 = new Path("/unfinished-block");
    FSDataOutputStream stm = TestFileCreation.createFile(fileSystem, file1, 1);

    // write partial block and sync
    int partialBlockSize = blockSize / 2;
    writeFileAndSync(stm, partialBlockSize);

    // Make sure a client can read it before it is closed
    checkCanRead(fileSystem, file1, partialBlockSize);

    stm.close();
  }

  /**
   * test case: if the BlockSender decides there is only one packet to send,
   * the previous computation of the pktSize based on transferToAllowed
   * would result in too small a buffer to do the buffer-copy needed
   * for partial chunks.
   */
  @Test (timeout = 30000)
  public void testUnfinishedBlockPacketBufferOverrun() throws IOException {
    // check that / exists
    Path path = new Path("/");
    System.out.println("Path : \"" + path.toString() + "\"");

    // create a new file in the root, write data, do no close
    Path file1 = new Path("/unfinished-block");
    final FSDataOutputStream stm =
      TestFileCreation.createFile(fileSystem, file1, 1);

    // write partial block and sync
    final int bytesPerChecksum = conf.getInt("io.bytes.per.checksum", 512);
    final int partialBlockSize = bytesPerChecksum - 1;

    writeFileAndSync(stm, partialBlockSize);

    // Make sure a client can read it before it is closed
    checkCanRead(fileSystem, file1, partialBlockSize);

    stm.close();
  }

  // use a small block size and a large write so that DN is busy creating
  // new blocks.  This makes it almost 100% sure we can reproduce
  // case of client getting a DN that hasn't yet created the blocks
  @Test (timeout = 30000)
  public void testImmediateReadOfNewFile()
    throws IOException {
    final int blockSize = 64 * 1024;
    final int writeSize = 10 * blockSize;
    Configuration conf = new Configuration();
    
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    init(conf);

    final int requiredSuccessfulOpens = 100;
    final Path file = new Path("/file1");
    final AtomicBoolean openerDone = new AtomicBoolean(false);
    final AtomicReference<String> errorMessage = new AtomicReference<String>();
    final FSDataOutputStream out = fileSystem.create(file);
    
    final Thread writer = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          while (!openerDone.get()) {
            out.write(DFSTestUtil.generateSequentialBytes(0, writeSize));
            out.hflush();
          }
        } catch (IOException e) {
          LOG.warn("error in writer", e);
        } finally {
          try {
            out.close();
          } catch (IOException e) {
            LOG.error("unable to close file");
          }
        }
      }
    });
    
    Thread opener = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          for (int i = 0; i < requiredSuccessfulOpens; i++) {
            fileSystem.open(file).close();
          }
          openerDone.set(true);
        } catch (IOException e) {
          openerDone.set(true);
          errorMessage.set(String.format(
            "got exception : %s",
            StringUtils.stringifyException(e)
          ));
        } catch (Exception e) {
          openerDone.set(true);
          errorMessage.set(String.format(
            "got exception : %s", 
            StringUtils.stringifyException(e)
          ));
          writer.interrupt();
          fail("here");
        }
      }
    });
    
    writer.start();
    opener.start();

    try {
      writer.join();
      opener.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    
    assertNull(errorMessage.get(), errorMessage.get());
  }

  // for some reason, using tranferTo evokes the race condition more often
  // so test separately
  @Test (timeout = 30000)
  public void testUnfinishedBlockCRCErrorTransferTo() throws IOException {
    runTestUnfinishedBlockCRCError(true, SyncType.SYNC, DEFAULT_WRITE_SIZE);
  }

  @Test (timeout = 30000)
  public void testUnfinishedBlockCRCErrorTransferToVerySmallWrite()
    throws IOException {
    runTestUnfinishedBlockCRCError(true, SyncType.SYNC, SMALL_WRITE_SIZE);
  }

  // fails due to issue w/append, disable 
  @Ignore
  public void _testUnfinishedBlockCRCErrorTransferToAppend()
    throws IOException {
    runTestUnfinishedBlockCRCError(true, SyncType.APPEND, DEFAULT_WRITE_SIZE);
  }

  @Test (timeout = 30000)
  public void testUnfinishedBlockCRCErrorNormalTransfer() throws IOException {
    runTestUnfinishedBlockCRCError(false, SyncType.SYNC, DEFAULT_WRITE_SIZE);
  }

  @Test (timeout = 30000)
  public void testUnfinishedBlockCRCErrorNormalTransferVerySmallWrite()
    throws IOException {
    runTestUnfinishedBlockCRCError(false, SyncType.SYNC, SMALL_WRITE_SIZE);
  }

  // fails due to issue w/append, disable 
  @Ignore
  public void _testUnfinishedBlockCRCErrorNormalTransferAppend()
    throws IOException {
    runTestUnfinishedBlockCRCError(false, SyncType.APPEND, DEFAULT_WRITE_SIZE);
  }

  private void runTestUnfinishedBlockCRCError(
    final boolean transferToAllowed, SyncType syncType, int writeSize
  ) throws IOException {
    runTestUnfinishedBlockCRCError(
      transferToAllowed, syncType, writeSize, new Configuration()
    );
  }

  private void runTestUnfinishedBlockCRCError(
    final boolean transferToAllowed,
    final SyncType syncType,
    final int writeSize,
    Configuration conf
  ) throws IOException {
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_TRANSFERTO_ALLOWED_KEY,
        transferToAllowed);
    init(conf);

    final Path file = new Path("/block-being-written-to");
    final int numWrites = 2000;
    final AtomicBoolean writerDone = new AtomicBoolean(false);
    final AtomicBoolean writerStarted = new AtomicBoolean(false);
    final AtomicBoolean error = new AtomicBoolean(false);

    final Thread writer = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          FSDataOutputStream outputStream = fileSystem.create(file);
          if (syncType == SyncType.APPEND) {
            outputStream.close();
            outputStream = fileSystem.append(file);
          }
          try {
            for (int i = 0; !error.get() && i < numWrites; i++) {
              final byte[] writeBuf =
                  DFSTestUtil.generateSequentialBytes(i * writeSize, writeSize);
              outputStream.write(writeBuf);
              if (syncType == SyncType.SYNC) {
                outputStream.hflush();
              }
              writerStarted.set(true);
            }
          } catch (IOException e) {
            error.set(true);
            LOG.error("error writing to file", e);
          } finally {
            outputStream.close();
          }
          writerDone.set(true);
        } catch (Exception e) {
          LOG.error("error in writer", e);

          throw new RuntimeException(e);
        }
      }
    });
    Thread tailer = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          long startPos = 0;
          while (!writerDone.get() && !error.get()) {
            if (writerStarted.get()) {
              try {
                startPos = tailFile(file, startPos);
              } catch (IOException e) {
                LOG.error(String.format("error tailing file %s", file), e);

                throw new RuntimeException(e);
              }
            }
          }
        } catch (RuntimeException e) {
          if (e.getCause() instanceof ChecksumException) {
            error.set(true);
          }

          writer.interrupt();
          LOG.error("error in tailer", e);
          throw e;
        }
      }
    });

    writer.start();
    tailer.start();

    try {
      writer.join();
      tailer.join();

      assertFalse(
        "error occurred, see log above", error.get()
      );
    } catch (InterruptedException e) {
      LOG.info("interrupted waiting for writer or tailer to complete");

      Thread.currentThread().interrupt();
    }
  }

  private boolean validateSequentialBytes(byte[] buf, int startPos, int len) {
    for (int i = 0; i < len; i++) {
      int expected = (i + startPos) % 127;

      if (buf[i] % 127 != expected) {
        LOG.error(String.format("at position [%d], got [%d] and expected [%d]",
          startPos, buf[i], expected));

        return false;
      }
    }

    return true;
  }

  private long tailFile(Path file, long startPos) throws IOException {
    long numRead = 0;
    FSDataInputStream inputStream = fileSystem.open(file);
    inputStream.seek(startPos);

    int len = 4 * 1024;
    byte[] buf = new byte[len];
    int read;
    while ((read = inputStream.read(buf)) > -1) {
      LOG.info(String.format("read %d bytes", read));

      if (!validateSequentialBytes(buf, (int) (startPos + numRead), read)) {
        LOG.error(String.format("invalid bytes: [%s]\n", Arrays.toString(buf)));
        throw new ChecksumException(
          String.format("unable to validate bytes"),
          startPos
        );
      }

      numRead += read;
    }

    inputStream.close();
    return numRead + startPos - 1;
  }
}
