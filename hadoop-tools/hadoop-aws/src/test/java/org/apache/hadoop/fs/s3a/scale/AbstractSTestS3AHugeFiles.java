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

package org.apache.hadoop.fs.s3a.scale;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.util.Progressable;
import org.junit.Assume;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.bandwidth;
import static org.apache.hadoop.fs.contract.ContractTestUtils.toHuman;
import static org.apache.hadoop.fs.s3a.Constants.BLOCK_OUTPUT;
import static org.apache.hadoop.fs.s3a.Constants.BLOCK_OUTPUT_BUFFER;
import static org.apache.hadoop.fs.s3a.Constants.MIN_MULTIPART_THRESHOLD;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_MIN_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.s3a.Constants.SOCKET_RECV_BUFFER;
import static org.apache.hadoop.fs.s3a.Constants.SOCKET_SEND_BUFFER;
import static org.apache.hadoop.fs.s3a.Constants.USER_AGENT_PREFIX;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestPropertyInt;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestPropertyLong;

/**
 * Scale test which creates a huge file.
 *
 * <b>Important:</b> the order in which these tests execute is fixed to
 * alphabetical order. Test cases are numbered {@code test_123_} to impose
 * an ordering based on the numbers.
 *
 * Having this ordering allows the tests to assume that the huge file
 * exists. Even so: they should all have a {@link #assumeHugeFileExists()}
 * check at the start, in case an individual test is executed.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class AbstractSTestS3AHugeFiles extends S3AScaleTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractSTestS3AHugeFiles.class);
  public static final int DEFAULT_UPLOAD_BLOCKSIZE = 64 * _1KB;
  public static final int DEFAULT_PARTITION_SIZE_MB = 8;
  private Path scaleTestDir;
  private Path hugefile;
  private Path hugefileRenamed;

  private int uploadBlockSize = DEFAULT_UPLOAD_BLOCKSIZE;
  private int partitionSize;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    final Path testPath = getTestPath();
    scaleTestDir = new Path(testPath, "scale");
    hugefile = new Path(scaleTestDir, "hugefile");
    hugefileRenamed = new Path(scaleTestDir, "hugefileRenamed");
  }

  @Override
  public void tearDown() throws Exception {
    // do nothing. Specifically: do not delete the test dir
  }

  /**
   * Note that this can get called before test setup.
   * @return the configuration to use.
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    partitionSize = getTestPropertyInt(conf,
        KEY_HUGE_PARTITION_SIZE,
        DEFAULT_PARTITION_SIZE_MB) * _1MB;
    assertTrue("Partition size too small: " + partitionSize,
        partitionSize > MULTIPART_MIN_SIZE);
    conf.setLong(SOCKET_SEND_BUFFER, _1MB);
    conf.setLong(SOCKET_RECV_BUFFER, _1MB);
    conf.setLong(MIN_MULTIPART_THRESHOLD, partitionSize);
    conf.setInt(MULTIPART_SIZE, partitionSize);
    conf.set(USER_AGENT_PREFIX, "STestS3AHugeFileCreate");
    conf.setBoolean(BLOCK_OUTPUT, true);
    conf.set(BLOCK_OUTPUT_BUFFER, getBlockOutputBufferName());
    return conf;
  }

  /**
   * The name of the buffering mechanism to use.
   * @return a buffering mechanism
   */
  protected abstract String getBlockOutputBufferName();

  @Test
  public void test_010_CreateHugeFile() throws IOException {
    long mb = getTestPropertyLong(getConf(), KEY_HUGE_FILESIZE,
        DEFAULT_HUGE_FILESIZE);
    long filesize = _1MB * mb;

    describe("Creating file %s of size %d MB" +
            " with partition size %d buffered by %s",
        hugefile, mb, partitionSize, getBlockOutputBufferName());

    byte[] data = new byte[uploadBlockSize];
    for (int i = 0; i < uploadBlockSize; i++) {
      data[i] = (byte) (i % 256);
    }

    assertEquals("File size set in " + KEY_HUGE_FILESIZE + " = " + filesize
            + " is not a multiple of " + uploadBlockSize,
        0, filesize % uploadBlockSize);
    long blocks = filesize / uploadBlockSize;
    long blocksPerMB = _1MB / uploadBlockSize;

    // perform the upload.
    // there's lots of logging here, so that a tail -f on the output log
    // can give a view of what is happening.
    StorageStatistics storageStatistics = fs.getStorageStatistics();
    String putRequests = Statistic.OBJECT_PUT_REQUESTS.getSymbol();
    String putBytes = Statistic.OBJECT_PUT_BYTES.getSymbol();
    Statistic putRequestsActive = Statistic.OBJECT_PUT_REQUESTS_ACTIVE;
    Statistic putBytesPending = Statistic.OBJECT_PUT_BYTES_PENDING;

    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();

    long blocksPer10MB = blocksPerMB * 10;
    try (FSDataOutputStream out = fs.create(hugefile,
        true,
        uploadBlockSize,
        new ProgressCallback())) {

      for (long block = 1; block <= blocks; block++) {
        out.write(data);
        // every 10 MB, dump something
        if (block % blocksPer10MB == 0) {
          long written = block * uploadBlockSize;
          long percentage = written * 100 / filesize;
          LOG.info(String.format("[%02d%%] Written %.2f MB out of %.2f MB;" +
                  " PUT = %d bytes (%d pending) in %d operations (%d active)",
              percentage,
              1.0 * written / _1MB,
              1.0 * filesize / _1MB,
              storageStatistics.getLong(putBytes),
              gaugeValue(putBytesPending),
              storageStatistics.getLong(putRequests),
              gaugeValue(putRequestsActive)));
        }
      }
      // now close the file
      LOG.info("Closing file and completing write operation");
      ContractTestUtils.NanoTimer closeTimer
          = new ContractTestUtils.NanoTimer();
      out.close();
      closeTimer.end("time to close() output stream");
    }

    timer.end("time to write %d MB in blocks of %d", mb, uploadBlockSize);
    logFSState();
    if (mb > 0) {
      LOG.info("Time per MB to write = {} nS", toHuman(timer.duration() / mb));
    }
    bandwidth(timer, filesize);
    long putRequestCount = storageStatistics.getLong(putRequests);
    Long putByteCount = storageStatistics.getLong(putBytes);
    LOG.info("PUT {} bytes in {} operations; {} MB/operation",
        putByteCount, putRequestCount,
        putByteCount / (putRequestCount * _1MB));
    LOG.info("Time per PUT {} nS",
        toHuman(timer.nanosPerOperation(putRequestCount)));
    S3AFileStatus status = fs.getFileStatus(hugefile);
    assertEquals("File size in " + status, filesize, status.getLen());
    assertEquals("active put requests in \n" + fs,
        0, gaugeValue(putRequestsActive));
  }

  /**
   * Progress callback from AWS. Likely to come in on a different thread.
   */
  private class ProgressCallback implements Progressable,
      ProgressListener {
    private int counter = 0;

    @Override
    public void progress() {
      counter++;
    }

    public int getCounter() {
      return counter;
    }

    @Override
    public void progressChanged(ProgressEvent progressEvent) {
      counter++;
      if (progressEvent.getEventType().isByteCountEvent()) {
        LOG.debug("Event {}", progressEvent);
      } else {
        LOG.info("Event {}", progressEvent);
      }
    }
  }

  void assumeHugeFileExists() throws IOException {
    Assume.assumeTrue("No file " + hugefile, fs.exists(hugefile));
  }

  private void logFSState() {
    LOG.info("File System state after operation:\n{}", fs);
  }

  @Test
  public void test_040_PositionedReadHugeFile() throws Throwable {
    assumeHugeFileExists();
    final String encryption = getConf().getTrimmed(
        SERVER_SIDE_ENCRYPTION_ALGORITHM);
    boolean encrypted = encryption != null;
    if (encrypted) {
      LOG.info("File is encrypted with algorithm {}", encryption);
    }
    String filetype = encrypted ? "encrypted file" : "file";
    describe("Positioned reads of %s %s", filetype, hugefile);
    S3AFileStatus status = fs.getFileStatus(hugefile);
    long filesize = status.getLen();
    int ops = 0;
    final int bufferSize = 8192;
    byte[] buffer = new byte[bufferSize];
    long eof = filesize - 1;

    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    ContractTestUtils.NanoTimer readAtByte0, readAtByte0Again, readAtEOF;
    try (FSDataInputStream in = fs.open(hugefile, uploadBlockSize)) {
      readAtByte0 = new ContractTestUtils.NanoTimer();
      in.readFully(0, buffer);
      readAtByte0.end("time to read data at start of file");
      ops++;

      readAtEOF = new ContractTestUtils.NanoTimer();
      in.readFully(eof - bufferSize, buffer);
      readAtEOF.end("time to read data at end of file");
      ops++;

      readAtByte0Again = new ContractTestUtils.NanoTimer();
      in.readFully(0, buffer);
      readAtByte0Again.end("time to read data at start of file again");
      ops++;
      LOG.info("Final stream state: {}", in);
    }
    long mb = Math.max(filesize / _1MB, 1);

    logFSState();
    timer.end("time to performed positioned reads of %s of %d MB ",
        filetype, mb);
    LOG.info("Time per positioned read = {} nS",
        toHuman(timer.nanosPerOperation(ops)));
  }

  @Test
  public void test_050_readHugeFile() throws Throwable {
    assumeHugeFileExists();
    describe("Reading %s", hugefile);
    S3AFileStatus status = fs.getFileStatus(hugefile);
    long filesize = status.getLen();
    long blocks = filesize / uploadBlockSize;
    byte[] data = new byte[uploadBlockSize];

    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    try (FSDataInputStream in = fs.open(hugefile, uploadBlockSize)) {
      for (long block = 0; block < blocks; block++) {
        in.readFully(data);
      }
      LOG.info("Final stream state: {}", in);
    }

    long mb = Math.max(filesize / _1MB, 1);
    timer.end("time to read file of %d MB ", mb);
    LOG.info("Time per MB to read = {} nS",
        toHuman(timer.nanosPerOperation(mb)));
    bandwidth(timer, filesize);
    logFSState();
  }

  @Test
  public void test_100_renameHugeFile() throws Throwable {
    assumeHugeFileExists();
    describe("renaming %s to %s", hugefile, hugefileRenamed);
    S3AFileStatus status = fs.getFileStatus(hugefile);
    long filesize = status.getLen();
    fs.delete(hugefileRenamed, false);
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    fs.rename(hugefile, hugefileRenamed);
    long mb = Math.max(filesize / _1MB, 1);
    timer.end("time to rename file of %d MB", mb);
    LOG.info("Time per MB to rename = {} nS",
        toHuman(timer.nanosPerOperation(mb)));
    bandwidth(timer, filesize);
    logFSState();
    S3AFileStatus destFileStatus = fs.getFileStatus(hugefileRenamed);
    assertEquals(filesize, destFileStatus.getLen());

    // rename back
    ContractTestUtils.NanoTimer timer2 = new ContractTestUtils.NanoTimer();
    fs.rename(hugefileRenamed, hugefile);
    timer2.end("Renaming back");
    LOG.info("Time per MB to rename = {} nS",
        toHuman(timer2.nanosPerOperation(mb)));
    bandwidth(timer2, filesize);
  }

  @Test
  public void test_999_DeleteHugeFiles() throws IOException {
    describe("Deleting %s", hugefile);
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    fs.delete(hugefile, false);
    timer.end("time to delete %s", hugefile);
    ContractTestUtils.NanoTimer timer2 = new ContractTestUtils.NanoTimer();

    fs.delete(hugefileRenamed, false);
    timer2.end("time to delete %s", hugefileRenamed);
    ContractTestUtils.rm(fs, getTestPath(), true, true);
  }

}
