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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListener;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.util.Progressable;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;

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
  public static final String DEFAULT_PARTITION_SIZE = "8M";
  private Path scaleTestDir;
  private Path hugefile;
  private Path hugefileRenamed;

  private int uploadBlockSize = DEFAULT_UPLOAD_BLOCKSIZE;
  private int partitionSize;
  private long filesize;

  @Override
  public void setup() throws Exception {
    super.setup();
    scaleTestDir = new Path(getTestPath(), getTestSuiteName());
    hugefile = new Path(scaleTestDir, "hugefile");
    hugefileRenamed = new Path(scaleTestDir, "hugefileRenamed");
    filesize = getTestPropertyBytes(getConf(), KEY_HUGE_FILESIZE,
        DEFAULT_HUGE_FILESIZE);
  }

  /**
   * Get the name of this test suite, which is used in path generation.
   * Base implementation uses {@link #getBlockOutputBufferName()} for this.
   * @return the name of the suite.
   */
  public String getTestSuiteName() {
    return getBlockOutputBufferName();
  }

  /**
   * Note that this can get called before test setup.
   * @return the configuration to use.
   */
  @Override
  protected Configuration createScaleConfiguration() {
    Configuration conf = super.createScaleConfiguration();
    partitionSize = (int) getTestPropertyBytes(conf,
        KEY_HUGE_PARTITION_SIZE,
        DEFAULT_PARTITION_SIZE);
    assertTrue("Partition size too small: " + partitionSize,
        partitionSize > MULTIPART_MIN_SIZE);
    conf.setLong(SOCKET_SEND_BUFFER, _1MB);
    conf.setLong(SOCKET_RECV_BUFFER, _1MB);
    conf.setLong(MIN_MULTIPART_THRESHOLD, partitionSize);
    conf.setInt(MULTIPART_SIZE, partitionSize);
    conf.set(USER_AGENT_PREFIX, "STestS3AHugeFileCreate");
    conf.set(FAST_UPLOAD_BUFFER, getBlockOutputBufferName());
    S3ATestUtils.disableFilesystemCaching(conf);
    return conf;
  }

  /**
   * The name of the buffering mechanism to use.
   * @return a buffering mechanism
   */
  protected abstract String getBlockOutputBufferName();

  @Test
  public void test_010_CreateHugeFile() throws IOException {
    assertFalse("Please run this test sequentially to avoid timeouts" +
        " and bandwidth problems", isParallelExecution());
    long filesizeMB = filesize / _1MB;

    // clean up from any previous attempts
    deleteHugeFile();

    Path fileToCreate = getPathOfFileToCreate();
    describe("Creating file %s of size %d MB" +
            " with partition size %d buffered by %s",
        fileToCreate, filesizeMB, partitionSize, getBlockOutputBufferName());

    // now do a check of available upload time, with a pessimistic bandwidth
    // (that of remote upload tests). If the test times out then not only is
    // the test outcome lost, as the follow-on tests continue, they will
    // overlap with the ongoing upload test, for much confusion.
    int timeout = getTestTimeoutSeconds();
    // assume 1 MB/s upload bandwidth
    int bandwidth = _1MB;
    long uploadTime = filesize / bandwidth;
    assertTrue(String.format("Timeout set in %s seconds is too low;" +
            " estimating upload time of %d seconds at 1 MB/s." +
            " Rerun tests with -D%s=%d",
        timeout, uploadTime, KEY_TEST_TIMEOUT, uploadTime * 2),
        uploadTime < timeout);
    assertEquals("File size set in " + KEY_HUGE_FILESIZE + " = " + filesize
            + " is not a multiple of " + uploadBlockSize,
        0, filesize % uploadBlockSize);

    byte[] data = new byte[uploadBlockSize];
    for (int i = 0; i < uploadBlockSize; i++) {
      data[i] = (byte) (i % 256);
    }

    long blocks = filesize / uploadBlockSize;
    long blocksPerMB = _1MB / uploadBlockSize;

    // perform the upload.
    // there's lots of logging here, so that a tail -f on the output log
    // can give a view of what is happening.
    S3AFileSystem fs = getFileSystem();
    StorageStatistics storageStatistics = fs.getStorageStatistics();
    String putRequests = Statistic.OBJECT_PUT_REQUESTS.getSymbol();
    String putBytes = Statistic.OBJECT_PUT_BYTES.getSymbol();
    Statistic putRequestsActive = Statistic.OBJECT_PUT_REQUESTS_ACTIVE;
    Statistic putBytesPending = Statistic.OBJECT_PUT_BYTES_PENDING;

    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    S3AInstrumentation.OutputStreamStatistics streamStatistics;
    long blocksPer10MB = blocksPerMB * 10;
    ProgressCallback progress = new ProgressCallback(timer);
    try (FSDataOutputStream out = fs.create(fileToCreate,
        true,
        uploadBlockSize,
        progress)) {
      try {
        streamStatistics = getOutputStreamStatistics(out);
      } catch (ClassCastException e) {
        LOG.info("Wrapped output stream is not block stream: {}",
            out.getWrappedStream());
        streamStatistics = null;
      }

      for (long block = 1; block <= blocks; block++) {
        out.write(data);
        long written = block * uploadBlockSize;
        // every 10 MB and on file upload @ 100%, print some stats
        if (block % blocksPer10MB == 0 || written == filesize) {
          long percentage = written * 100 / filesize;
          double elapsedTime = timer.elapsedTime() / 1.0e9;
          double writtenMB = 1.0 * written / _1MB;
          LOG.info(String.format("[%02d%%] Buffered %.2f MB out of %d MB;" +
                  " PUT %d bytes (%d pending) in %d operations (%d active);" +
                  " elapsedTime=%.2fs; write to buffer bandwidth=%.2f MB/s",
              percentage,
              writtenMB,
              filesizeMB,
              storageStatistics.getLong(putBytes),
              gaugeValue(putBytesPending),
              storageStatistics.getLong(putRequests),
              gaugeValue(putRequestsActive),
              elapsedTime,
              writtenMB / elapsedTime));
        }
      }
      // now close the file
      LOG.info("Closing stream {}", out);
      LOG.info("Statistics : {}", streamStatistics);
      ContractTestUtils.NanoTimer closeTimer
          = new ContractTestUtils.NanoTimer();
      out.close();
      closeTimer.end("time to close() output stream");
    }

    timer.end("time to write %d MB in blocks of %d",
        filesizeMB, uploadBlockSize);
    logFSState();
    bandwidth(timer, filesize);
    LOG.info("Statistics after stream closed: {}", streamStatistics);
    long putRequestCount = storageStatistics.getLong(putRequests);
    Long putByteCount = storageStatistics.getLong(putBytes);
    LOG.info("PUT {} bytes in {} operations; {} MB/operation",
        putByteCount, putRequestCount,
        putByteCount / (putRequestCount * _1MB));
    LOG.info("Time per PUT {} nS",
        toHuman(timer.nanosPerOperation(putRequestCount)));
    assertEquals("active put requests in \n" + fs,
        0, gaugeValue(putRequestsActive));
    progress.verifyNoFailures(
        "Put file " + fileToCreate + " of size " + filesize);
    if (streamStatistics != null) {
      assertEquals("actively allocated blocks in " + streamStatistics,
          0, streamStatistics.blocksActivelyAllocated());
    }
  }

  /**
   * Get the path of the file which is to created. This is normally
   * {@link #hugefile}
   * @return the path to use when creating the file.
   */
  protected Path getPathOfFileToCreate() {
    return this.hugefile;
  }

  protected Path getScaleTestDir() {
    return scaleTestDir;
  }

  protected Path getHugefile() {
    return hugefile;
  }

  public void setHugefile(Path hugefile) {
    this.hugefile = hugefile;
  }

  protected Path getHugefileRenamed() {
    return hugefileRenamed;
  }

  protected int getUploadBlockSize() {
    return uploadBlockSize;
  }

  protected int getPartitionSize() {
    return partitionSize;
  }

  /**
   * Progress callback from AWS. Likely to come in on a different thread.
   */
  private final class ProgressCallback implements Progressable,
      ProgressListener {
    private AtomicLong bytesTransferred = new AtomicLong(0);
    private AtomicInteger failures = new AtomicInteger(0);
    private final ContractTestUtils.NanoTimer timer;

    private ProgressCallback(NanoTimer timer) {
      this.timer = timer;
    }

    @Override
    public void progress() {
    }

    @Override
    public void progressChanged(ProgressEvent progressEvent) {
      ProgressEventType eventType = progressEvent.getEventType();
      if (eventType.isByteCountEvent()) {
        bytesTransferred.addAndGet(progressEvent.getBytesTransferred());
      }
      switch (eventType) {
      case TRANSFER_PART_FAILED_EVENT:
        // failure
        failures.incrementAndGet();
        LOG.warn("Transfer failure");
        break;
      case TRANSFER_PART_COMPLETED_EVENT:
        // completion
        long elapsedTime = timer.elapsedTime();
        double elapsedTimeS = elapsedTime / 1.0e9;
        long written = bytesTransferred.get();
        long writtenMB = written / _1MB;
        LOG.info(String.format(
            "Event %s; total uploaded=%d MB in %.1fs;" +
                " effective upload bandwidth = %.2f MB/s",
            progressEvent,
            writtenMB, elapsedTimeS, writtenMB / elapsedTimeS));
        break;
      default:
        if (eventType.isByteCountEvent()) {
          LOG.debug("Event {}", progressEvent);
        } else {
          LOG.info("Event {}", progressEvent);
        }
        break;
      }
    }

    @Override
    public String toString() {
      String sb = "ProgressCallback{"
          + "bytesTransferred=" + bytesTransferred +
          ", failures=" + failures +
          '}';
      return sb;
    }

    private void verifyNoFailures(String operation) {
      assertEquals("Failures in " + operation + ": " + this, 0, failures.get());
    }
  }

  /**
   * Assume that the huge file exists; skip the test if it does not.
   * @throws IOException IO failure
   */
  void assumeHugeFileExists() throws IOException {
    assumeFileExists(this.hugefile);
  }

  /**
   * Assume a specific file exists.
   * @param file file to look for
   * @throws IOException IO problem
   */
  private void assumeFileExists(Path file) throws IOException {
    S3AFileSystem fs = getFileSystem();
    ContractTestUtils.assertPathExists(fs, "huge file not created",
        file);
    FileStatus status = fs.getFileStatus(file);
    ContractTestUtils.assertIsFile(file, status);
    assertTrue("File " + file + " is empty", status.getLen() > 0);
  }

  private void logFSState() {
    LOG.info("File System state after operation:\n{}", getFileSystem());
  }

  /**
   * This is the set of actions to perform when verifying the file actually
   * was created. With the s3guard committer, the file doesn't come into
   * existence; a different set of assertions must be checked.
   */
  @Test
  public void test_030_postCreationAssertions() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    ContractTestUtils.assertPathExists(fs, "Huge file", hugefile);
    FileStatus status = fs.getFileStatus(hugefile);
    ContractTestUtils.assertIsFile(hugefile, status);
    assertEquals("File size in " + status, filesize, status.getLen());
  }

  /**
   * Read in the file using Positioned read(offset) calls.
   * @throws Throwable failure
   */
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
    S3AFileSystem fs = getFileSystem();
    FileStatus status = fs.getFileStatus(hugefile);
    long size = status.getLen();
    int ops = 0;
    final int bufferSize = 8192;
    byte[] buffer = new byte[bufferSize];
    long eof = size - 1;

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
    long mb = Math.max(size / _1MB, 1);

    logFSState();
    timer.end("time to perform positioned reads of %s of %d MB ",
        filetype, mb);
    LOG.info("Time per positioned read = {} nS",
        toHuman(timer.nanosPerOperation(ops)));
  }

  /**
   * Read in the entire file using read() calls.
   * @throws Throwable failure
   */
  @Test
  public void test_050_readHugeFile() throws Throwable {
    assumeHugeFileExists();
    describe("Reading %s", hugefile);
    S3AFileSystem fs = getFileSystem();
    FileStatus status = fs.getFileStatus(hugefile);
    long size = status.getLen();
    long blocks = size / uploadBlockSize;
    byte[] data = new byte[uploadBlockSize];

    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    try (FSDataInputStream in = fs.open(hugefile, uploadBlockSize)) {
      for (long block = 0; block < blocks; block++) {
        in.readFully(data);
      }
      LOG.info("Final stream state: {}", in);
    }

    long mb = Math.max(size / _1MB, 1);
    timer.end("time to read file of %d MB ", mb);
    LOG.info("Time per MB to read = {} nS",
        toHuman(timer.nanosPerOperation(mb)));
    bandwidth(timer, size);
    logFSState();
  }

  @Test
  public void test_100_renameHugeFile() throws Throwable {
    assumeHugeFileExists();
    describe("renaming %s to %s", hugefile, hugefileRenamed);
    S3AFileSystem fs = getFileSystem();
    FileStatus status = fs.getFileStatus(hugefile);
    long size = status.getLen();
    fs.delete(hugefileRenamed, false);
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    fs.rename(hugefile, hugefileRenamed);
    long mb = Math.max(size / _1MB, 1);
    timer.end("time to rename file of %d MB", mb);
    LOG.info("Time per MB to rename = {} nS",
        toHuman(timer.nanosPerOperation(mb)));
    bandwidth(timer, size);
    logFSState();
    FileStatus destFileStatus = fs.getFileStatus(hugefileRenamed);
    assertEquals(size, destFileStatus.getLen());

    // rename back
    ContractTestUtils.NanoTimer timer2 = new ContractTestUtils.NanoTimer();
    fs.rename(hugefileRenamed, hugefile);
    timer2.end("Renaming back");
    LOG.info("Time per MB to rename = {} nS",
        toHuman(timer2.nanosPerOperation(mb)));
    bandwidth(timer2, size);
  }

  /**
   * Cleanup: delete the files.
   */
  @Test
  public void test_800_DeleteHugeFiles() throws IOException {
    try {
      deleteHugeFile();
      delete(hugefileRenamed, false);
    } finally {
      ContractTestUtils.rm(getFileSystem(), getTestPath(), true, false);
    }
  }

  /**
   * After all the work, dump the statistics.
   */
  @Test
  public void test_900_dumpStats() {
    StringBuilder sb = new StringBuilder();

    getFileSystem().getStorageStatistics()
        .forEach(kv -> sb.append(kv.toString()).append("\n"));

    LOG.info("Statistics\n{}", sb);
  }

  protected void deleteHugeFile() throws IOException {
    delete(hugefile, false);
  }

  /**
   * Delete any file, time how long it took.
   * @param path path to delete
   * @param recursive recursive flag
   */
  protected void delete(Path path, boolean recursive) throws IOException {
    describe("Deleting %s", path);
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    getFileSystem().delete(path, recursive);
    timer.end("time to delete %s", path);
  }

}
