package org.apache.hadoop.fs.s3a;

import java.io.IOException;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;

import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_BLOCK_DEFAULT_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_BLOCK_SIZE_KEY;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_ENABLED_KEY;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;

public class ITestS3PrefetchingInputStream extends AbstractS3ACostTest {

  public ITestS3PrefetchingInputStream() {
    super(true);
  }

  /**
   * Tests that require a large file only run if the there is a named test file that can be read.
   */
  private boolean testDataAvailable = true;
  private static final int _1KB = 1024;

  // Path for file which should have length > block size so S3CachingInputStream is used
  private Path largeFile;
  private FileSystem fs;
  private int numBlocks;
  private int blockSize;
  private long largeFileSize;
  // Size should be < block size so S3InMemoryInputStream is used
  private int smallFileSize = _1KB * 16;

  @Override
  public void setup() throws Exception {
    super.setup();

    Configuration conf = getConfiguration();
    conf.setBoolean(PREFETCH_ENABLED_KEY, true);
  }

  private void openFS() throws IOException {
    Configuration conf = getConfiguration();

    largeFile = new Path(DEFAULT_CSVTEST_FILE);
    blockSize = conf.getInt(PREFETCH_BLOCK_SIZE_KEY, PREFETCH_BLOCK_DEFAULT_SIZE);
    fs = largeFile.getFileSystem(getConfiguration());
    FileStatus fileStatus = fs.getFileStatus(largeFile);
    largeFileSize = fileStatus.getLen();
    numBlocks = (largeFileSize == 0) ?
        0 :
        ((int) (largeFileSize / blockSize)) + (largeFileSize % blockSize > 0 ? 1 : 0);
  }

  @Test
  public void testReadLargeFileFully() throws Throwable {
    describe("read a large file fully");
    skipIfEmptyTestFile();
    openFS();

    try (FSDataInputStream in = fs.open(largeFile)) {
      IOStatistics ioStats = in.getIOStatistics();

      byte[] buffer = new byte[(int) largeFileSize];

      in.read(buffer, 0, (int) largeFileSize);

      verifyStatisticCounterValue(ioStats, StoreStatisticNames.ACTION_HTTP_GET_REQUEST, numBlocks);

      verifyStatisticCounterValue(ioStats, StreamStatisticNames.STREAM_READ_OPENED, numBlocks);
    }
  }

  @Test
  public void testRandomReadLargeFile() throws Throwable {
    describe("read a large file fully");
    skipIfEmptyTestFile();
    openFS();

    try (FSDataInputStream in = fs.open(largeFile)) {
      IOStatistics ioStats = in.getIOStatistics();

      byte[] buffer = new byte[blockSize];

      // Don't read the block completely so it gets cached on seek
      in.read(buffer, 0, blockSize - _1KB * 10);
      in.seek(blockSize + _1KB * 10);

      // Backwards seek, will use cached block
      in.seek(_1KB * 5);
      in.read();

      verifyStatisticCounterValue(ioStats, StoreStatisticNames.ACTION_HTTP_GET_REQUEST, 2);

      verifyStatisticCounterValue(ioStats, StreamStatisticNames.STREAM_READ_OPENED, 2);
    }
  }

  @Test
  public void testRandomReadSmallFile() throws Throwable {
    describe("random read on a small file");

    byte[] data = ContractTestUtils.dataset(smallFileSize, 'a', 26);
    Path smallFile = path("randomReadSmallFile");
    ContractTestUtils.writeDataset(getFileSystem(), smallFile, data, data.length, 16, true);

    try (FSDataInputStream in = getFileSystem().open(smallFile)) {
      IOStatistics ioStats = in.getIOStatistics();

      byte[] buffer = new byte[smallFileSize];

      in.read(buffer, 0, _1KB * 4);
      in.seek(_1KB * 12);
      in.read(buffer, 0, _1KB * 4);

      verifyStatisticCounterValue(ioStats, StoreStatisticNames.ACTION_HTTP_GET_REQUEST, 1);

      verifyStatisticCounterValue(ioStats, StreamStatisticNames.STREAM_READ_OPENED, 1);
    }

  }

  private void skipIfEmptyTestFile() {
    if (!testDataAvailable) {
      skip("Test data file not specified");
    }
  }
}
