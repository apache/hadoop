package org.apache.hadoop.fs.azurebfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.hadoop.fs.azurebfs.utils.MockFastpathConnection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.azurebfs.services.MockAbfsInputStream;

import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.GET_RESPONSES;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_MAX_IO_RETRIES;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_READ_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READ_AHEAD_QUEUE_DEPTH;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MIN_BUFFER_SIZE;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class TestAbfsFastpath extends AbstractAbfsIntegrationTest {

  @Rule
  public TestName methodName = new TestName();

  public TestAbfsFastpath() throws Exception {
    super();
  }

  @Test
  public void testMockFastpathFileDeleted() throws Exception {
    AzureBlobFileSystem fs = getAbfsFileSystem(2, MIN_BUFFER_SIZE, 0);
    AbfsInputStream inStream = createTestfileAndGetInputStream(fs,
        this.methodName.getMethodName(), 4 * MIN_BUFFER_SIZE);
    ((MockAbfsInputStream) inStream).induceError(404);
    Map<String, Long> metricMap;
    metricMap = fs.getInstrumentationMap();
    long expectedConnectionsMade = metricMap.get(
        CONNECTIONS_MADE.getStatName());
    long expectedGetResponses = metricMap.get(GET_RESPONSES.getStatName());
    // read will fail with FileNotFound, there will be no retries
    intercept(FileNotFoundException.class, () -> inStream.read());
    expectedConnectionsMade += 1;
    expectedGetResponses += 1;
    metricMap = fs.getInstrumentationMap();
    assertAbfsStatistics(CONNECTIONS_MADE,
        expectedConnectionsMade, metricMap);
    assertAbfsStatistics(GET_RESPONSES,
        expectedGetResponses, metricMap);

  }

  private AzureBlobFileSystem getAbfsFileSystem(int maxReqRetryCount,
      int bufferSize,
      int readAheadDepth) throws IOException {
    Configuration config = this.getRawConfiguration();
    config.setInt(AZURE_MAX_IO_RETRIES, maxReqRetryCount);
    config.setInt(AZURE_READ_BUFFER_SIZE, bufferSize);
    config.setInt(FS_AZURE_READ_AHEAD_QUEUE_DEPTH, readAheadDepth);
    return (AzureBlobFileSystem) FileSystem.get(this.getFileSystem().getUri(),
        config);
  }

  private AbfsInputStream createTestfileAndGetInputStream(final AzureBlobFileSystem fs,
      final String methodName,
      int fileSize)
      throws IOException {
    final byte[] writeBuffer = new byte[fileSize];
    new Random().nextBytes(writeBuffer);
    Path testPath = new Path(methodName);
    try (FSDataOutputStream outStream = fs.create(testPath)) {
      outStream.write(writeBuffer);
    }

    MockFastpathConnection.registerAppend(fileSize, testPath.getName(),
        writeBuffer, 0, fileSize);
    return getMockAbfsInputStream(fs, testPath);
  }

  @Test
  public void testThrottled() throws Exception {
    AzureBlobFileSystem fs = getAbfsFileSystem(2, MIN_BUFFER_SIZE, 0);
    AbfsInputStream inStream = createTestfileAndGetInputStream(fs,
        this.methodName.getMethodName(), 4 * MIN_BUFFER_SIZE);
    ((MockAbfsInputStream) inStream).induceError(503);
    Map<String, Long> metricMap;
    metricMap = fs.getInstrumentationMap();
    long expectedConnectionsMade = metricMap.get(
        CONNECTIONS_MADE.getStatName());
    long expectedGetResponses = metricMap.get(GET_RESPONSES.getStatName());
    // read will fail with IOException, retries capped to 2 in this test class
    // so total 3
    intercept(IOException.class, () -> inStream.read());
    expectedConnectionsMade += 3;
    expectedGetResponses += 3;
    metricMap = fs.getInstrumentationMap();
    assertAbfsStatistics(CONNECTIONS_MADE,
        expectedConnectionsMade, metricMap);
    assertAbfsStatistics(GET_RESPONSES,
        expectedGetResponses, metricMap);
  }

  @Test
  public void testFastpathRequestFailure() throws IOException {
    AzureBlobFileSystem fs = getAbfsFileSystem(2, MIN_BUFFER_SIZE, 0);
    AbfsInputStream inStream = createTestfileAndGetInputStream(fs,
        this.methodName.getMethodName(), 4 * MIN_BUFFER_SIZE);
    ((MockAbfsInputStream) inStream).induceRequestException();
    byte[] readBuffer = new byte[MIN_BUFFER_SIZE];
    Map<String, Long> metricMap;
    metricMap = fs.getInstrumentationMap();
    long expectedConnectionsMade = metricMap.get(
        CONNECTIONS_MADE.getStatName());
    long expectedGetResponses = metricMap.get(GET_RESPONSES.getStatName());
    // read will attempt over fastpath, but will fail with exception => 1+conn 0+getresp
    // will attempt on http connection => 1+conn 1+getrsp
    inStream.read(readBuffer, 0, MIN_BUFFER_SIZE);
    // move out of buffered range
    inStream.seek(3 * MIN_BUFFER_SIZE);
    // input stream still on fast path as earlier it was request failure
    // read will attempt over fastpath, but will fail with exception => 1+conn 0+getresp
    // will attempt on http connection => 1+conn 1+getrsp
    inStream.read(readBuffer, 0, MIN_BUFFER_SIZE);
    expectedConnectionsMade += 4;
    expectedGetResponses += 2;
    metricMap = fs.getInstrumentationMap();
    assertAbfsStatistics(CONNECTIONS_MADE,
        expectedConnectionsMade, metricMap);
    assertAbfsStatistics(GET_RESPONSES,
        expectedGetResponses, metricMap);
  }

  @Test
  public void testFastpathConnectionFailure() throws IOException {
    AzureBlobFileSystem fs = getAbfsFileSystem(2, MIN_BUFFER_SIZE, 0);
    AbfsInputStream inStream = createTestfileAndGetInputStream(fs,
        this.methodName.getMethodName(), 4 * MIN_BUFFER_SIZE);
    ((MockAbfsInputStream) inStream).induceConnectionException();
    byte[] readBuffer = new byte[MIN_BUFFER_SIZE];
    Map<String, Long> metricMap;
    metricMap = fs.getInstrumentationMap();
    long expectedConnectionsMade = metricMap.get(
        CONNECTIONS_MADE.getStatName());
    long expectedGetResponses = metricMap.get(GET_RESPONSES.getStatName());
    // read will attempt over fastpath, but will fail with exception => 1+conn 0+getresp
    // will attempt on http connection => 1+conn 1+getrsp
    inStream.read(readBuffer, 0, MIN_BUFFER_SIZE);
    // move out of buffered range
    inStream.seek(3 * MIN_BUFFER_SIZE);
    // input stream will have switched to http permanentely due to conn failure
    // next read direct on http => 1+conn 1+getrsp
    inStream.read(readBuffer, 0, MIN_BUFFER_SIZE);
    expectedConnectionsMade += 3;
    expectedGetResponses += 2;
    metricMap = fs.getInstrumentationMap();
    assertAbfsStatistics(CONNECTIONS_MADE,
        expectedConnectionsMade, metricMap);
    assertAbfsStatistics(GET_RESPONSES,
        expectedGetResponses, metricMap);
  }
}
