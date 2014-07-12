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

package org.apache.hadoop.fs.azure.metrics;

import static org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation.WASB_CLIENT_ERRORS;
import static org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation.WASB_DIRECTORIES_CREATED;
import static org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation.WASB_DOWNLOAD_LATENCY;
import static org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation.WASB_DOWNLOAD_RATE;
import static org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation.WASB_FILES_CREATED;
import static org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation.WASB_FILES_DELETED;
import static org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation.WASB_SERVER_ERRORS;
import static org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation.WASB_UPLOAD_LATENCY;
import static org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation.WASB_UPLOAD_RATE;
import static org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation.WASB_WEB_RESPONSES;
import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.verify;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Date;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.AzureBlobStorageTestAccount;
import org.apache.hadoop.fs.azure.AzureException;
import org.apache.hadoop.fs.azure.AzureNativeFileSystemStore;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsTag;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestAzureFileSystemInstrumentation {
  private FileSystem fs;
  private AzureBlobStorageTestAccount testAccount;

  @Before
  public void setUp() throws Exception {
    testAccount = AzureBlobStorageTestAccount.create();
    if (testAccount != null) {
      fs = testAccount.getFileSystem();
    }
    assumeNotNull(testAccount);
  }

  @After
  public void tearDown() throws Exception {
    if (testAccount != null) {
      testAccount.cleanup();
      testAccount = null;
      fs = null;
    }
  }

  @Test
  public void testMetricTags() throws Exception {
    String accountName =
        testAccount.getRealAccount().getBlobEndpoint()
        .getAuthority();
    String containerName =
        testAccount.getRealContainer().getName();
    MetricsRecordBuilder myMetrics = getMyMetrics();
    verify(myMetrics).add(argThat(
        new TagMatcher("accountName", accountName)
        ));
    verify(myMetrics).add(argThat(
        new TagMatcher("containerName", containerName)
        ));
    verify(myMetrics).add(argThat(
        new TagMatcher("Context", "azureFileSystem")
        ));
    verify(myMetrics).add(argThat(
        new TagExistsMatcher("wasbFileSystemId")
        ));
  }
  

  @Test
  public void testMetricsOnMkdirList() throws Exception {
    long base = getBaseWebResponses();
    
    // Create a directory
    assertTrue(fs.mkdirs(new Path("a")));
    // At the time of writing, it takes 1 request to create the actual directory,
    // plus 2 requests per level to check that there's no blob with that name and
    // 1 request per level above to create it if it doesn't exist.
    // So for the path above (/user/<name>/a), it takes 2 requests each to check
    // there's no blob called /user, no blob called /user/<name> and no blob
    // called /user/<name>/a, and then 3 request for the creation of the three
    // levels, and then 2 requests for checking/stamping the version of AS,
    // totaling 11.
    // Also, there's the initial 1 request for container check so total is 12.
    base = assertWebResponsesInRange(base, 1, 12);
    assertEquals(1,
        AzureMetricsTestUtil.getLongCounterValue(getInstrumentation(), WASB_DIRECTORIES_CREATED));

    // List the root contents
    assertEquals(1, fs.listStatus(new Path("/")).length);    
    base = assertWebResponsesEquals(base, 1);

    assertNoErrors();
  }

  private BandwidthGaugeUpdater getBandwidthGaugeUpdater() {
    NativeAzureFileSystem azureFs = (NativeAzureFileSystem)fs;
    AzureNativeFileSystemStore azureStore = azureFs.getStore();
    return azureStore.getBandwidthGaugeUpdater();
  }

  private static byte[] nonZeroByteArray(int size) {
    byte[] data = new byte[size];
    Arrays.fill(data, (byte)5);
    return data;
  }

  @Test
  public void testMetricsOnFileCreateRead() throws Exception {
    long base = getBaseWebResponses();
    
    assertEquals(0, AzureMetricsTestUtil.getCurrentBytesWritten(getInstrumentation()));

    Path filePath = new Path("/metricsTest_webResponses");
    final int FILE_SIZE = 1000;

    // Suppress auto-update of bandwidth metrics so we get
    // to update them exactly when we want to.
    getBandwidthGaugeUpdater().suppressAutoUpdate();

    // Create a file
    Date start = new Date();
    OutputStream outputStream = fs.create(filePath);
    outputStream.write(nonZeroByteArray(FILE_SIZE));
    outputStream.close();
    long uploadDurationMs = new Date().getTime() - start.getTime();
    
    // The exact number of requests/responses that happen to create a file
    // can vary  - at the time of writing this code it takes 10
    // requests/responses for the 1000 byte file (33 for 100 MB),
    // plus the initial container-check request but that
    // can very easily change in the future. Just assert that we do roughly
    // more than 2 but less than 15.
    logOpResponseCount("Creating a 1K file", base);
    base = assertWebResponsesInRange(base, 2, 15);
    getBandwidthGaugeUpdater().triggerUpdate(true);
    long bytesWritten = AzureMetricsTestUtil.getCurrentBytesWritten(getInstrumentation());
    assertTrue("The bytes written in the last second " + bytesWritten +
        " is pretty far from the expected range of around " + FILE_SIZE +
        " bytes plus a little overhead.",
        bytesWritten > (FILE_SIZE / 2) && bytesWritten < (FILE_SIZE * 2));
    long totalBytesWritten = AzureMetricsTestUtil.getCurrentTotalBytesWritten(getInstrumentation());
    assertTrue("The total bytes written  " + totalBytesWritten +
        " is pretty far from the expected range of around " + FILE_SIZE +
        " bytes plus a little overhead.",
        totalBytesWritten >= FILE_SIZE && totalBytesWritten < (FILE_SIZE * 2));
    long uploadRate = AzureMetricsTestUtil.getLongGaugeValue(getInstrumentation(), WASB_UPLOAD_RATE);
    System.out.println("Upload rate: " + uploadRate + " bytes/second.");
    long expectedRate = (FILE_SIZE * 1000L) / uploadDurationMs;
    assertTrue("The upload rate " + uploadRate +
        " is below the expected range of around " + expectedRate +
        " bytes/second that the unit test observed. This should never be" +
        " the case since the test underestimates the rate by looking at " +
        " end-to-end time instead of just block upload time.",
        uploadRate >= expectedRate);
    long uploadLatency = AzureMetricsTestUtil.getLongGaugeValue(getInstrumentation(),
        WASB_UPLOAD_LATENCY);
    System.out.println("Upload latency: " + uploadLatency);
    long expectedLatency = uploadDurationMs; // We're uploading less than a block.
    assertTrue("The upload latency " + uploadLatency +
        " should be greater than zero now that I've just uploaded a file.",
        uploadLatency > 0);
    assertTrue("The upload latency " + uploadLatency +
        " is more than the expected range of around " + expectedLatency +
        " milliseconds that the unit test observed. This should never be" +
        " the case since the test overestimates the latency by looking at " +
        " end-to-end time instead of just block upload time.",
        uploadLatency <= expectedLatency);
    
    // Read the file
    start = new Date();
    InputStream inputStream = fs.open(filePath);
    int count = 0;
    while (inputStream.read() >= 0) {
      count++;
    }
    inputStream.close();
    long downloadDurationMs = new Date().getTime() - start.getTime();
    assertEquals(FILE_SIZE, count);

    // Again, exact number varies. At the time of writing this code
    // it takes 4 request/responses, so just assert a rough range between
    // 1 and 10.
    logOpResponseCount("Reading a 1K file", base);
    base = assertWebResponsesInRange(base, 1, 10);
    getBandwidthGaugeUpdater().triggerUpdate(false);
    long totalBytesRead = AzureMetricsTestUtil.getCurrentTotalBytesRead(getInstrumentation());
    assertEquals(FILE_SIZE, totalBytesRead);
    long bytesRead = AzureMetricsTestUtil.getCurrentBytesRead(getInstrumentation());
    assertTrue("The bytes read in the last second " + bytesRead +
        " is pretty far from the expected range of around " + FILE_SIZE +
        " bytes plus a little overhead.",
        bytesRead > (FILE_SIZE / 2) && bytesRead < (FILE_SIZE * 2));
    long downloadRate = AzureMetricsTestUtil.getLongGaugeValue(getInstrumentation(), WASB_DOWNLOAD_RATE);
    System.out.println("Download rate: " + downloadRate + " bytes/second.");
    expectedRate = (FILE_SIZE * 1000L) / downloadDurationMs;
    assertTrue("The download rate " + downloadRate +
        " is below the expected range of around " + expectedRate +
        " bytes/second that the unit test observed. This should never be" +
        " the case since the test underestimates the rate by looking at " +
        " end-to-end time instead of just block download time.",
        downloadRate >= expectedRate);
    long downloadLatency = AzureMetricsTestUtil.getLongGaugeValue(getInstrumentation(),
        WASB_DOWNLOAD_LATENCY);
    System.out.println("Download latency: " + downloadLatency);
    expectedLatency = downloadDurationMs; // We're downloading less than a block.
    assertTrue("The download latency " + downloadLatency +
        " should be greater than zero now that I've just downloaded a file.",
        downloadLatency > 0);
    assertTrue("The download latency " + downloadLatency +
        " is more than the expected range of around " + expectedLatency +
        " milliseconds that the unit test observed. This should never be" +
        " the case since the test overestimates the latency by looking at " +
        " end-to-end time instead of just block download time.",
        downloadLatency <= expectedLatency);

    assertNoErrors();
  }

  @Test
  public void testMetricsOnBigFileCreateRead() throws Exception {
    long base = getBaseWebResponses();

    assertEquals(0, AzureMetricsTestUtil.getCurrentBytesWritten(getInstrumentation()));

    Path filePath = new Path("/metricsTest_webResponses");
    final int FILE_SIZE = 100 * 1024 * 1024;

    // Suppress auto-update of bandwidth metrics so we get
    // to update them exactly when we want to.
    getBandwidthGaugeUpdater().suppressAutoUpdate();

    // Create a file
    OutputStream outputStream = fs.create(filePath);
    outputStream.write(new byte[FILE_SIZE]);
    outputStream.close();

    // The exact number of requests/responses that happen to create a file
    // can vary  - at the time of writing this code it takes 34
    // requests/responses for the 100 MB file,
    // plus the initial container check request, but that
    // can very easily change in the future. Just assert that we do roughly
    // more than 20 but less than 50.
    logOpResponseCount("Creating a 100 MB file", base);
    base = assertWebResponsesInRange(base, 20, 50);
    getBandwidthGaugeUpdater().triggerUpdate(true);
    long totalBytesWritten = AzureMetricsTestUtil.getCurrentTotalBytesWritten(getInstrumentation());
    assertTrue("The total bytes written  " + totalBytesWritten +
        " is pretty far from the expected range of around " + FILE_SIZE +
        " bytes plus a little overhead.",
        totalBytesWritten >= FILE_SIZE && totalBytesWritten < (FILE_SIZE * 2));
    long uploadRate = AzureMetricsTestUtil.getLongGaugeValue(getInstrumentation(), WASB_UPLOAD_RATE);
    System.out.println("Upload rate: " + uploadRate + " bytes/second.");
    long uploadLatency = AzureMetricsTestUtil.getLongGaugeValue(getInstrumentation(),
        WASB_UPLOAD_LATENCY);
    System.out.println("Upload latency: " + uploadLatency);
    assertTrue("The upload latency " + uploadLatency +
        " should be greater than zero now that I've just uploaded a file.",
        uploadLatency > 0);

    // Read the file
    InputStream inputStream = fs.open(filePath);
    int count = 0;
    while (inputStream.read() >= 0) {
      count++;
    }
    inputStream.close();
    assertEquals(FILE_SIZE, count);

    // Again, exact number varies. At the time of writing this code
    // it takes 27 request/responses, so just assert a rough range between
    // 20 and 40.
    logOpResponseCount("Reading a 100 MB file", base);
    base = assertWebResponsesInRange(base, 20, 40);
    getBandwidthGaugeUpdater().triggerUpdate(false);
    long totalBytesRead = AzureMetricsTestUtil.getCurrentTotalBytesRead(getInstrumentation());
    assertEquals(FILE_SIZE, totalBytesRead);
    long downloadRate = AzureMetricsTestUtil.getLongGaugeValue(getInstrumentation(), WASB_DOWNLOAD_RATE);
    System.out.println("Download rate: " + downloadRate + " bytes/second.");
    long downloadLatency = AzureMetricsTestUtil.getLongGaugeValue(getInstrumentation(),
        WASB_DOWNLOAD_LATENCY);
    System.out.println("Download latency: " + downloadLatency);
    assertTrue("The download latency " + downloadLatency +
        " should be greater than zero now that I've just downloaded a file.",
        downloadLatency > 0);
  }

  @Test
  public void testMetricsOnFileRename() throws Exception {
    long base = getBaseWebResponses();

    Path originalPath = new Path("/metricsTest_RenameStart");
    Path destinationPath = new Path("/metricsTest_RenameFinal");

    // Create an empty file
    assertEquals(0, AzureMetricsTestUtil.getLongCounterValue(getInstrumentation(), WASB_FILES_CREATED));
    assertTrue(fs.createNewFile(originalPath));
    logOpResponseCount("Creating an empty file", base);
    base = assertWebResponsesInRange(base, 2, 20);
    assertEquals(1, AzureMetricsTestUtil.getLongCounterValue(getInstrumentation(), WASB_FILES_CREATED));

    // Rename the file
    assertTrue(fs.rename(originalPath, destinationPath));
    // Varies: at the time of writing this code it takes 7 requests/responses.
    logOpResponseCount("Renaming a file", base);
    base = assertWebResponsesInRange(base, 2, 15);

    assertNoErrors();
  }

  @Test
  public void testMetricsOnFileExistsDelete() throws Exception {
    long base = getBaseWebResponses();

    Path filePath = new Path("/metricsTest_delete");

    // Check existence
    assertFalse(fs.exists(filePath));
    // At the time of writing this code it takes 2 requests/responses to
    // check existence, which seems excessive, plus initial request for
    // container check.
    logOpResponseCount("Checking file existence for non-existent file", base);
    base = assertWebResponsesInRange(base, 1, 3);

    // Create an empty file
    assertTrue(fs.createNewFile(filePath));
    base = getCurrentWebResponses();

    // Check existence again
    assertTrue(fs.exists(filePath));
    logOpResponseCount("Checking file existence for existent file", base);
    base = assertWebResponsesInRange(base, 1, 2);

    // Delete the file
    assertEquals(0, AzureMetricsTestUtil.getLongCounterValue(getInstrumentation(), WASB_FILES_DELETED));
    assertTrue(fs.delete(filePath, false));
    // At the time of writing this code it takes 4 requests/responses to
    // delete, which seems excessive. Check for range 1-4 for now.
    logOpResponseCount("Deleting a file", base);
    base = assertWebResponsesInRange(base, 1, 4);
    assertEquals(1, AzureMetricsTestUtil.getLongCounterValue(getInstrumentation(), WASB_FILES_DELETED));

    assertNoErrors();
  }

  @Test
  public void testMetricsOnDirRename() throws Exception {
    long base = getBaseWebResponses();
    
    Path originalDirName = new Path("/metricsTestDirectory_RenameStart");
    Path innerFileName = new Path(originalDirName, "innerFile");
    Path destDirName = new Path("/metricsTestDirectory_RenameFinal");
    
    // Create an empty directory
    assertTrue(fs.mkdirs(originalDirName));
    base = getCurrentWebResponses();
    
    // Create an inner file
    assertTrue(fs.createNewFile(innerFileName));
    base = getCurrentWebResponses();
    
    // Rename the directory
    assertTrue(fs.rename(originalDirName, destDirName));
    // At the time of writing this code it takes 11 requests/responses
    // to rename the directory with one file. Check for range 1-20 for now.
    logOpResponseCount("Renaming a directory", base);
    base = assertWebResponsesInRange(base, 1, 20);

    assertNoErrors();
  }

  @Test
  public void testClientErrorMetrics() throws Exception {
    String directoryName = "metricsTestDirectory_ClientError";
    Path directoryPath = new Path("/" + directoryName);
    assertTrue(fs.mkdirs(directoryPath));
    String leaseID = testAccount.acquireShortLease(directoryName);
    try {
      try {
        fs.delete(directoryPath, true);
        assertTrue("Should've thrown.", false);
      } catch (AzureException ex) {
        assertTrue("Unexpected exception: " + ex,
            ex.getMessage().contains("lease"));
      }
      assertEquals(1, AzureMetricsTestUtil.getLongCounterValue(getInstrumentation(), WASB_CLIENT_ERRORS));
      assertEquals(0, AzureMetricsTestUtil.getLongCounterValue(getInstrumentation(), WASB_SERVER_ERRORS));
    } finally {
      testAccount.releaseLease(leaseID, directoryName);
    }
  }

  private void logOpResponseCount(String opName, long base) {
    System.out.println(opName + " took " + (getCurrentWebResponses() - base) +
        " web responses to complete.");
  }

  /**
   * Gets (and asserts) the value of the wasb_web_responses counter just
   * after the creation of the file system object.
   */
  private long getBaseWebResponses() {
    // The number of requests should start at 0
    return assertWebResponsesEquals(0, 0);
  }

  /**
   * Gets the current value of the wasb_web_responses counter.
   */
  private long getCurrentWebResponses() {
	    return AzureMetricsTestUtil.getCurrentWebResponses(getInstrumentation());
  }

  /**
   * Checks that the wasb_web_responses counter is at the given value.
   * @param base The base value (before the operation of interest).
   * @param expected The expected value for the operation of interest.
   * @return The new base value now.
   */
  private long assertWebResponsesEquals(long base, long expected) {
    assertCounter(WASB_WEB_RESPONSES, base + expected, getMyMetrics());
    return base + expected;
  }

  private void assertNoErrors() {
    assertEquals(0, AzureMetricsTestUtil.getLongCounterValue(getInstrumentation(), WASB_CLIENT_ERRORS));
    assertEquals(0, AzureMetricsTestUtil.getLongCounterValue(getInstrumentation(), WASB_SERVER_ERRORS));
  }

  /**
   * Checks that the wasb_web_responses counter is in the given range.
   * @param base The base value (before the operation of interest).
   * @param inclusiveLowerLimit The lower limit for what it should increase by.
   * @param inclusiveUpperLimit The upper limit for what it should increase by.
   * @return The new base value now.
   */
  private long assertWebResponsesInRange(long base,
      long inclusiveLowerLimit,
      long inclusiveUpperLimit) {
    long currentResponses = getCurrentWebResponses();
    long justOperation = currentResponses - base;
    assertTrue(String.format(
        "Web responses expected in range [%d, %d], but was %d.",
        inclusiveLowerLimit, inclusiveUpperLimit, justOperation),
        justOperation >= inclusiveLowerLimit &&
        justOperation <= inclusiveUpperLimit);
    return currentResponses;
  }  

  /**
   * Gets the metrics for the file system object.
   * @return The metrics record.
   */
  private MetricsRecordBuilder getMyMetrics() {
    return getMetrics(getInstrumentation());
  }

  private AzureFileSystemInstrumentation getInstrumentation() {
    return ((NativeAzureFileSystem)fs).getInstrumentation();
  }

  /**
   * A matcher class for asserting that we got a tag with a given
   * value.
   */
  private static class TagMatcher extends TagExistsMatcher {
    private final String tagValue;
    
    public TagMatcher(String tagName, String tagValue) {
      super(tagName);
      this.tagValue = tagValue;
    }

    @Override
    public boolean matches(MetricsTag toMatch) {
      return toMatch.value().equals(tagValue);
    }

    @Override
    public void describeTo(Description desc) {
      super.describeTo(desc);
      desc.appendText(" with value " + tagValue);
    }
  }

  /**
   * A matcher class for asserting that we got a tag with any value.
   */
  private static class TagExistsMatcher extends BaseMatcher<MetricsTag> {
    private final String tagName;
    
    public TagExistsMatcher(String tagName) {
      this.tagName = tagName;
    }

    @Override
    public boolean matches(Object toMatch) {
      MetricsTag asTag = (MetricsTag)toMatch;
      return asTag.name().equals(tagName) && matches(asTag);
    }
    
    protected boolean matches(MetricsTag toMatch) {
      return true;
    }

    @Override
    public void describeTo(Description desc) {
      desc.appendText("Has tag " + tagName);
    }
  }
  
}
