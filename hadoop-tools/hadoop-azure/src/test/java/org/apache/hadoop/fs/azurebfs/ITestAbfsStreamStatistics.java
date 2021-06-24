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

package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.storage.fastpath.MockFastpathConnection;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.azurebfs.services.MockAbfsInputStream;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READ_AHEAD_QUEUE_DEPTH;

/**
 * Test Abfs Stream.
 */

public class ITestAbfsStreamStatistics extends AbstractAbfsIntegrationTest {
  public ITestAbfsStreamStatistics() throws Exception {
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestAbfsStreamStatistics.class);

  private static final int LARGE_NUMBER_OF_OPS = 999999;

  /***
   * Testing {@code incrementReadOps()} in class {@code AbfsInputStream} and
   * {@code incrementWriteOps()} in class {@code AbfsOutputStream}.
   *
   * @throws Exception
   */
  @Test
  public void testMockFastpathAbfsStreamOps() throws Exception {
    // Run mock test only if feature is set to off
    Assume.assumeFalse(getDefaultFastpathFeatureStatus());
    Path smallOperationsFile = new Path("testOneReadWriteOps_mock");
    Path largeOperationsFile = new Path("testLargeReadWriteOps_mock");
    testAbfsStreamOps(smallOperationsFile, largeOperationsFile, true);
  }

  @Test
  public void testAbfsStreamOps() throws Exception {
    Path smallOperationsFile = new Path("testOneReadWriteOps");
    Path largeOperationsFile = new Path("testLargeReadWriteOps");
    testAbfsStreamOps(smallOperationsFile, largeOperationsFile, false);
  }

  public void testAbfsStreamOps(Path smallOperationsFile,
      Path largeOperationsFile, boolean isMockFastpathTest) throws Exception {
    describe("Test to see correct population of read and write operations in "
        + "Abfs");

    Configuration configuration = getRawConfiguration();
    configuration.set(FS_AZURE_READ_AHEAD_QUEUE_DEPTH, "0");
    final AzureBlobFileSystem fs =
        (AzureBlobFileSystem) FileSystem.newInstance(configuration);//getFileSystem();
    FileSystem.Statistics statistics = fs.getFsStatistics();
    String testReadWriteOps = "test this";
    statistics.reset();

    //Test for zero write operation
    assertReadWriteOps("write", 0, statistics.getWriteOps());

    //Test for zero read operation
    assertReadWriteOps("read", 0, statistics.getReadOps());

    FSDataOutputStream outForOneOperation = null;
    FSDataInputStream inForOneOperation = null;
    try {
      outForOneOperation = fs.create(smallOperationsFile);
      statistics.reset();
      outForOneOperation.write(testReadWriteOps.getBytes());

      //Test for a single write operation
      assertReadWriteOps("write", 1, statistics.getWriteOps());

      //Flushing output stream to see content to read
      outForOneOperation.hflush();
      byte[] buff = testReadWriteOps.getBytes();
      if (isMockFastpathTest) {
        MockFastpathConnection
            .registerAppend(buff.length, smallOperationsFile.getName(), buff, 0,
                buff.length);
      }
      inForOneOperation = isMockFastpathTest
          ? openMockAbfsInputStream(fs, smallOperationsFile)
          : fs.open(smallOperationsFile);
      statistics.reset();
      int result = inForOneOperation.read(testReadWriteOps.getBytes(), 0,
          testReadWriteOps.getBytes().length);

      LOG.info("Result of Read operation : {}", result);
      /*
       * Testing if 2 read_ops value is coming after reading full content
       * from a file (3 if anything to read from Buffer too). Reason: read()
       * call gives read_ops=1, reading from AbfsClient(http GET) gives
       * read_ops=2.
       *
       * In some cases ABFS-prefetch thread runs in the background which
       * returns some bytes from buffer and gives an extra readOp.
       * Thus, making readOps values arbitrary and giving intermittent
       * failures in some cases. Hence, readOps values of 2 or 3 is seen in
       * different setups.
       *
       */
      assertTrue(String.format("The actual value of %d was not equal to the "
              + "expected value of 2 or 3", statistics.getReadOps()),
          statistics.getReadOps() == 2 || statistics.getReadOps() == 3);

    } finally {
      IOUtils.cleanupWithLogger(LOG, inForOneOperation,
          outForOneOperation);
    }

    //Validating if content is being written in the smallOperationsFile
    if (isMockFastpathTest) {
      assertTrue("Mismatch in content validation",
          validateContent((MockAbfsInputStream) getMockAbfsInputStream(fs, smallOperationsFile),
              testReadWriteOps.getBytes()));
    } else {
      assertTrue("Mismatch in content validation",
          validateContent(fs, smallOperationsFile,
              testReadWriteOps.getBytes()));
    }

    FSDataOutputStream outForLargeOperations = null;
    FSDataInputStream inForLargeOperations = null;
    StringBuilder largeOperationsValidationString = new StringBuilder();
    try {
      outForLargeOperations = fs.create(largeOperationsFile);
      statistics.reset();
      int largeValue = LARGE_NUMBER_OF_OPS;
      byte[] buff = testReadWriteOps.getBytes();
      int totalLen = largeValue*buff.length;
      for (int i = 0; i < largeValue; i++) {
        outForLargeOperations.write(testReadWriteOps.getBytes());
        if (isMockFastpathTest) {
          MockFastpathConnection
              .registerAppend(totalLen, largeOperationsFile.getName(), buff, 0,
                  buff.length);
        }

        //Creating the String for content Validation
        largeOperationsValidationString.append(testReadWriteOps);
      }
      LOG.info("Number of bytes of Large data written: {}",
          largeOperationsValidationString.toString().getBytes().length);

      //Test for 1000000 write operations
      assertReadWriteOps("write", largeValue, statistics.getWriteOps());

      inForLargeOperations = isMockFastpathTest
          ? openMockAbfsInputStream(fs, largeOperationsFile)
          : fs.open(largeOperationsFile);
      for (int i = 0; i < largeValue; i++) {
        inForLargeOperations
            .read(testReadWriteOps.getBytes(), 0,
                testReadWriteOps.getBytes().length);
      }

      if (fs.getAbfsStore().isAppendBlobKey(fs.makeQualified(largeOperationsFile).toString())) {
        // for appendblob data is already flushed, so there is more data to read.
        assertTrue(String.format("The actual value of %d was not equal to the "
              + "expected value", statistics.getReadOps()),
          statistics.getReadOps() == (largeValue + 3) || statistics.getReadOps() == (largeValue + 4));
      } else {
        //Test for 1000000 read operations
        assertReadWriteOps("read", largeValue, statistics.getReadOps());
      }

    } finally {
      IOUtils.cleanupWithLogger(LOG, inForLargeOperations,
          outForLargeOperations);
    }

    //Validating if content is being written in largeOperationsFile
    if (isMockFastpathTest) {
      assertTrue("Mismatch in content validation",
          validateContent((MockAbfsInputStream) getMockAbfsInputStream(fs, largeOperationsFile),
              largeOperationsValidationString.toString().getBytes()));
    } else {
      assertTrue("Mismatch in content validation",
          validateContent(fs, largeOperationsFile,
              largeOperationsValidationString.toString().getBytes()));
    }

    if (isMockFastpathTest) {
      MockFastpathConnection.unregisterAppend(smallOperationsFile.getName());
      MockFastpathConnection.unregisterAppend(largeOperationsFile.getName());
    }
  }

  /**
   * Generic method to assert both Read an write operations.
   *
   * @param operation     what operation is being asserted
   * @param expectedValue value which is expected
   * @param actualValue   value which is actual
   */

  private void assertReadWriteOps(String operation, long expectedValue,
      long actualValue) {
    assertEquals("Mismatch in " + operation + " operations", expectedValue,
        actualValue);
  }
}
