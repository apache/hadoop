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

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * Test Abfs Stream.
 */

public class ITestAbfsStreamStatistics extends AbstractAbfsIntegrationTest {
  public ITestAbfsStreamStatistics() throws Exception {
  }

  private static int LARGE_NUMBER_OF_OPS = 1000000;

  /***
   * Testing {@code incrementReadOps()} in class {@code AbfsInputStream} and
   * {@code incrementWriteOps()} in class {@code AbfsOutputStream}.
   *
   * @throws Exception
   */
  @Test
  public void testAbfsStreamOps() throws Exception {
    describe("Test to see correct population of read and write operations in "
        + "Abfs");

    final AzureBlobFileSystem fs = getFileSystem();
    Path smallOperationsFile = new Path("testOneReadWriteOps");
    Path largeOperationsFile = new Path("testLargeReadWriteOps");
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

      inForOneOperation = fs.open(smallOperationsFile);
      inForOneOperation.read(testReadWriteOps.getBytes(), 0,
          testReadWriteOps.getBytes().length);

      //Test for a single read operation
      assertReadWriteOps("read", 1, statistics.getReadOps());

    } finally {
      IOUtils.cleanupWithLogger(null, inForOneOperation,
          outForOneOperation);
    }

    //Validating if content is being written in the smallOperationsFile
    Assert.assertEquals("Mismatch in content validation", true,
        validateContent(fs, smallOperationsFile,
            testReadWriteOps.getBytes()));

    FSDataOutputStream outForLargeOperations = null;
    FSDataInputStream inForLargeOperations = null;
    StringBuilder largeOperationsValidationString = new StringBuilder();
    try {
      outForLargeOperations = fs.create(largeOperationsFile);
      statistics.reset();
      int largeValue = LARGE_NUMBER_OF_OPS;
      for (int i = 0; i < largeValue; i++) {
        outForLargeOperations.write(testReadWriteOps.getBytes());

        //Creating the String for content Validation
        largeOperationsValidationString.append(testReadWriteOps);
      }

      //Test for 1000000 write operations
      assertReadWriteOps("write", largeValue, statistics.getWriteOps());

      inForLargeOperations = fs.open(largeOperationsFile);
      for (int i = 0; i < largeValue; i++) {
        inForLargeOperations
            .read(testReadWriteOps.getBytes(), 0,
                testReadWriteOps.getBytes().length);
      }

      //Test for 1000000 read operations
      assertReadWriteOps("read", largeValue, statistics.getReadOps());

    } finally {
      IOUtils.cleanupWithLogger(null, inForLargeOperations,
          outForLargeOperations);
    }

    //Validating if content is being written in largeOperationsFile
    Assert.assertTrue("Mismatch in content validation",
        validateContent(fs, largeOperationsFile,
            largeOperationsValidationString.toString().getBytes()));

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
    Assert
        .assertEquals("Mismatch in " + operation + " operations", expectedValue,
            actualValue);
  }
}
