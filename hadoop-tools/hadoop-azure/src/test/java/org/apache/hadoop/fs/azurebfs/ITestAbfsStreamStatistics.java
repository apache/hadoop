/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;

/**
 * Test Abfs Stream.
 */

public class ITestAbfsStreamStatistics extends AbstractAbfsIntegrationTest {
  public ITestAbfsStreamStatistics() throws Exception {
  }

  /***
   * {@link AbfsInputStream#incrementReadOps()}.
   *
   * @throws Exception
   */
  @Test
  public void testAbfsStreamOps() throws Exception {
    describe("Test to see correct population of read and write operations in "
        + "Abfs");

    final AzureBlobFileSystem fs = getFileSystem();
    Path smallOperaionsFile = new Path("testOneReadWriteOps");
    Path largeOperationsFile = new Path("testLargeReadWriteOps");
    FileSystem.Statistics statistics = fs.getFsStatistics();
    String testReadWriteOps = "test this";
    statistics.reset();

    //Test for zero read and write operation
    Assert.assertEquals("Zero read operations", 0, statistics.getReadOps());
    Assert.assertEquals("Zero write operations", 0, statistics.getWriteOps());

    FSDataOutputStream outForOneOperation = fs.create(smallOperaionsFile);
    statistics.reset();
    outForOneOperation.write(testReadWriteOps.getBytes());
    FSDataInputStream inForOneCall = fs.open(smallOperaionsFile);
    inForOneCall.read(testReadWriteOps.getBytes(), 0,
        testReadWriteOps.getBytes().length);

    //Test for one read and write operation
    Assert.assertEquals("one read operation is performed", 1,
        statistics.getReadOps());
    Assert.assertEquals("one write operation is performed", 1,
        statistics.getWriteOps());

    outForOneOperation.close();
    //validating Content of file
    Assert.assertEquals("one operation Content validation", true,
        validateContent(fs, smallOperaionsFile,
            testReadWriteOps.getBytes()));

    FSDataOutputStream outForLargeOperations = fs.create(largeOperationsFile);
    statistics.reset();

    StringBuilder largeOperationsValidationString = new StringBuilder();
    for (int i = 0; i < 1000000; i++) {
      outForLargeOperations.write(testReadWriteOps.getBytes());

      //Creating the String for content Validation
      largeOperationsValidationString.append(testReadWriteOps);
    }

    FSDataInputStream inForLargeCalls = fs.open(largeOperationsFile);

    for (int i = 0; i < 1000000; i++)
      inForLargeCalls
          .read(testReadWriteOps.getBytes(), 0,
              testReadWriteOps.getBytes().length);

    //Test for one million read and write operations
    Assert.assertEquals("Large read operations", 1000000,
        statistics.getReadOps());
    Assert.assertEquals("Large write operations", 1000000,
        statistics.getWriteOps());

    outForLargeOperations.close();
    //Validating if actually "test" is being written million times in largeOperationsFile
    Assert.assertEquals("Large File content validation", true,
        validateContent(fs, largeOperationsFile,
            largeOperationsValidationString.toString().getBytes()));

  }
}
