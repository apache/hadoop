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
 * Test Abfs Input Stream.
 */

public class ITestAbfsInputStream extends AbstractAbfsIntegrationTest {
  public ITestAbfsInputStream() throws Exception {
  }

  /***
   * {@link AbfsInputStream#incrementReadOps()}
   *
   * @throws Exception
   */
  @Test
  public void testAbfsInputStreamReadOps() throws Exception {
    describe("Test to see correct population of Read operations in Abfs");

    final AzureBlobFileSystem fs = getFileSystem();
    Path smallFile = new Path("testOneReadCall");
    Path largeFile = new Path("testLargeReadCalls");
    FileSystem.Statistics statistics = fs.getFsStatistics();
    String testReadOps = "test this";
    statistics.reset();

    //Test for zero read operation
    Assert.assertEquals(0, statistics.getReadOps());

    FSDataOutputStream outForOneCall = fs.create(smallFile);
    statistics.reset();
    outForOneCall.write(testReadOps.getBytes());
    FSDataInputStream inForOneCall = fs.open(smallFile);
    inForOneCall.read(testReadOps.getBytes(), 0, testReadOps.getBytes().length);

    //Test for one read operation
    Assert.assertEquals(1, statistics.getReadOps());

    FSDataOutputStream outForLargeCalls = fs.create(largeFile);
    statistics.reset();
    outForLargeCalls.write(testReadOps.getBytes());
    FSDataInputStream inForLargeCalls = fs.open(largeFile);

    for (int i = 0; i < 1000; i++)
      inForLargeCalls
          .read(testReadOps.getBytes(), 0, testReadOps.getBytes().length);

    //Test for thousand read operations
    Assert.assertEquals(1000, statistics.getReadOps());
    statistics.reset();

  }

}
