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
 *
 */

package org.apache.hadoop.fs.adl.live;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Random;

/**
 * Verify different data segment size writes ensure the integrity and
 * order of the data.
 */
public class TestAdlDifferentSizeWritesLive {

  public static byte[] getRandomByteArrayData(int size) {
    byte[] b = new byte[size];
    Random rand = new Random();
    rand.nextBytes(b);
    return b;
  }

  @Before
  public void setup() throws Exception {
    org.junit.Assume
        .assumeTrue(AdlStorageConfiguration.isContractTestEnabled());
  }

  @Test
  public void testSmallDataWrites() throws IOException {
    testDataIntegrity(4 * 1024 * 1024, 1 * 1024);
    testDataIntegrity(4 * 1024 * 1024, 7 * 1024);
    testDataIntegrity(4 * 1024 * 1024, 10);
    testDataIntegrity(2 * 1024 * 1024, 10);
    testDataIntegrity(1 * 1024 * 1024, 10);
    testDataIntegrity(100, 1);
  }

  @Test
  public void testMediumDataWrites() throws IOException {
    testDataIntegrity(4 * 1024 * 1024, 1 * 1024 * 1024);
    testDataIntegrity(7 * 1024 * 1024, 2 * 1024 * 1024);
    testDataIntegrity(9 * 1024 * 1024, 2 * 1024 * 1024);
    testDataIntegrity(10 * 1024 * 1024, 3 * 1024 * 1024);
  }

  private void testDataIntegrity(int totalSize, int chunkSize)
      throws IOException {
    Path path = new Path("/test/dataIntegrityCheck");
    FileSystem fs = null;
    try {
      fs = AdlStorageConfiguration.createAdlStorageConnector();
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Can not initialize ADL FileSystem. "
          + "Please check fs.defaultFS property.", e);
    }
    byte[] expectedData = getRandomByteArrayData(totalSize);

    FSDataOutputStream out = fs.create(path, true);
    int iteration = totalSize / chunkSize;
    int reminderIteration = totalSize % chunkSize;
    int offset = 0;
    for (int i = 0; i < iteration; ++i) {
      out.write(expectedData, offset, chunkSize);
      offset += chunkSize;
    }

    out.write(expectedData, offset, reminderIteration);
    out.close();

    byte[] actualData = new byte[totalSize];
    FSDataInputStream in = fs.open(path);
    in.readFully(0, actualData);
    in.close();
    Assert.assertArrayEquals(expectedData, actualData);
    Assert.assertTrue(fs.delete(path, true));
  }
}
