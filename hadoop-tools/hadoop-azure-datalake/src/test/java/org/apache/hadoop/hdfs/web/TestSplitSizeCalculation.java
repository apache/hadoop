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

package org.apache.hadoop.hdfs.web;

import com.squareup.okhttp.mockwebserver.MockResponse;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.adl.TestADLResponseData;
import org.apache.hadoop.fs.common.AdlMockWebServer;
import org.apache.hadoop.hdfs.web.PrivateAzureDataLakeFileSystem.BatchByteArrayInputStream;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * This class is responsible for testing split size calculation during
 * read ahead buffer initiation based on the data size and configuration
 * initialization.
 */
public class TestSplitSizeCalculation extends AdlMockWebServer {

  @Test
  public void testSplitSizeCalculations()
      throws URISyntaxException, IOException {

    getMockServer().enqueue(new MockResponse().setResponseCode(200).setBody(
        TestADLResponseData.getGetFileStatusJSONResponse(128 * 1024 * 1024)));
    getMockServer().enqueue(new MockResponse().setResponseCode(200).setBody(
        TestADLResponseData.getGetFileStatusJSONResponse(128 * 1024 * 1024)));
    getMockServer().enqueue(new MockResponse().setResponseCode(200).setBody(
        TestADLResponseData.getGetFileStatusJSONResponse(128 * 1024 * 1024)));
    getMockServer().enqueue(new MockResponse().setResponseCode(200).setBody(
        TestADLResponseData.getGetFileStatusJSONResponse(128 * 1024 * 1024)));

    URL url = getMockServer().getUrl("");

    BatchByteArrayInputStream stream = getMockAdlFileSystem()
        .new BatchByteArrayInputStream(url,
        new Path("/test1/test2"), 16 * 1024 * 1024, 4);
    Assert.assertEquals(1, stream.getSplitSize(1 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(2 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(3 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(4 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(5 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(6 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(7 * 1024 * 1024));
    Assert.assertEquals(2, stream.getSplitSize(8 * 1024 * 1024));
    Assert.assertEquals(4, stream.getSplitSize(16 * 1024 * 1024));
    Assert.assertEquals(3, stream.getSplitSize(12 * 1024 * 1024));
    Assert.assertEquals(4, stream.getSplitSize(102 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(102));
    stream.close();

    stream = getMockAdlFileSystem().new BatchByteArrayInputStream(url,
        new Path("/test1/test2"), 4 * 1024 * 1024, 4);
    Assert.assertEquals(1, stream.getSplitSize(1 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(2 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(3 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(4 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(5 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(8 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(5 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(6 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(7 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(16 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(12 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(102 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(102));
    stream.close();

    stream = getMockAdlFileSystem().new BatchByteArrayInputStream(url,
        new Path("/test1/test2"), 16 * 1024 * 1024, 2);
    Assert.assertEquals(1, stream.getSplitSize(1 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(2 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(3 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(4 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(5 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(5 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(6 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(7 * 1024 * 1024));
    Assert.assertEquals(2, stream.getSplitSize(8 * 1024 * 1024));
    Assert.assertEquals(2, stream.getSplitSize(16 * 1024 * 1024));
    Assert.assertEquals(2, stream.getSplitSize(12 * 1024 * 1024));
    Assert.assertEquals(2, stream.getSplitSize(102 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(102));
    stream.close();

    stream = getMockAdlFileSystem().new BatchByteArrayInputStream(url,
        new Path("/test1/test2"), 8 * 1024 * 1024, 2);
    Assert.assertEquals(1, stream.getSplitSize(1 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(2 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(3 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(4 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(5 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(6 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(7 * 1024 * 1024));
    Assert.assertEquals(2, stream.getSplitSize(8 * 1024 * 1024));
    Assert.assertEquals(2, stream.getSplitSize(16 * 1024 * 1024));
    Assert.assertEquals(2, stream.getSplitSize(12 * 1024 * 1024));
    Assert.assertEquals(2, stream.getSplitSize(102 * 1024 * 1024));
    Assert.assertEquals(1, stream.getSplitSize(102));
    stream.close();
  }
}
