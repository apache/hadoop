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

package org.apache.hadoop.fs.adl;

import com.squareup.okhttp.mockwebserver.MockResponse;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.common.AdlMockWebServer;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * This class is responsible for testing local listStatus implementation to
 * cover correct parsing of successful and error JSON response from the server.
 * Adls ListStatus functionality is in detail covered in
 * org.apache.hadoop.fs.adl.live testing package.
 */
public class TestListStatus extends AdlMockWebServer {

  @Test
  public void listStatusReturnsAsExpected() throws IOException {
    getMockServer().enqueue(new MockResponse().setResponseCode(200)
        .setBody(TestADLResponseData.getListFileStatusJSONResponse(10)));
    long startTime = Time.monotonicNow();
    FileStatus[] ls = getMockAdlFileSystem().listStatus(
        new Path("/test1/test2"));
    long endTime = Time.monotonicNow();
    System.out.println("Time : " + (endTime - startTime));
    Assert.assertEquals(ls.length, 10);

    getMockServer().enqueue(new MockResponse().setResponseCode(200)
        .setBody(TestADLResponseData.getListFileStatusJSONResponse(200)));
    startTime = Time.monotonicNow();
    ls = getMockAdlFileSystem().listStatus(new Path("/test1/test2"));
    endTime = Time.monotonicNow();
    System.out.println("Time : " + (endTime - startTime));
    Assert.assertEquals(ls.length, 200);

    getMockServer().enqueue(new MockResponse().setResponseCode(200)
        .setBody(TestADLResponseData.getListFileStatusJSONResponse(2048)));
    startTime = Time.monotonicNow();
    ls = getMockAdlFileSystem().listStatus(new Path("/test1/test2"));
    endTime = Time.monotonicNow();
    System.out.println("Time : " + (endTime - startTime));
    Assert.assertEquals(ls.length, 2048);
  }

  @Test
  public void listStatusonFailure() throws IOException {
    getMockServer().enqueue(new MockResponse().setResponseCode(403).setBody(
        TestADLResponseData.getErrorIllegalArgumentExceptionJSONResponse()));
    FileStatus[] ls = null;
    long startTime = Time.monotonicNow();
    try {
      ls = getMockAdlFileSystem().listStatus(new Path("/test1/test2"));
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("Bad Offset 0x83090015"));
    }
    long endTime = Time.monotonicNow();
    System.out.println("Time : " + (endTime - startTime));

    getMockServer().enqueue(new MockResponse().setResponseCode(500)
        .setBody(
            TestADLResponseData.getErrorInternalServerExceptionJSONResponse()));
    startTime = Time.monotonicNow();
    try {
      ls = getMockAdlFileSystem().listStatus(new Path("/test1/test2"));
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("Internal Server Error"));
    }
    endTime = Time.monotonicNow();
    System.out.println("Time : " + (endTime - startTime));
  }

}
