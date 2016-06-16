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
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * This class is responsible for testing local getFileStatus implementation
 * to cover correct parsing of successful and error JSON response
 * from the server.
 * Adls GetFileStatus operation is in detail covered in
 * org.apache.hadoop.fs.adl.live testing package.
 */
public class TestGetFileStatus extends AdlMockWebServer {

  @Test
  public void getFileStatusReturnsAsExpected()
      throws URISyntaxException, IOException {
    getMockServer().enqueue(new MockResponse().setResponseCode(200)
        .setBody(TestADLResponseData.getGetFileStatusJSONResponse()));
    long startTime = Time.monotonicNow();
    FileStatus fileStatus = getMockAdlFileSystem().getFileStatus(
        new Path("/test1/test2"));
    long endTime = Time.monotonicNow();
    System.out.println("Time : " + (endTime - startTime));
    Assert.assertTrue(fileStatus.isFile());
    Assert.assertEquals(fileStatus.getPath().toString(),
        "adl://" + getMockServer().getHostName() + ":"
            + getMockServer().getPort()
            + "/test1/test2");
    Assert.assertEquals(fileStatus.getLen(), 4194304);
    Assert.assertEquals(fileStatus.getBlockSize(), 268435456);
    Assert.assertEquals(fileStatus.getReplication(), 0);
    Assert.assertEquals(fileStatus.getPermission(), new FsPermission("777"));
    Assert.assertEquals(fileStatus.getOwner(), "NotSupportYet");
    Assert.assertEquals(fileStatus.getGroup(), "NotSupportYet");
  }
}
