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
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.apache.hadoop.fs.adl.AdlConfKeys.ADL_BLOCK_SIZE;

/**
 * This class is responsible for testing local getFileStatus implementation
 * to cover correct parsing of successful and error JSON response
 * from the server.
 * Adls GetFileStatus operation is in detail covered in
 * org.apache.hadoop.fs.adl.live testing package.
 */
public class TestGetFileStatus extends AdlMockWebServer {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestGetFileStatus.class);

  @Test
  public void getFileStatusReturnsAsExpected()
      throws URISyntaxException, IOException {
    getMockServer().enqueue(new MockResponse().setResponseCode(200)
        .setBody(TestADLResponseData.getGetFileStatusJSONResponse()));
    long startTime = Time.monotonicNow();
    Path path = new Path("/test1/test2");
    FileStatus fileStatus = getMockAdlFileSystem().getFileStatus(path);
    long endTime = Time.monotonicNow();
    LOG.debug("Time : " + (endTime - startTime));
    Assert.assertTrue(fileStatus.isFile());
    Assert.assertEquals("adl://" + getMockServer().getHostName() + ":" +
        getMockServer().getPort() + "/test1/test2",
        fileStatus.getPath().toString());
    Assert.assertEquals(4194304, fileStatus.getLen());
    Assert.assertEquals(ADL_BLOCK_SIZE, fileStatus.getBlockSize());
    Assert.assertEquals(1, fileStatus.getReplication());
    Assert.assertEquals(new FsPermission("777"), fileStatus.getPermission());
    Assert.assertEquals("NotSupportYet", fileStatus.getOwner());
    Assert.assertEquals("NotSupportYet", fileStatus.getGroup());
    Assert.assertTrue(path + " should have Acl!", fileStatus.hasAcl());
    Assert.assertFalse(path + " should not be encrypted!",
        fileStatus.isEncrypted());
    Assert.assertFalse(path + " should not be erasure coded!",
        fileStatus.isErasureCoded());
  }

  @Test
  public void getFileStatusAclBit() throws URISyntaxException, IOException {
    // With ACLBIT set to true
    getMockServer().enqueue(new MockResponse().setResponseCode(200)
            .setBody(TestADLResponseData.getGetFileStatusJSONResponse(true)));
    long startTime = Time.monotonicNow();
    FileStatus fileStatus = getMockAdlFileSystem()
            .getFileStatus(new Path("/test1/test2"));
    long endTime = Time.monotonicNow();
    LOG.debug("Time : " + (endTime - startTime));
    Assert.assertTrue(fileStatus.isFile());
    Assert.assertTrue(fileStatus.hasAcl());
    Assert.assertTrue(fileStatus.getPermission().getAclBit());

    // With ACLBIT set to false
    getMockServer().enqueue(new MockResponse().setResponseCode(200)
            .setBody(TestADLResponseData.getGetFileStatusJSONResponse(false)));
    startTime = Time.monotonicNow();
    fileStatus = getMockAdlFileSystem()
            .getFileStatus(new Path("/test1/test2"));
    endTime = Time.monotonicNow();
    LOG.debug("Time : " + (endTime - startTime));
    Assert.assertTrue(fileStatus.isFile());
    Assert.assertFalse(fileStatus.hasAcl());
    Assert.assertFalse(fileStatus.getPermission().getAclBit());
  }

}
