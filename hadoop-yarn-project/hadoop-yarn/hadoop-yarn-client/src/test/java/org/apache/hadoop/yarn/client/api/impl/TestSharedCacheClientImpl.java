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

package org.apache.hadoop.yarn.client.api.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ClientSCMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.ReleaseSharedCacheResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UseSharedCacheResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UseSharedCacheResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.UseSharedCacheResourceResponsePBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSharedCacheClientImpl {

  private static final Log LOG = LogFactory
      .getLog(TestSharedCacheClientImpl.class);

  public static SharedCacheClientImpl client;
  public static ClientSCMProtocol cProtocol;
  private static Path TEST_ROOT_DIR;
  private static FileSystem localFs;
  private static String input = "This is a test file.";
  private static String inputChecksumSHA256 =
      "f29bc64a9d3732b4b9035125fdb3285f5b6455778edca72414671e0ca3b2e0de";

  @BeforeClass
  public static void beforeClass() throws IOException {
    localFs = FileSystem.getLocal(new Configuration());
    TEST_ROOT_DIR =
        new Path("target", TestSharedCacheClientImpl.class.getName()
            + "-tmpDir").makeQualified(localFs.getUri(),
            localFs.getWorkingDirectory());
  }

  @AfterClass
  public static void afterClass() {
    try {
      if (localFs != null) {
        localFs.close();
      }
    } catch (IOException ioe) {
      LOG.info("IO exception in closing file system)");
      ioe.printStackTrace();
    }
  }

  @Before
  public void setup() {
    cProtocol = mock(ClientSCMProtocol.class);
    client = new SharedCacheClientImpl() {
      @Override
      protected ClientSCMProtocol createClientProxy() {
        return cProtocol;
      }

      @Override
      protected void stopClientProxy() {
        // do nothing because it is mocked
      }
    };
    client.init(new Configuration());
    client.start();
  }

  @After
  public void cleanup() {
    if (client != null) {
      client.stop();
      client = null;
    }
  }

  @Test
  public void testUseCacheMiss() throws Exception {
    UseSharedCacheResourceResponse response =
        new UseSharedCacheResourceResponsePBImpl();
    response.setPath(null);
    when(cProtocol.use(isA(UseSharedCacheResourceRequest.class))).thenReturn(
        response);
    URL newURL = client.use(mock(ApplicationId.class), "key");
    assertNull("The path is not null!", newURL);
  }

  @Test
  public void testUseCacheHit() throws Exception {
    Path file = new Path("viewfs://test/path");
    URL useUrl = URL.fromPath(new Path("viewfs://test/path"));
    UseSharedCacheResourceResponse response =
        new UseSharedCacheResourceResponsePBImpl();
    response.setPath(file.toString());
    when(cProtocol.use(isA(UseSharedCacheResourceRequest.class))).thenReturn(
        response);
    URL newURL = client.use(mock(ApplicationId.class), "key");
    assertEquals("The paths are not equal!", useUrl, newURL);
  }

  @Test(expected = YarnException.class)
  public void testUseError() throws Exception {
    String message = "Mock IOExcepiton!";
    when(cProtocol.use(isA(UseSharedCacheResourceRequest.class))).thenThrow(
        new IOException(message));
    client.use(mock(ApplicationId.class), "key");
  }

  @Test
  public void testRelease() throws Exception {
    // Release does not care about the return value because it is empty
    when(cProtocol.release(isA(ReleaseSharedCacheResourceRequest.class)))
        .thenReturn(null);
    client.release(mock(ApplicationId.class), "key");
  }

  @Test(expected = YarnException.class)
  public void testReleaseError() throws Exception {
    String message = "Mock IOExcepiton!";
    when(cProtocol.release(isA(ReleaseSharedCacheResourceRequest.class)))
        .thenThrow(new IOException(message));
    client.release(mock(ApplicationId.class), "key");
  }

  @Test
  public void testChecksum() throws Exception {
    String filename = "test1.txt";
    Path file = makeFile(filename);
    assertEquals(inputChecksumSHA256, client.getFileChecksum(file));
  }

  @Test(expected = FileNotFoundException.class)
  public void testNonexistantFileChecksum() throws Exception {
    Path file = new Path(TEST_ROOT_DIR, "non-existant-file");
    client.getFileChecksum(file);
  }

  private Path makeFile(String filename) throws Exception {
    Path file = new Path(TEST_ROOT_DIR, filename);
    DataOutputStream out = null;
    try {
      out = localFs.create(file);
      out.write(input.getBytes("UTF-8"));
    } finally {
      if(out != null) {
        out.close();
      }
    }
    return file;
  }
}
