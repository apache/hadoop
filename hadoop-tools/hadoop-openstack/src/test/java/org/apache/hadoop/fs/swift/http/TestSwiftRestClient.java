/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.swift.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.SwiftTestConstants;
import org.apache.hadoop.fs.swift.util.Duration;
import org.apache.hadoop.fs.swift.util.DurationStats;
import org.apache.hadoop.fs.swift.util.SwiftObjectPath;
import org.apache.hadoop.fs.swift.util.SwiftTestUtils;
import org.apache.http.Header;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

public class TestSwiftRestClient implements SwiftTestConstants {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSwiftRestClient.class);

  private Configuration conf;
  private boolean runTests;
  private URI serviceURI;

  @Before
  public void setup() throws IOException {
    conf = new Configuration();
    runTests = SwiftTestUtils.hasServiceURI(conf);
    if (runTests) {
      serviceURI = SwiftTestUtils.getServiceURI(conf);
    }
  }

  protected void assumeEnabled() {
    Assume.assumeTrue(runTests);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testCreate() throws Throwable {
    assumeEnabled();
    SwiftRestClient client = createClient();
  }

  private SwiftRestClient createClient() throws IOException {
    return SwiftRestClient.getInstance(serviceURI, conf);
  }


  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testAuthenticate() throws Throwable {
    assumeEnabled();
    SwiftRestClient client = createClient();
    client.authenticate();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testPutAndDelete() throws Throwable {
    assumeEnabled();
    SwiftRestClient client = createClient();
    client.authenticate();
    Path path = new Path("restTestPutAndDelete");
    SwiftObjectPath sobject = SwiftObjectPath.fromPath(serviceURI, path);
    byte[] stuff = new byte[1];
    stuff[0] = 'a';
    client.upload(sobject, new ByteArrayInputStream(stuff), stuff.length);
    //check file exists
    Duration head = new Duration();
    Header[] responseHeaders = client.headRequest("expect success",
                                                  sobject,
                                                  SwiftRestClient.NEWEST);
    head.finished();
    LOG.info("head request duration " + head);
    for (Header header: responseHeaders) {
      LOG.info(header.toString());
    }
    //delete the file
    client.delete(sobject);
    //check file is gone
    try {
      Header[] headers = client.headRequest("expect fail",
                                            sobject,
                                            SwiftRestClient.NEWEST);
      Assert.fail("Expected deleted file, but object is still present: "
                  + sobject);
    } catch (FileNotFoundException e) {
      //expected
    }
    for (DurationStats stats: client.getOperationStatistics()) {
      LOG.info(stats.toString());
    }
  }

}
