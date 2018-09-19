/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.web.client;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.RatisTestHelper;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.apache.hadoop.ozone.web.client
    .TestKeys.PutHelper;
import static org.apache.hadoop.ozone.web.client
    .TestKeys.getMultiPartKey;
import static org.apache.hadoop.ozone.web.client
    .TestKeys.runTestGetKeyInfo;
import static org.apache.hadoop.ozone.web.client
    .TestKeys.runTestPutAndDeleteKey;
import static org.apache.hadoop.ozone.web.client
    .TestKeys.runTestPutAndGetKey;
import static org.apache.hadoop.ozone.web.client
    .TestKeys.runTestPutAndGetKeyWithDnRestart;
import static org.apache.hadoop.ozone.web.client
    .TestKeys.runTestPutAndListKey;
import static org.apache.hadoop.ozone.web.client
    .TestKeys.runTestPutKey;

/** The same as {@link TestKeys} except that this test is Ratis enabled. */
public class TestKeysRatis {
  @Rule
  public Timeout testTimeout = new Timeout(300000);
  private static RatisTestHelper.RatisTestSuite suite;
  private static MiniOzoneCluster ozoneCluster = null;
  static private String path;
  private static ClientProtocol client = null;

  @BeforeClass
  public static void init() throws Exception {
    suite = new RatisTestHelper.RatisTestSuite();
    path = GenericTestUtils.getTempPath(TestKeysRatis.class.getSimpleName());
    ozoneCluster = suite.getCluster();
    ozoneCluster.waitForClusterToBeReady();
    client = suite.newOzoneClient();
  }

  /**
   * shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (suite != null) {
      suite.close();
    }
  }


  @Test
  public void testPutKey() throws Exception {
    runTestPutKey(new PutHelper(client, path));
    String delimiter = RandomStringUtils.randomAlphanumeric(1);
    runTestPutKey(new PutHelper(client, path,
        getMultiPartKey(delimiter)));
  }

  @Test
  public void testPutAndGetKeyWithDnRestart() throws Exception {
    runTestPutAndGetKeyWithDnRestart(
        new PutHelper(client, path), ozoneCluster);
    String delimiter = RandomStringUtils.randomAlphanumeric(1);
    runTestPutAndGetKeyWithDnRestart(
        new PutHelper(client, path, getMultiPartKey(delimiter)),
        ozoneCluster);
  }

  @Test
  public void testPutAndGetKey() throws Exception {
    runTestPutAndGetKey(new PutHelper(client, path));
    String delimiter = RandomStringUtils.randomAlphanumeric(1);
    runTestPutAndGetKey(new PutHelper(client, path,
        getMultiPartKey(delimiter)));
  }

  @Test
  public void testPutAndDeleteKey() throws Exception  {
    runTestPutAndDeleteKey(new PutHelper(client, path));
    String delimiter = RandomStringUtils.randomAlphanumeric(1);
    runTestPutAndDeleteKey(new PutHelper(client, path,
        getMultiPartKey(delimiter)));
  }

  @Test
  public void testPutAndListKey() throws Exception {
    runTestPutAndListKey(new PutHelper(client, path));
    String delimiter = RandomStringUtils.randomAlphanumeric(1);
    runTestPutAndListKey(new PutHelper(client, path,
        getMultiPartKey(delimiter)));
  }

  @Test
  public void testGetKeyInfo() throws Exception {
    runTestGetKeyInfo(new PutHelper(client, path));
    String delimiter = RandomStringUtils.randomAlphanumeric(1);
    runTestGetKeyInfo(new PutHelper(client, path,
        getMultiPartKey(delimiter)));
  }
}
