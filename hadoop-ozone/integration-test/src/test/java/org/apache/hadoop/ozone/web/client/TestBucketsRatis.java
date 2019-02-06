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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.RatisTestHelper;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.client.rest.RestClient;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/** The same as {@link TestBuckets} except that this test is Ratis enabled. */
@Ignore("Disabling Ratis tests for pipeline work.")
@RunWith(value = Parameterized.class)
public class TestBucketsRatis {
  @Rule
  public Timeout testTimeout = new Timeout(300000);

  private static RatisTestHelper.RatisTestSuite suite;
  private static ClientProtocol client;
  private static OzoneConfiguration conf;

  @Parameterized.Parameters
  public static Collection<Object[]> clientProtocol() {
    Object[][] params = new Object[][] {
        {RpcClient.class},
        {RestClient.class}};
    return Arrays.asList(params);
  }

  @Parameterized.Parameter
  @SuppressWarnings("visibilitymodifier")
  public static Class clientProtocol;

  @BeforeClass
  public static void init() throws Exception {
    suite = new RatisTestHelper.RatisTestSuite();
    conf = suite.getConf();
  }

  @Before
  public void setup() throws Exception {
    if (clientProtocol.equals(RestClient.class)) {
      client = new RestClient(conf);
    } else {
      client = new RpcClient(conf);
    }
  }

  @AfterClass
  public static void shutdown() {
    if (suite != null) {
      suite.close();
    }
  }

  @Test
  public void testCreateBucket() throws Exception {
    TestBuckets.runTestCreateBucket(client);
  }

  @Test
  public void testAddBucketAcls() throws Exception {
    TestBuckets.runTestAddBucketAcls(client);
  }

  @Test
  public void testRemoveBucketAcls() throws Exception {
    TestBuckets.runTestRemoveBucketAcls(client);
  }

  @Test
  public void testDeleteBucket() throws OzoneException, IOException {
    TestBuckets.runTestDeleteBucket(client);
  }
  @Test
  public void testListBucket() throws Exception {
    TestBuckets.runTestListBucket(client);
  }
}
