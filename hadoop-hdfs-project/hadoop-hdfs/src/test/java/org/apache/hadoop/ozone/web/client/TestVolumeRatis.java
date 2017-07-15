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

import org.apache.hadoop.ozone.RatisTestHelper;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.junit.*;
import org.junit.rules.Timeout;

import java.io.IOException;

/** The same as {@link TestVolume} except that this test is Ratis enabled. */
public class TestVolumeRatis {
  @Rule
  public Timeout testTimeout = new Timeout(300000);

  private static RatisTestHelper.RatisTestSuite suite;
  private static OzoneRestClient ozoneClient;

  @BeforeClass
  public static void init() throws Exception {
    suite = new RatisTestHelper.RatisTestSuite(TestVolumeRatis.class);
    ozoneClient = suite.newOzoneRestClient();
  }

  @AfterClass
  public static void shutdown() {
    if (suite != null) {
      suite.close();
    }
  }

  @Test
  public void testCreateVolume() throws OzoneException, IOException {
    TestVolume.runTestCreateVolume(ozoneClient);
  }

  @Test
  public void testCreateDuplicateVolume() throws OzoneException {
    TestVolume.runTestCreateDuplicateVolume(ozoneClient);
  }

  @Test
  public void testDeleteVolume() throws OzoneException {
    TestVolume.runTestDeleteVolume(ozoneClient);
  }

  @Test
  public void testChangeOwnerOnVolume() throws OzoneException {
    TestVolume.runTestChangeOwnerOnVolume(ozoneClient);
  }

  @Test
  public void testChangeQuotaOnVolume() throws OzoneException, IOException {
    TestVolume.runTestChangeQuotaOnVolume(ozoneClient);
  }

  // TODO: remove @Ignore below once the problem has been resolved.
  @Ignore("listVolumes not implemented in DistributedStorageHandler")
  @Test
  public void testListVolume() throws OzoneException, IOException {
    TestVolume.runTestListVolume(ozoneClient);
  }

  // TODO: remove @Ignore below once the problem has been resolved.
  @Ignore("See TestVolume.testListVolumePagination()")
  @Test
  public void testListVolumePagination() throws OzoneException, IOException {
    TestVolume.runTestListVolumePagination(ozoneClient);
  }

  // TODO: remove @Ignore below once the problem has been resolved.
  @Ignore("See TestVolume.testListAllVolumes()")
  @Test
  public void testListAllVolumes() throws OzoneException, IOException {
    TestVolume.runTestListAllVolumes(ozoneClient);
  }

  @Test
  public void testListVolumes() throws OzoneException, IOException {
    TestVolume.runTestListVolumes(ozoneClient);
  }
}
