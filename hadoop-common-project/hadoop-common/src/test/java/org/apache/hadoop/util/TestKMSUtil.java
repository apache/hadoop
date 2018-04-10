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
package org.apache.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link KMSUtil}.
 */
public class TestKMSUtil {

  public static final Logger LOG = LoggerFactory.getLogger(TestKMSUtil.class);

  @Rule
  public Timeout globalTimeout = new Timeout(90000);

  @Test
  public void testCreateKeyProviderFromTokenService() throws Exception {
    final Configuration conf = new Configuration();
    KeyProvider kp = KMSUtil.createKeyProviderFromTokenService(conf,
        "kms://https@localhost:9600/kms");
    assertNotNull(kp);
    kp.close();

    kp = KMSUtil.createKeyProviderFromTokenService(conf,
        "kms://https@localhost:9600/kms,kms://localhost1:9600/kms");
    assertNotNull(kp);
    kp.close();

    String invalidService = "whatever:9600";
    try {
      KMSUtil.createKeyProviderFromTokenService(conf, invalidService);
    } catch (Exception ex) {
      LOG.info("Expected exception:", ex);
      assertTrue(ex instanceof IllegalArgumentException);
      GenericTestUtils.assertExceptionContains(
          "Invalid token service " + invalidService, ex);
    }
  }
}
