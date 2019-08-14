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

package org.apache.hadoop.fs.s3a;

import com.amazonaws.ClientConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.util.Objects;


/**
 * Test whether or not Custom Signing Algorithm Override works by turning it on.
 */
public class TestS3ASigningAlgorithmOverride extends AbstractS3ATestBase {

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.set(Constants.SIGNING_ALGORITHM,
            S3ATestConstants.S3A_SIGNING_ALGORITHM);
    LOG.debug("Inside createConfiguration...");
    return conf;
  }

  @Test
  public void testCustomSignerOverride() throws AssertionError {
    LOG.debug("Inside createConfiguration...");
    assertTrue(assertIsCustomSignerLoaded());
  }

  private boolean assertIsCustomSignerLoaded() {
    final ClientConfiguration awsConf = new ClientConfiguration();
    LOG.debug("Inside assertIsCustomSignerLoaded...");
    return assertEquals(awsConf.getSignerOverride(),
            S3ATestConstants.S3A_SIGNING_ALGORITHM
    );
  }

  private boolean assertEquals(String str1, String str2) {
    return Objects.equals(str1, str2);
  }
}
