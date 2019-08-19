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
import org.junit.Assert;
import org.junit.Test;

import java.util.Objects;


/**
 * Test whether or not Custom Signing Algorithm Override works by turning it on.
 */
public class TestS3ASigningAlgorithmOverride extends AbstractS3ATestBase {

  @Override
  protected Configuration createConfiguration() {
    // awsConf is instantiated here
    Configuration conf = super.createConfiguration();
    conf.set(Constants.SIGNING_ALGORITHM,
            S3ATestConstants.S3A_SIGNING_ALGORITHM);
    return conf;
  }

  @Test
  public void testCustomSignerOverride() throws AssertionError {
    assertIsCustomSignerLoaded();
  }

  private void assertIsCustomSignerLoaded() {
    final ClientConfiguration awsConf = new ClientConfiguration();
    Assert.assertEquals(awsConf.getSignerOverride(),
            S3ATestConstants.S3A_SIGNING_ALGORITHM);
  }
}
